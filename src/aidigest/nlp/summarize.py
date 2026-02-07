from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import Post, PostSummary
from aidigest.db.repo_dedup import SummarySnapshot, find_existing_summary_by_hash
from aidigest.db.repo_summaries import get_posts_in_window, has_summary, upsert_summary
from aidigest.db.session import get_session
from aidigest.nlp.prompts import ALLOWED_TAGS, SYSTEM_PROMPT, build_post_prompt

_ALLOWED_TAGS_BY_LOWER = {tag.lower(): tag for tag in ALLOWED_TAGS}


@dataclass(slots=True)
class SummarizeStats:
    total_candidates: int = 0
    skipped_existing: int = 0
    copied_exact_dup: int = 0
    summarized: int = 0
    errors: int = 0


def _to_summary_snapshot(summary: PostSummary) -> SummarySnapshot:
    return SummarySnapshot(
        key_point=summary.key_point,
        why_it_matters=summary.why_it_matters,
        tags=list(summary.tags or []),
        importance=int(summary.importance),
    )


def get_or_copy_summary_for_post(post_id: int) -> SummarySnapshot | None:
    with get_session() as session:
        existing_summary = session.execute(
            select(PostSummary).where(PostSummary.post_id == post_id)
        ).scalar_one_or_none()
        if existing_summary is not None:
            return _to_summary_snapshot(existing_summary)

        post = session.execute(select(Post).where(Post.id == post_id)).scalar_one_or_none()
        if post is None:
            raise RuntimeError(f"post not found: {post_id}")
        post_hash = post.content_hash

    matched = find_existing_summary_by_hash(post_hash)
    if matched is None:
        return None

    source_post_id, matched_summary = matched
    stmt = (
        pg_insert(PostSummary)
        .values(
            post_id=post_id,
            key_point=matched_summary.key_point,
            why_it_matters=matched_summary.why_it_matters,
            tags=matched_summary.tags,
            importance=matched_summary.importance,
        )
        .on_conflict_do_update(
            index_elements=[PostSummary.post_id],
            set_={
                "key_point": matched_summary.key_point,
                "why_it_matters": matched_summary.why_it_matters,
                "tags": matched_summary.tags,
                "importance": matched_summary.importance,
            },
        )
    )

    with get_session() as session:
        session.execute(stmt)

    logger.info(
        "copied exact-dup summary: source_post_id={} target_post_id={} content_hash={}",
        source_post_id,
        post_id,
        post_hash,
    )
    return matched_summary


def _channel_title(post: Post) -> str:
    return str(getattr(post, "channel_title", f"channel:{post.channel_id}"))


def _media_only_summary() -> SummarySnapshot:
    return SummarySnapshot(
        key_point="Медиа без текста",
        why_it_matters="",
        tags=["News"],
        importance=1,
    )


def _normalize_summary_payload(payload: dict[str, Any]) -> SummarySnapshot:
    key_point = str(payload.get("key_point", "")).strip()
    if not key_point:
        raise RuntimeError("LLM response is missing key_point")
    key_point = key_point[:160].strip()

    why_it_matters = str(payload.get("why_it_matters", "") or "").strip()[:200]

    tags_raw = payload.get("tags", [])
    if not isinstance(tags_raw, list):
        tags_raw = []

    tags: list[str] = []
    for item in tags_raw:
        normalized = _ALLOWED_TAGS_BY_LOWER.get(str(item).strip().lower())
        if normalized and normalized not in tags:
            tags.append(normalized)
    if not tags:
        tags = ["News"]

    try:
        importance = int(payload.get("importance", 3))
    except (TypeError, ValueError):
        importance = 3
    importance = max(1, min(5, importance))

    return SummarySnapshot(
        key_point=key_point,
        why_it_matters=why_it_matters,
        tags=tags,
        importance=importance,
    )


def summarize_window(
    *,
    start_at: datetime,
    end_at: datetime,
    limit: int,
    dry_run: bool,
) -> SummarizeStats:
    from aidigest.config import get_settings
    from aidigest.nlp.yandex_llm import chat_json, make_client

    posts = get_posts_in_window(start_at=start_at, end_at=end_at, limit=limit)
    stats = SummarizeStats(total_candidates=len(posts))
    if not posts:
        return stats

    settings = get_settings()
    client = None
    if not dry_run and settings.yandex_api_key and settings.yandex_folder_id:
        client = make_client(settings)

    for post in posts:
        channel_title = _channel_title(post)
        posted_at = post.posted_at.isoformat()
        llm_called = False

        try:
            if has_summary(post.id):
                stats.skipped_existing += 1
                logger.info(
                    "summarize post_id={} channel='{}' posted_at={} action=skipped",
                    post.id,
                    channel_title,
                    posted_at,
                )
                continue

            if dry_run:
                exact_match = find_existing_summary_by_hash(post.content_hash)
                if exact_match is not None and int(exact_match[0]) != int(post.id):
                    stats.copied_exact_dup += 1
                    logger.info(
                        "summarize post_id={} channel='{}' posted_at={} action=copied dry_run=true",
                        post.id,
                        channel_title,
                        posted_at,
                    )
                    continue

                stats.summarized += 1
                logger.info(
                    "summarize post_id={} channel='{}' posted_at={} action=summarized dry_run=true",
                    post.id,
                    channel_title,
                    posted_at,
                )
                continue

            copied = get_or_copy_summary_for_post(post.id)
            if copied is not None:
                stats.copied_exact_dup += 1
                logger.info(
                    "summarize post_id={} channel='{}' posted_at={} action=copied",
                    post.id,
                    channel_title,
                    posted_at,
                )
                continue

            if not post.text and post.has_media:
                summary = _media_only_summary()
            else:
                if client is None:
                    raise RuntimeError(
                        "YANDEX_API_KEY, YANDEX_FOLDER_ID and YANDEX_MODEL_URI must be set for summarize."
                    )
                if not settings.yandex_model_uri:
                    raise RuntimeError("YANDEX_MODEL_URI must be set for summarize.")

                payload = chat_json(
                    client=client,
                    model_uri=settings.yandex_model_uri,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": build_post_prompt(post)},
                    ],
                    post_id=post.id,
                )
                summary = _normalize_summary_payload(payload)
                llm_called = True

            upsert_summary(
                post_id=post.id,
                key_point=summary.key_point,
                why_it_matters=summary.why_it_matters,
                tags=summary.tags,
                importance=summary.importance,
            )
            stats.summarized += 1
            logger.info(
                "summarize post_id={} channel='{}' posted_at={} action=summarized",
                post.id,
                channel_title,
                posted_at,
            )
        except Exception as exc:
            stats.errors += 1
            logger.error(
                "summarize failed post_id={} channel='{}' posted_at={} error={}",
                post.id,
                channel_title,
                posted_at,
                exc,
            )
        finally:
            if llm_called:
                time.sleep(random.uniform(0.2, 0.5))

    return stats
