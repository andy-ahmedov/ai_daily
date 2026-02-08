from __future__ import annotations

import random
import re
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
from aidigest.nlp.prompts import ALLOWED_TAGS, CATEGORIES, SYSTEM_PROMPT, build_post_prompt

_ALLOWED_TAGS_BY_LOWER = {tag.lower(): tag for tag in ALLOWED_TAGS}
_CATEGORIES_BY_LOWER = {item.lower(): item for item in CATEGORIES}
_CATEGORY_IMPORTANCE_RANGES = {
    "LLM_RELEASE": (5, 5),
    "PRACTICE_INSIGHT": (4, 4),
    "ANALYSIS_OPINION": (4, 4),
    "DEALS": (3, 4),
    "OTHER_USEFUL": (3, 3),
    "NOISE": (1, 2),
}
_NOISE_KEYWORDS = (
    "реклама",
    "sponsored",
    "advert",
    "промокод",
    "скидк",
    "giveaway",
    "розыгрыш",
    "конкурс",
    "лотерея",
    "игра",
    "game",
    "квиз",
    "quiz",
    "мем",
    "meme",
)
_AI_KEYWORDS = (
    "ai",
    "llm",
    "gpt",
    "нейросет",
    "ии",
    "machine learning",
    "ml",
    "rag",
    "diffusion",
    "openai",
    "anthropic",
    "gemini",
    "mistral",
    "llama",
    "yandexgpt",
)
_NON_AI_TOPIC_KEYWORDS = (
    "футбол",
    "матч",
    "чемпионат",
    "кино",
    "сериал",
    "музыка",
    "кулинар",
    "гороскоп",
    "погода",
    "политика",
    "недвижим",
    "автомобил",
)
_URL_RE = re.compile(r"https?://\S+")
_WHITESPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(.+?[.!?…])(?:\s|$)", re.DOTALL)
_QUOTED_BLOCK_RE = re.compile(r"[\"“”«»'`][^\"“”«»'`]{2,160}[\"“”«»'`]")
_QUOTE_CHARS_RE = re.compile(r"[\"“”«»'`]+")

_WHY_FALLBACK_BY_CATEGORY = {
    "LLM_RELEASE": "Откройте пост, чтобы сразу понять, что нового в модели и как это повлияет на ваши задачи.",
    "PRACTICE_INSIGHT": "Откройте пост, чтобы взять практический прием и применить его в работе уже сегодня.",
    "ANALYSIS_OPINION": "Откройте пост, чтобы быстро оценить аргументы и риски перед принятием решения.",
    "DEALS": "Откройте пост, чтобы проверить условия предложения и понять, есть ли реальная выгода.",
    "OTHER_USEFUL": "Откройте пост, чтобы быстро понять, есть ли здесь практическая польза для ваших задач.",
    "NOISE": "Откройте пост только если вам нужен контекст сообщества, практической пользы здесь обычно мало.",
}


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
        category=str(summary.category or "OTHER_USEFUL"),
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
            category=matched_summary.category,
        )
        .on_conflict_do_update(
            index_elements=[PostSummary.post_id],
            set_={
                "key_point": matched_summary.key_point,
                "why_it_matters": matched_summary.why_it_matters,
                "tags": matched_summary.tags,
                "importance": matched_summary.importance,
                "category": matched_summary.category,
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
        why_it_matters=_WHY_FALLBACK_BY_CATEGORY["NOISE"],
        tags=["News"],
        importance=1,
        category="NOISE",
    )


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _normalize_category(value: Any) -> str:
    raw = str(value or "").strip()
    normalized = _CATEGORIES_BY_LOWER.get(raw.lower())
    if normalized:
        return normalized
    return "OTHER_USEFUL"


def _parse_importance(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_importance(category: str, raw_importance: int | None) -> int:
    low, high = _CATEGORY_IMPORTANCE_RANGES[category]
    if raw_importance is None:
        return low
    return max(low, min(high, raw_importance))


def _normalize_text_for_overlap(text: str) -> str:
    stripped = re.sub(r"[^\w\s]", " ", text.lower())
    return _WHITESPACE_RE.sub(" ", stripped).strip()


def _has_long_fragment_overlap(candidate: str, source: str) -> bool:
    candidate_norm = _normalize_text_for_overlap(candidate)
    source_norm = _normalize_text_for_overlap(source)
    if not candidate_norm or not source_norm:
        return False

    if len(candidate_norm) >= 40 and candidate_norm in source_norm:
        return True

    tokens = candidate_norm.split(" ")
    if len(tokens) < 6:
        return False

    for idx in range(0, len(tokens) - 5):
        ngram = " ".join(tokens[idx : idx + 6])
        if ngram in source_norm:
            return True
    return False


def _sanitize_why_text(text: str) -> str:
    cleaned = _URL_RE.sub("", text)
    cleaned = _QUOTED_BLOCK_RE.sub("", cleaned)
    cleaned = _QUOTE_CHARS_RE.sub("", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned[:200].strip()


def _to_single_sentence(text: str) -> str:
    if not text:
        return ""
    match = _SENTENCE_RE.search(text)
    sentence = (match.group(1) if match else text).strip()
    sentence = sentence.rstrip(".!?… ").strip()
    if not sentence:
        return ""
    return f"{sentence}."


def _normalize_why_it_matters(raw_why: Any, *, category: str, post_text: str) -> str:
    cleaned = _sanitize_why_text(str(raw_why or ""))
    sentence = _to_single_sentence(cleaned)
    if not sentence:
        return _WHY_FALLBACK_BY_CATEGORY[category]
    if _has_long_fragment_overlap(sentence, post_text):
        return _WHY_FALLBACK_BY_CATEGORY[category]
    return sentence


def _looks_like_noise(*, post_text: str, key_point: str) -> bool:
    combined = f"{key_point}\n{post_text}".strip().lower()
    if not combined:
        return False
    if _contains_any(combined, _NOISE_KEYWORDS):
        return True
    if _contains_any(combined, _NON_AI_TOPIC_KEYWORDS) and not _contains_any(combined, _AI_KEYWORDS):
        return True
    return False


def _normalize_summary_payload(payload: dict[str, Any], *, post_text: str) -> SummarySnapshot:
    key_point = str(payload.get("key_point", "")).strip()
    if not key_point:
        raise RuntimeError("LLM response is missing key_point")
    key_point = key_point[:160].strip()

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

    category = _normalize_category(payload.get("category"))
    if _looks_like_noise(post_text=post_text, key_point=key_point):
        category = "NOISE"

    importance = _normalize_importance(category, _parse_importance(payload.get("importance")))
    why_it_matters = _normalize_why_it_matters(
        payload.get("why_it_matters"),
        category=category,
        post_text=post_text,
    )

    return SummarySnapshot(
        key_point=key_point,
        why_it_matters=why_it_matters,
        tags=tags,
        importance=importance,
        category=category,
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
                summary = _normalize_summary_payload(payload, post_text=str(post.text or ""))
                llm_called = True

            upsert_summary(
                post_id=post.id,
                key_point=summary.key_point,
                why_it_matters=summary.why_it_matters,
                tags=summary.tags,
                importance=summary.importance,
                category=summary.category,
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
