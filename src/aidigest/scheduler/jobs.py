from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger

from aidigest.config import Settings, get_settings
from aidigest.db.repo_dedup_clusters import get_or_create_window, set_window_status
from aidigest.db.repo_digests import get_digest_by_window, upsert_digest
from aidigest.db.repo_embeddings import get_posts_missing_embedding, update_post_embedding
from aidigest.db.repo_posts import count_posts_in_window
from aidigest.digest.build import build_digest_data
from aidigest.digest.format import render_digest_html
from aidigest.ingest import IngestSummary, ingest_posts_for_date
from aidigest.ingest.window import compute_window
from aidigest.nlp.dedup import DedupStats, run_semantic_dedup
from aidigest.nlp.summarize import SummarizeStats, summarize_window
from aidigest.telegram.bot_client import DigestPublisher
from aidigest.telegram.user_client import UserTelegramClient


@dataclass(slots=True)
class EmbedStats:
    total_candidates: int
    embedded: int
    failed_batches: int
    failed_posts: int


@dataclass(slots=True)
class PipelineStats:
    ingest: IngestSummary | None = None
    summarize: SummarizeStats | None = None
    embed: EmbedStats | None = None
    dedup: DedupStats | None = None
    messages_sent: int = 0
    total_duration_seconds: float = 0.0
    failed: bool = False
    error: str | None = None


def _run_async(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError as exc:
        if "asyncio.run()" in str(exc):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(coro)
        raise


def _parse_chat_id(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        return None


def _stage_duration(started_at: float) -> float:
    return time.monotonic() - started_at


def _select_target_date(target_date: date | None, settings: Settings) -> date:
    return target_date or datetime.now(ZoneInfo(settings.timezone)).date()


async def _ingest_async(*, settings: Settings, effective_date: date) -> IngestSummary:
    if not settings.tg_api_id or not settings.tg_api_hash:
        raise RuntimeError("TG_API_ID and TG_API_HASH must be set for ingest stage")

    client = UserTelegramClient(
        api_id=settings.tg_api_id,
        api_hash=settings.tg_api_hash,
        session_path=settings.tg_session_path,
    )
    await client.connect(allow_interactive_login=False)
    try:
        return await ingest_posts_for_date(
            client=client,
            target_date=effective_date,
            timezone=settings.timezone,
            start_hour=settings.window_start_hour,
            dry_run=False,
        )
    finally:
        await client.disconnect()


def ingest_window(*, settings: Settings, effective_date: date) -> IngestSummary:
    started_at = time.monotonic()
    result = _run_async(_ingest_async(settings=settings, effective_date=effective_date))
    logger.info(
        "pipeline stage=ingest done duration={:.2f}s fetched={} inserted={} updated={}",
        _stage_duration(started_at),
        result.posts_fetched,
        result.posts_inserted,
        result.posts_updated,
    )
    return result


def summarize_window_full(*, start_at: datetime, end_at: datetime) -> SummarizeStats:
    started_at = time.monotonic()
    limit = max(1, count_posts_in_window(start_at=start_at, end_at=end_at))
    stats = summarize_window(start_at=start_at, end_at=end_at, limit=limit, dry_run=False)
    logger.info(
        "pipeline stage=summarize done duration={:.2f}s total={} summarized={} copied={} skipped={} errors={}",
        _stage_duration(started_at),
        stats.total_candidates,
        stats.summarized,
        stats.copied_exact_dup,
        stats.skipped_existing,
        stats.errors,
    )
    return stats


def embed_window(*, start_at: datetime, end_at: datetime, batch_size: int = 16) -> EmbedStats:
    from aidigest.nlp.embed import embed_texts, make_yandex_client, validate_embedding

    settings = get_settings()
    if not settings.yandex_api_key or not settings.yandex_folder_id:
        raise RuntimeError("YANDEX_API_KEY and YANDEX_FOLDER_ID must be set for embed stage")
    if not settings.yandex_embed_model_uri:
        raise RuntimeError("YANDEX_EMBED_MODEL_URI must be set for embed stage")

    started_at = time.monotonic()
    posts = get_posts_missing_embedding(start_at=start_at, end_at=end_at, limit=100000)
    total_candidates = len(posts)
    embedded = 0
    failed_batches = 0
    failed_posts = 0

    if total_candidates:
        make_yandex_client(settings)
        for offset in range(0, total_candidates, batch_size):
            batch = posts[offset : offset + batch_size]
            texts = [str(post.text or "") for post in batch]
            if not texts:
                continue
            try:
                vectors = embed_texts(texts)
                if len(vectors) != len(batch):
                    raise RuntimeError(
                        f"batch size mismatch: expected {len(batch)}, got {len(vectors)}"
                    )
                for post, vector in zip(batch, vectors):
                    update_post_embedding(post.id, validate_embedding(vector))
                    embedded += 1
            except Exception as exc:
                failed_batches += 1
                failed_posts += len(batch)
                logger.error(
                    "pipeline stage=embed batch failed offset={} size={} error={}",
                    offset,
                    len(batch),
                    exc,
                )
            finally:
                if offset + batch_size < total_candidates:
                    time.sleep(random.uniform(0.1, 0.3))

    stats = EmbedStats(
        total_candidates=total_candidates,
        embedded=embedded,
        failed_batches=failed_batches,
        failed_posts=failed_posts,
    )
    logger.info(
        "pipeline stage=embed done duration={:.2f}s candidates={} embedded={} failed_batches={} failed_posts={}",
        _stage_duration(started_at),
        stats.total_candidates,
        stats.embedded,
        stats.failed_batches,
        stats.failed_posts,
    )
    return stats


def dedup_window(
    *, start_at: datetime, end_at: datetime, threshold: float, top_k: int = 80
) -> DedupStats:
    started_at = time.monotonic()
    stats = run_semantic_dedup(
        start_at=start_at,
        end_at=end_at,
        threshold=threshold,
        top_k=top_k,
        dry_run=False,
    )
    logger.info(
        "pipeline stage=dedup done duration={:.2f}s clusters={} posts_assigned={} largest_cluster={}",
        _stage_duration(started_at),
        stats.clusters_created,
        stats.posts_assigned,
        stats.largest_cluster_size,
    )
    return stats


def publish_window(
    *, settings: Settings, window_id: int, start_at: datetime, end_at: datetime
) -> int:
    chat_id = _parse_chat_id(settings.digest_channel_id)
    if chat_id is None:
        raise RuntimeError("DIGEST_CHANNEL_ID must be a Telegram chat_id (e.g. -100...)")
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN must be set for publish stage")

    existing = get_digest_by_window(window_id)
    if existing is not None and existing.published_at is not None:
        logger.info(
            "pipeline stage=publish skipped already published window_id={} published_at={}",
            window_id,
            existing.published_at,
        )
        return 0

    started_at = time.monotonic()
    digest_data = build_digest_data(start_at=start_at, end_at=end_at, window_id=window_id, top_n=10)
    messages = render_digest_html(digest_data)
    if not messages:
        raise RuntimeError("digest rendering produced no messages")

    with DigestPublisher(settings.bot_token) as publisher:
        message_ids = publisher.send_html_messages(chat_id=chat_id, messages=messages)

    stats = {
        "messages": len(messages),
        "top_clusters": len(digest_data.top_clusters),
        "channels": len(digest_data.per_channel),
        "posts": sum(channel.posts_count for channel in digest_data.per_channel),
    }
    content = "\n\n----- MESSAGE BREAK -----\n\n".join(messages)
    upsert_digest(
        window_id=window_id,
        channel_id=chat_id,
        message_ids=message_ids,
        content=content,
        stats=stats,
        published_at=datetime.now(timezone.utc),
    )
    logger.info(
        "pipeline stage=publish done duration={:.2f}s messages_sent={}",
        _stage_duration(started_at),
        len(message_ids),
    )
    return len(message_ids)


def run_daily_pipeline(target_date: date | None = None) -> PipelineStats:
    settings = get_settings()
    pipeline_started_at = time.monotonic()
    stats = PipelineStats()
    window_id: int | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None

    try:
        effective_date = _select_target_date(target_date, settings)
        start_at, end_at = compute_window(
            target_date=effective_date,
            tz=settings.timezone,
            start_hour=settings.window_start_hour,
        )
        window = get_or_create_window(start_at=start_at, end_at=end_at)
        window_id = int(window.id)

        existing_digest = get_digest_by_window(window_id)
        if existing_digest is not None and existing_digest.published_at is not None:
            logger.info(
                "pipeline skipped: already published window={}..{} window_id={}",
                start_at.isoformat(),
                end_at.isoformat(),
                window_id,
            )
            set_window_status(window_id, "published")
            stats.total_duration_seconds = _stage_duration(pipeline_started_at)
            return stats

        logger.info(
            "pipeline started window={}..{} window_id={} timezone={}",
            start_at.isoformat(),
            end_at.isoformat(),
            window_id,
            settings.timezone,
        )

        stats.ingest = ingest_window(settings=settings, effective_date=effective_date)
        set_window_status(window_id, "ingested")

        stats.summarize = summarize_window_full(start_at=start_at, end_at=end_at)
        set_window_status(window_id, "summarized")

        stats.embed = embed_window(start_at=start_at, end_at=end_at, batch_size=16)
        set_window_status(window_id, "embedded")

        stats.dedup = dedup_window(
            start_at=start_at,
            end_at=end_at,
            threshold=settings.dedup_threshold,
            top_k=80,
        )
        set_window_status(window_id, "deduped")

        stats.messages_sent = publish_window(
            settings=settings,
            window_id=window_id,
            start_at=start_at,
            end_at=end_at,
        )
        set_window_status(window_id, "published")

        stats.total_duration_seconds = _stage_duration(pipeline_started_at)
        logger.info(
            "pipeline finished window_id={} duration={:.2f}s fetched={} summarized={} embedded={} clusters={} messages={}",
            window_id,
            stats.total_duration_seconds,
            stats.ingest.posts_fetched if stats.ingest else 0,
            stats.summarize.summarized if stats.summarize else 0,
            stats.embed.embedded if stats.embed else 0,
            stats.dedup.clusters_created if stats.dedup else 0,
            stats.messages_sent,
        )
        return stats
    except Exception as exc:
        if window_id is not None:
            try:
                set_window_status(window_id, "failed")
            except Exception as status_exc:
                logger.error("pipeline failed to set window status failed: {}", status_exc)

        if start_at is not None and end_at is not None:
            logger.exception(
                "pipeline failed window={}..{} error={}",
                start_at.isoformat(),
                end_at.isoformat(),
                exc,
            )
        else:
            logger.exception("pipeline failed before window initialization error={}", exc)
        stats.total_duration_seconds = _stage_duration(pipeline_started_at)
        stats.failed = True
        stats.error = str(exc)
        return stats
