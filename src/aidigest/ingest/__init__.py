from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime

from loguru import logger

from aidigest.db.models import Channel
from aidigest.db.repo_channels import list_channels
from aidigest.db.repo_posts import get_existing_message_ids, upsert_post
from aidigest.ingest.fetch_window import fetch_posts_in_window
from aidigest.ingest.window import compute_window
from aidigest.telegram.user_client import UserTelegramClient


@dataclass(slots=True)
class ChannelIngestStats:
    channel_id: int
    title: str
    fetched: int
    inserted: int
    updated: int
    error: str | None = None


@dataclass(slots=True)
class IngestSummary:
    start_at: datetime
    end_at: datetime
    channels_processed: int
    posts_fetched: int
    posts_inserted: int
    posts_updated: int
    duration_seconds: float
    per_channel: list[ChannelIngestStats] = field(default_factory=list)


async def _resolve_channel_entity(client: UserTelegramClient, channel: Channel) -> object:
    try:
        return await client.resolve_entity_by_peer_id(channel.tg_peer_id)
    except Exception as peer_exc:
        if not channel.username:
            raise RuntimeError("failed to resolve channel entity") from peer_exc

    ref = channel.username if channel.username.startswith("@") else f"@{channel.username}"
    try:
        return await client.resolve_entity(ref)
    except Exception as username_exc:
        raise RuntimeError("failed to resolve channel entity") from username_exc


async def ingest_posts_for_date(
    *,
    client: UserTelegramClient,
    target_date: date,
    timezone: str,
    start_hour: int,
    dry_run: bool = False,
) -> IngestSummary:
    started_at = time.monotonic()
    start_at, end_at = compute_window(target_date=target_date, tz=timezone, start_hour=start_hour)
    logger.info(
        "Ingest window: {} -> {} ({})",
        start_at.isoformat(),
        end_at.isoformat(),
        timezone,
    )

    channels = list_channels(active_only=True)
    per_channel: list[ChannelIngestStats] = []
    posts_fetched = 0
    posts_inserted = 0
    posts_updated = 0

    for idx, channel in enumerate(channels):
        try:
            entity = await _resolve_channel_entity(client, channel)
            fetched_posts = await fetch_posts_in_window(
                client=client.client,
                entity=entity,
                channel_username=channel.username,
                start_at=start_at,
                end_at=end_at,
            )
            fetched_count = len(fetched_posts)
            posts_fetched += fetched_count

            message_ids = [post.message_id for post in fetched_posts]
            existing_ids = get_existing_message_ids(channel.id, message_ids)
            inserted_count = sum(1 for message_id in message_ids if message_id not in existing_ids)
            updated_count = fetched_count - inserted_count

            posts_inserted += inserted_count
            posts_updated += updated_count

            if not dry_run:
                for post in fetched_posts:
                    upsert_post(
                        channel_id=channel.id,
                        message_id=post.message_id,
                        posted_at=post.posted_at,
                        edited_at=post.edited_at,
                        text=post.text,
                        raw=post.raw,
                        has_media=post.has_media,
                        views=post.views,
                        forwards=post.forwards,
                        reactions=post.reactions,
                        permalink=post.permalink,
                        content_hash=post.content_hash,
                        lang=post.lang,
                    )

            per_channel.append(
                ChannelIngestStats(
                    channel_id=channel.id,
                    title=channel.title,
                    fetched=fetched_count,
                    inserted=inserted_count,
                    updated=updated_count,
                )
            )
            logger.info(
                "Ingested channel '{}' (fetched={}, inserted={}, updated={}, dry_run={})",
                channel.title,
                fetched_count,
                inserted_count,
                updated_count,
                dry_run,
            )
        except Exception as exc:
            logger.exception("Failed to ingest channel '{}': {}", channel.title, exc)
            per_channel.append(
                ChannelIngestStats(
                    channel_id=channel.id,
                    title=channel.title,
                    fetched=0,
                    inserted=0,
                    updated=0,
                    error=str(exc),
                )
            )
        finally:
            if idx < len(channels) - 1:
                await asyncio.sleep(random.uniform(0.3, 1.0))

    duration = time.monotonic() - started_at
    return IngestSummary(
        start_at=start_at,
        end_at=end_at,
        channels_processed=len(channels),
        posts_fetched=posts_fetched,
        posts_inserted=posts_inserted,
        posts_updated=posts_updated,
        duration_seconds=duration,
        per_channel=per_channel,
    )


__all__ = [
    "ChannelIngestStats",
    "IngestSummary",
    "ingest_posts_for_date",
]
