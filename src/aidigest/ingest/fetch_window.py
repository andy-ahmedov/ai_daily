from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from aidigest.ingest.normalize import compute_content_hash, normalize_text


@dataclass(slots=True)
class FetchedPost:
    message_id: int
    posted_at: datetime
    edited_at: datetime | None
    text: str | None
    has_media: bool
    views: int | None
    forwards: int | None
    reactions: dict[str, Any] | None
    raw: dict[str, Any]
    permalink: str | None
    content_hash: str
    lang: str | None = None


def _wait_flood(retry_state: Any) -> float:
    exc = retry_state.outcome.exception()
    if isinstance(exc, FloodWaitError):
        return float(exc.seconds) + random.uniform(0.3, 1.0)
    return 1.0


def _before_sleep(retry_state: Any) -> None:
    exc = retry_state.outcome.exception()
    if not isinstance(exc, FloodWaitError):
        return
    wait_for = retry_state.next_action.sleep if retry_state.next_action else float(exc.seconds)
    logger.warning(
        "FloodWait while fetching posts ({}s). Sleeping {:.2f}s before retry #{}.",
        exc.seconds,
        wait_for,
        retry_state.attempt_number + 1,
    )


def _serialize_reactions(raw_reactions: Any) -> dict[str, Any] | None:
    if raw_reactions is None:
        return None
    if hasattr(raw_reactions, "to_dict"):
        return raw_reactions.to_dict()
    return None


def _build_permalink(username: str | None, message_id: int) -> str | None:
    if not username:
        return None
    clean_username = username.lstrip("@")
    if not clean_username:
        return None
    return f"https://t.me/{clean_username}/{message_id}"


@retry(
    retry=retry_if_exception_type(FloodWaitError),
    wait=_wait_flood,
    stop=stop_after_attempt(5),
    reraise=True,
    before_sleep=_before_sleep,
)
async def fetch_posts_in_window(
    *,
    client: TelegramClient,
    entity: Any,
    channel_username: str | None,
    start_at: datetime,
    end_at: datetime,
) -> list[FetchedPost]:
    posts: list[FetchedPost] = []
    async for message in client.iter_messages(entity, offset_date=end_at):
        if message is None:
            continue
        if getattr(message, "action", None) is not None:
            continue
        if message.id is None or message.date is None:
            continue

        posted_at = message.date
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)

        if posted_at >= end_at:
            continue
        if posted_at < start_at:
            break

        has_media = message.media is not None
        source_text = message.message or ""
        normalized_text = normalize_text(source_text)
        text = normalized_text or None

        raw: dict[str, Any] = {
            "id": int(message.id),
            "date": posted_at.isoformat(),
            "message": message.message,
            "views": message.views,
            "forwards": message.forwards,
        }
        permalink = _build_permalink(channel_username, int(message.id))

        posts.append(
            FetchedPost(
                message_id=int(message.id),
                posted_at=posted_at.astimezone(timezone.utc),
                edited_at=(
                    message.edit_date.astimezone(timezone.utc)
                    if message.edit_date is not None and message.edit_date.tzinfo is not None
                    else message.edit_date
                ),
                text=text,
                has_media=has_media,
                views=message.views,
                forwards=message.forwards,
                reactions=_serialize_reactions(message.reactions),
                raw=raw,
                permalink=permalink,
                content_hash=compute_content_hash(
                    text,
                    has_media=has_media,
                    permalink=permalink,
                    posted_at=posted_at,
                ),
            )
        )

    return posts
