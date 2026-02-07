from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import Post
from aidigest.db.session import get_session


def upsert_post(
    *,
    channel_id: int,
    message_id: int,
    posted_at: datetime,
    edited_at: datetime | None,
    text: str | None,
    raw: dict[str, Any] | None,
    has_media: bool,
    views: int | None,
    forwards: int | None,
    reactions: dict[str, Any] | None,
    permalink: str | None,
    content_hash: str,
    lang: str | None = None,
) -> Post:
    values = {
        "channel_id": channel_id,
        "message_id": message_id,
        "posted_at": posted_at,
        "edited_at": edited_at,
        "text": text,
        "raw": raw,
        "has_media": has_media,
        "views": views,
        "forwards": forwards,
        "reactions": reactions,
        "permalink": permalink,
        "content_hash": content_hash,
        "lang": lang,
    }

    update_values = {
        "edited_at": edited_at,
        "text": text,
        "raw": raw,
        "has_media": has_media,
        "views": views,
        "forwards": forwards,
        "reactions": reactions,
        "permalink": permalink,
        "content_hash": content_hash,
        "lang": lang,
    }

    stmt = (
        pg_insert(Post)
        .values(**values)
        .on_conflict_do_update(
            index_elements=[Post.channel_id, Post.message_id],
            set_=update_values,
        )
    )

    with get_session() as session:
        session.execute(stmt)
        return session.execute(
            select(Post).where(
                Post.channel_id == channel_id,
                Post.message_id == message_id,
            )
        ).scalar_one()


def count_posts_in_window(start_at: datetime, end_at: datetime) -> int:
    with get_session() as session:
        stmt = select(func.count(Post.id)).where(
            Post.posted_at >= start_at,
            Post.posted_at < end_at,
        )
        return int(session.execute(stmt).scalar_one())


def get_existing_message_ids(channel_id: int, message_ids: Iterable[int]) -> set[int]:
    ids = sorted(set(int(message_id) for message_id in message_ids))
    if not ids:
        return set()

    with get_session() as session:
        rows = session.execute(
            select(Post.message_id).where(
                Post.channel_id == channel_id,
                Post.message_id.in_(ids),
            )
        ).scalars()
        return {int(value) for value in rows}
