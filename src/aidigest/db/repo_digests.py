from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import Digest
from aidigest.db.session import get_session


def get_digest_by_window(window_id: int) -> Digest | None:
    with get_session() as session:
        return session.execute(
            select(Digest).where(Digest.window_id == window_id)
        ).scalar_one_or_none()


def upsert_digest(
    *,
    window_id: int,
    channel_id: int,
    message_ids: list[int],
    content: str,
    stats: dict[str, Any] | None,
    published_at: datetime | None,
) -> Digest:
    normalized_ids = [int(value) for value in message_ids]
    stmt = (
        pg_insert(Digest)
        .values(
            window_id=window_id,
            channel_id=int(channel_id),
            message_ids=normalized_ids,
            content=content,
            stats=stats,
            published_at=published_at,
        )
        .on_conflict_do_update(
            index_elements=[Digest.window_id],
            set_={
                "channel_id": int(channel_id),
                "message_ids": normalized_ids,
                "content": content,
                "stats": stats,
                "published_at": published_at,
            },
        )
    )

    with get_session() as session:
        session.execute(stmt)
        return session.execute(select(Digest).where(Digest.window_id == window_id)).scalar_one()
