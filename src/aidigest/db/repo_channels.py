from __future__ import annotations

from typing import List

from loguru import logger
from sqlalchemy import select

from aidigest.db.models import Channel
from aidigest.db.session import get_session


def upsert_channel(
    tg_peer_id: int,
    username: str | None,
    title: str,
    is_active: bool = True,
) -> Channel:
    with get_session() as session:
        channel = session.execute(
            select(Channel).where(Channel.tg_peer_id == tg_peer_id)
        ).scalar_one_or_none()

        if channel:
            channel.username = username
            channel.title = title
            channel.is_active = is_active
            logger.info("Updated channel {}", tg_peer_id)
        else:
            channel = Channel(
                tg_peer_id=tg_peer_id,
                username=username,
                title=title,
                is_active=is_active,
            )
            session.add(channel)
            logger.info("Inserted channel {}", tg_peer_id)

        session.flush()
        return channel


def list_channels(active_only: bool = False) -> List[Channel]:
    with get_session() as session:
        stmt = select(Channel)
        if active_only:
            stmt = stmt.where(Channel.is_active.is_(True))
        stmt = stmt.order_by(Channel.id.asc())
        return list(session.execute(stmt).scalars())
