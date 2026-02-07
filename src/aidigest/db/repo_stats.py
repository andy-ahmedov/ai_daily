from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select

from aidigest.db.models import Channel, DedupCluster, Digest, Post, PostSummary, Window
from aidigest.db.session import get_session


@dataclass(slots=True)
class LastPublishedDigest:
    window_id: int
    start_at: datetime
    end_at: datetime
    published_at: datetime
    message_ids: list[int]
    channel_id: int


def count_channels(active_only: bool) -> int:
    with get_session() as session:
        stmt = select(func.count(Channel.id))
        if active_only:
            stmt = stmt.where(Channel.is_active.is_(True))
        return int(session.execute(stmt).scalar_one())


def count_posts_in_window(start_at: datetime, end_at: datetime) -> int:
    with get_session() as session:
        value = session.execute(
            select(func.count(Post.id)).where(Post.posted_at >= start_at, Post.posted_at < end_at)
        ).scalar_one()
        return int(value)


def count_missing_summaries(start_at: datetime, end_at: datetime) -> int:
    with get_session() as session:
        value = session.execute(
            select(func.count(Post.id))
            .outerjoin(PostSummary, PostSummary.post_id == Post.id)
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                PostSummary.post_id.is_(None),
            )
        ).scalar_one()
        return int(value)


def count_missing_embeddings(start_at: datetime, end_at: datetime) -> int:
    with get_session() as session:
        value = session.execute(
            select(func.count(Post.id)).where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Post.embedding.is_(None),
            )
        ).scalar_one()
        return int(value)


def count_clusters(window_id: int) -> int:
    with get_session() as session:
        value = session.execute(
            select(func.count(DedupCluster.id)).where(DedupCluster.window_id == window_id)
        ).scalar_one()
        return int(value)


def get_window_by_range(start_at: datetime, end_at: datetime) -> Window | None:
    with get_session() as session:
        return session.execute(
            select(Window).where(Window.start_at == start_at, Window.end_at == end_at)
        ).scalar_one_or_none()


def get_last_published_digest() -> LastPublishedDigest | None:
    with get_session() as session:
        row = session.execute(
            select(
                Digest.window_id,
                Digest.channel_id,
                Digest.message_ids,
                Digest.published_at,
                Window.start_at,
                Window.end_at,
            )
            .join(Window, Window.id == Digest.window_id)
            .where(Digest.published_at.is_not(None))
            .order_by(Digest.published_at.desc(), Digest.id.desc())
            .limit(1)
        ).first()

    if row is None or row.published_at is None:
        return None

    return LastPublishedDigest(
        window_id=int(row.window_id),
        start_at=row.start_at,
        end_at=row.end_at,
        published_at=row.published_at,
        message_ids=[int(value) for value in (row.message_ids or [])],
        channel_id=int(row.channel_id),
    )
