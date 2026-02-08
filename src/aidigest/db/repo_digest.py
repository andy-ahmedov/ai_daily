from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select

from aidigest.db.models import Channel, DedupCluster, DedupClusterPost, Post, PostSummary, Window
from aidigest.db.session import get_session


@dataclass(slots=True)
class DigestPostRecord:
    post_id: int
    channel_id: int
    channel_title: str
    channel_username: str | None
    posted_at: datetime
    text: str | None
    permalink: str | None
    content_hash: str
    key_point: str | None
    why_it_matters: str | None
    tags: list[str] | None
    importance: int | None
    category: str | None


@dataclass(slots=True)
class DigestClusterRecord:
    cluster_id: int
    representative_post_id: int | None
    post_id: int
    similarity: float | None
    channel_title: str
    channel_username: str | None
    posted_at: datetime
    text: str | None
    permalink: str | None
    content_hash: str
    key_point: str | None
    why_it_matters: str | None
    tags: list[str] | None
    importance: int | None
    category: str | None


def get_window_by_range(start_at: datetime, end_at: datetime) -> Window | None:
    with get_session() as session:
        return session.execute(
            select(Window).where(Window.start_at == start_at, Window.end_at == end_at)
        ).scalar_one_or_none()


def get_active_channels() -> list[Channel]:
    with get_session() as session:
        return list(
            session.execute(
                select(Channel).where(Channel.is_active.is_(True)).order_by(Channel.id.asc())
            ).scalars()
        )


def get_posts_for_digest(start_at: datetime, end_at: datetime) -> list[DigestPostRecord]:
    with get_session() as session:
        rows = session.execute(
            select(
                Post.id.label("post_id"),
                Post.channel_id.label("channel_id"),
                Channel.title.label("channel_title"),
                Channel.username.label("channel_username"),
                Post.posted_at.label("posted_at"),
                Post.text.label("text"),
                Post.permalink.label("permalink"),
                Post.content_hash.label("content_hash"),
                PostSummary.key_point.label("key_point"),
                PostSummary.why_it_matters.label("why_it_matters"),
                PostSummary.tags.label("tags"),
                PostSummary.importance.label("importance"),
                PostSummary.category.label("category"),
            )
            .join(Channel, Channel.id == Post.channel_id)
            .outerjoin(PostSummary, PostSummary.post_id == Post.id)
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Channel.is_active.is_(True),
            )
            .order_by(Post.posted_at.asc(), Post.id.asc())
        ).all()

    return [
        DigestPostRecord(
            post_id=int(row.post_id),
            channel_id=int(row.channel_id),
            channel_title=row.channel_title,
            channel_username=row.channel_username,
            posted_at=row.posted_at,
            text=row.text,
            permalink=row.permalink,
            content_hash=row.content_hash,
            key_point=row.key_point,
            why_it_matters=row.why_it_matters,
            tags=list(row.tags) if row.tags is not None else None,
            importance=int(row.importance) if row.importance is not None else None,
            category=str(row.category) if row.category is not None else None,
        )
        for row in rows
    ]


def get_cluster_records(window_id: int) -> list[DigestClusterRecord]:
    with get_session() as session:
        rows = session.execute(
            select(
                DedupCluster.id.label("cluster_id"),
                DedupCluster.representative_post_id.label("representative_post_id"),
                DedupClusterPost.post_id.label("post_id"),
                DedupClusterPost.similarity.label("similarity"),
                Channel.title.label("channel_title"),
                Channel.username.label("channel_username"),
                Post.posted_at.label("posted_at"),
                Post.text.label("text"),
                Post.permalink.label("permalink"),
                Post.content_hash.label("content_hash"),
                PostSummary.key_point.label("key_point"),
                PostSummary.why_it_matters.label("why_it_matters"),
                PostSummary.tags.label("tags"),
                PostSummary.importance.label("importance"),
                PostSummary.category.label("category"),
            )
            .join(DedupClusterPost, DedupClusterPost.cluster_id == DedupCluster.id)
            .join(Post, Post.id == DedupClusterPost.post_id)
            .join(Channel, Channel.id == Post.channel_id)
            .outerjoin(PostSummary, PostSummary.post_id == Post.id)
            .where(DedupCluster.window_id == window_id, Channel.is_active.is_(True))
            .order_by(DedupCluster.id.asc(), Post.posted_at.asc(), Post.id.asc())
        ).all()

    return [
        DigestClusterRecord(
            cluster_id=int(row.cluster_id),
            representative_post_id=(
                int(row.representative_post_id) if row.representative_post_id is not None else None
            ),
            post_id=int(row.post_id),
            similarity=float(row.similarity) if row.similarity is not None else None,
            channel_title=row.channel_title,
            channel_username=row.channel_username,
            posted_at=row.posted_at,
            text=row.text,
            permalink=row.permalink,
            content_hash=row.content_hash,
            key_point=row.key_point,
            why_it_matters=row.why_it_matters,
            tags=list(row.tags) if row.tags is not None else None,
            importance=int(row.importance) if row.importance is not None else None,
            category=str(row.category) if row.category is not None else None,
        )
        for row in rows
    ]
