from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import Channel, Post, PostSummary
from aidigest.db.session import get_session


def has_summary(post_id: int) -> bool:
    with get_session() as session:
        return (
            session.execute(
                select(PostSummary.post_id).where(PostSummary.post_id == post_id)
            ).scalar_one_or_none()
            is not None
        )


def upsert_summary(
    *,
    post_id: int,
    key_point: str,
    why_it_matters: str | None,
    tags: list[str],
    importance: int,
    category: str,
) -> PostSummary:
    stmt = (
        pg_insert(PostSummary)
        .values(
            post_id=post_id,
            key_point=key_point,
            why_it_matters=why_it_matters,
            tags=tags,
            importance=importance,
            category=category,
        )
        .on_conflict_do_update(
            index_elements=[PostSummary.post_id],
            set_={
                "key_point": key_point,
                "why_it_matters": why_it_matters,
                "tags": tags,
                "importance": importance,
                "category": category,
            },
        )
    )

    with get_session() as session:
        session.execute(stmt)
        return session.execute(
            select(PostSummary).where(PostSummary.post_id == post_id)
        ).scalar_one()


def get_posts_in_window(start_at: datetime, end_at: datetime, limit: int) -> list[Post]:
    with get_session() as session:
        rows = session.execute(
            select(Post, Channel.title.label("channel_title"))
            .join(Channel, Channel.id == Post.channel_id)
            .where(Post.posted_at >= start_at, Post.posted_at < end_at)
            .order_by(Post.posted_at.asc(), Post.id.asc())
            .limit(limit)
        ).all()

        posts: list[Post] = []
        for row in rows:
            post = row[0]
            setattr(post, "channel_title", row.channel_title)
            posts.append(post)
        return posts


def get_posts_by_ids(post_ids: list[int]) -> list[Post]:
    ids = [int(post_id) for post_id in post_ids]
    if not ids:
        return []

    order = {post_id: idx for idx, post_id in enumerate(ids)}
    with get_session() as session:
        rows = session.execute(
            select(Post, Channel.title.label("channel_title"))
            .join(Channel, Channel.id == Post.channel_id)
            .where(Post.id.in_(ids))
        ).all()

        posts: list[Post] = []
        for row in rows:
            post = row[0]
            setattr(post, "channel_title", row.channel_title)
            posts.append(post)

    posts.sort(key=lambda post: order.get(int(post.id), len(order)))
    return posts


def get_missing_posts_in_window(start_at: datetime, end_at: datetime, limit: int) -> list[Post]:
    with get_session() as session:
        rows = session.execute(
            select(Post, Channel.title.label("channel_title"))
            .join(Channel, Channel.id == Post.channel_id)
            .outerjoin(PostSummary, PostSummary.post_id == Post.id)
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                PostSummary.post_id.is_(None),
            )
            .order_by(Post.posted_at.asc(), Post.id.asc())
            .limit(limit)
        ).all()

        posts: list[Post] = []
        for row in rows:
            post = row[0]
            setattr(post, "channel_title", row.channel_title)
            posts.append(post)
        return posts
