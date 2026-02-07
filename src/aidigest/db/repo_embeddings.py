from __future__ import annotations

from datetime import datetime

from sqlalchemy import select, update

from aidigest.db.models import Post
from aidigest.db.session import get_session


def get_posts_missing_embedding(start_at: datetime, end_at: datetime, limit: int) -> list[Post]:
    with get_session() as session:
        rows = session.execute(
            select(Post)
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Post.embedding.is_(None),
                Post.text.is_not(None),
            )
            .order_by(Post.posted_at.asc(), Post.id.asc())
            .limit(limit)
        ).scalars()
        return list(rows)


def update_post_embedding(post_id: int, embedding_vector: list[float]) -> None:
    with get_session() as session:
        session.execute(
            update(Post)
            .where(Post.id == post_id)
            .values(embedding=embedding_vector)
        )
