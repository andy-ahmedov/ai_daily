from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import DedupCluster, DedupClusterPost, Post, PostSummary, Window
from aidigest.db.session import get_session


@dataclass(slots=True)
class DedupPost:
    post_id: int
    posted_at: datetime
    embedding: list[float]
    importance: int | None = None


@dataclass(slots=True)
class SimilarPost:
    post_id: int
    similarity: float


def get_or_create_window(start_at: datetime, end_at: datetime) -> Window:
    stmt = (
        pg_insert(Window)
        .values(start_at=start_at, end_at=end_at)
        .on_conflict_do_nothing(index_elements=[Window.start_at, Window.end_at])
    )
    with get_session() as session:
        session.execute(stmt)
        return session.execute(
            select(Window).where(Window.start_at == start_at, Window.end_at == end_at)
        ).scalar_one()


def set_window_status(window_id: int, status: str) -> None:
    with get_session() as session:
        session.execute(update(Window).where(Window.id == window_id).values(status=status))


def clear_clusters_for_window(window_id: int) -> None:
    with get_session() as session:
        cluster_ids = select(DedupCluster.id).where(DedupCluster.window_id == window_id)
        session.execute(
            delete(DedupClusterPost).where(DedupClusterPost.cluster_id.in_(cluster_ids))
        )
        session.execute(delete(DedupCluster).where(DedupCluster.window_id == window_id))


def create_cluster(
    window_id: int, representative_post_id: int, label: str | None = None
) -> DedupCluster:
    stmt = pg_insert(DedupCluster).values(
        window_id=window_id,
        representative_post_id=representative_post_id,
        label=label,
    )
    with get_session() as session:
        session.execute(stmt)
        return session.execute(
            select(DedupCluster)
            .where(
                DedupCluster.window_id == window_id,
                DedupCluster.representative_post_id == representative_post_id,
            )
            .order_by(DedupCluster.id.desc())
            .limit(1)
        ).scalar_one()


def add_cluster_posts(cluster_id: int, posts: list[tuple[int, float]]) -> None:
    if not posts:
        return
    values = [
        {"cluster_id": cluster_id, "post_id": int(post_id), "similarity": float(similarity)}
        for post_id, similarity in posts
    ]
    stmt = pg_insert(DedupClusterPost).values(values)
    with get_session() as session:
        session.execute(stmt)


def get_posts_for_semantic_dedup(start_at: datetime, end_at: datetime) -> list[DedupPost]:
    with get_session() as session:
        rows = session.execute(
            select(
                Post.id.label("post_id"),
                Post.posted_at.label("posted_at"),
                Post.embedding.label("embedding"),
                PostSummary.importance.label("importance"),
            )
            .outerjoin(PostSummary, PostSummary.post_id == Post.id)
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Post.embedding.is_not(None),
            )
            .order_by(
                PostSummary.importance.desc().nullslast(), Post.posted_at.asc(), Post.id.asc()
            )
        ).all()

        result: list[DedupPost] = []
        for row in rows:
            if row.embedding is None:
                continue
            result.append(
                DedupPost(
                    post_id=int(row.post_id),
                    posted_at=row.posted_at,
                    embedding=[float(value) for value in row.embedding],
                    importance=int(row.importance) if row.importance is not None else None,
                )
            )
        return result


def count_posts_without_embedding(start_at: datetime, end_at: datetime) -> int:
    with get_session() as session:
        value = session.execute(
            select(func.count(Post.id)).where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Post.embedding.is_(None),
            )
        ).scalar_one()
        return int(value)


def find_similar_posts_for_embedding(
    *,
    start_at: datetime,
    end_at: datetime,
    representative_embedding: list[float],
    exclude_post_ids: set[int],
    top_k: int,
) -> list[SimilarPost]:
    if top_k <= 0:
        return []

    distance_expr = Post.embedding.cosine_distance(representative_embedding)
    similarity_expr = (1 - distance_expr).label("similarity")

    with get_session() as session:
        stmt = (
            select(
                Post.id.label("post_id"),
                similarity_expr,
            )
            .where(
                Post.posted_at >= start_at,
                Post.posted_at < end_at,
                Post.embedding.is_not(None),
            )
            .order_by(distance_expr.asc(), Post.id.asc())
            .limit(top_k)
        )
        if exclude_post_ids:
            stmt = stmt.where(Post.id.not_in(sorted(exclude_post_ids)))

        rows = session.execute(stmt).all()
        return [
            SimilarPost(post_id=int(row.post_id), similarity=float(row.similarity)) for row in rows
        ]
