from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import desc, func, select

from aidigest.db.models import Channel, Post, PostSummary
from aidigest.db.session import get_session


@dataclass(slots=True)
class SummarySnapshot:
    key_point: str
    why_it_matters: str | None
    tags: list[str]
    importance: int
    category: str = "OTHER_USEFUL"


@dataclass(slots=True)
class DedupGroup:
    content_hash: str
    duplicates: int
    channel_titles: list[str]


def find_existing_summary_by_hash(content_hash: str) -> tuple[int, SummarySnapshot] | None:
    with get_session() as session:
        row = session.execute(
            select(
                PostSummary.post_id,
                PostSummary.key_point,
                PostSummary.why_it_matters,
                PostSummary.tags,
                PostSummary.importance,
                PostSummary.category,
            )
            .join(Post, Post.id == PostSummary.post_id)
            .where(Post.content_hash == content_hash)
            .order_by(PostSummary.post_id.asc())
            .limit(1)
        ).first()

        if row is None:
            return None

        return (
            int(row.post_id),
            SummarySnapshot(
                key_point=row.key_point,
                why_it_matters=row.why_it_matters,
                tags=list(row.tags or []),
                importance=int(row.importance),
                category=str(row.category or "OTHER_USEFUL"),
            ),
        )


def top_hash_groups_in_window(
    *,
    start_at: datetime,
    end_at: datetime,
    limit: int = 10,
) -> list[DedupGroup]:
    with get_session() as session:
        rows = session.execute(
            select(
                Post.content_hash,
                func.count(Post.id).label("duplicates"),
                func.array_agg(func.distinct(Channel.title)).label("channel_titles"),
            )
            .join(Channel, Channel.id == Post.channel_id)
            .where(Post.posted_at >= start_at, Post.posted_at < end_at)
            .group_by(Post.content_hash)
            .having(func.count(Post.id) > 1)
            .order_by(desc("duplicates"), Post.content_hash.asc())
            .limit(limit)
        ).all()

        groups: list[DedupGroup] = []
        for row in rows:
            groups.append(
                DedupGroup(
                    content_hash=row.content_hash,
                    duplicates=int(row.duplicates),
                    channel_titles=sorted([title for title in (row.channel_titles or []) if title]),
                )
            )
        return groups
