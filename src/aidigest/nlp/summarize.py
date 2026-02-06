from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aidigest.db.models import Post, PostSummary
from aidigest.db.repo_dedup import SummarySnapshot, find_existing_summary_by_hash
from aidigest.db.session import get_session


def _to_summary_snapshot(summary: PostSummary) -> SummarySnapshot:
    return SummarySnapshot(
        key_point=summary.key_point,
        why_it_matters=summary.why_it_matters,
        tags=list(summary.tags or []),
        importance=int(summary.importance),
    )


def get_or_copy_summary_for_post(post_id: int) -> SummarySnapshot | None:
    with get_session() as session:
        existing_summary = session.execute(
            select(PostSummary).where(PostSummary.post_id == post_id)
        ).scalar_one_or_none()
        if existing_summary is not None:
            return _to_summary_snapshot(existing_summary)

        post = session.execute(select(Post).where(Post.id == post_id)).scalar_one_or_none()
        if post is None:
            raise RuntimeError(f"post not found: {post_id}")
        post_hash = post.content_hash

    matched = find_existing_summary_by_hash(post_hash)
    if matched is None:
        return None

    source_post_id, matched_summary = matched
    stmt = pg_insert(PostSummary).values(
        post_id=post_id,
        key_point=matched_summary.key_point,
        why_it_matters=matched_summary.why_it_matters,
        tags=matched_summary.tags,
        importance=matched_summary.importance,
    ).on_conflict_do_update(
        index_elements=[PostSummary.post_id],
        set_={
            "key_point": matched_summary.key_point,
            "why_it_matters": matched_summary.why_it_matters,
            "tags": matched_summary.tags,
            "importance": matched_summary.importance,
        },
    )

    with get_session() as session:
        session.execute(stmt)

    logger.info(
        "copied exact-dup summary: source_post_id={} target_post_id={} content_hash={}",
        source_post_id,
        post_id,
        post_hash,
    )
    return matched_summary

