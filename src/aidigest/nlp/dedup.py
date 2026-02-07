from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime

from aidigest.db.repo_dedup_clusters import (
    add_cluster_posts,
    clear_clusters_for_window,
    count_posts_without_embedding,
    create_cluster,
    find_similar_posts_for_embedding,
    get_or_create_window,
    get_posts_for_semantic_dedup,
    set_window_status,
)


@dataclass(slots=True)
class ClusterResult:
    representative_post_id: int
    members: list[tuple[int, float]]


@dataclass(slots=True)
class DedupStats:
    clusters_created: int = 0
    posts_assigned: int = 0
    posts_skipped_no_embedding: int = 0
    largest_cluster_size: int = 0
    average_cluster_size: float = 0.0
    duration_seconds: float = 0.0
    top_clusters: list[ClusterResult] = field(default_factory=list)


def _build_clusters(
    *,
    start_at: datetime,
    end_at: datetime,
    threshold: float,
    top_k: int,
) -> tuple[list[ClusterResult], int]:
    posts = get_posts_for_semantic_dedup(start_at=start_at, end_at=end_at)
    assigned: set[int] = set()
    clusters: list[ClusterResult] = []

    for post in posts:
        if post.post_id in assigned:
            continue

        members: list[tuple[int, float]] = [(post.post_id, 1.0)]
        assigned.add(post.post_id)

        similar_posts = find_similar_posts_for_embedding(
            start_at=start_at,
            end_at=end_at,
            representative_embedding=post.embedding,
            exclude_post_ids=assigned,
            top_k=top_k,
        )
        for candidate in similar_posts:
            if candidate.post_id in assigned:
                continue
            if candidate.similarity < threshold:
                continue
            assigned.add(candidate.post_id)
            members.append((candidate.post_id, candidate.similarity))

        clusters.append(ClusterResult(representative_post_id=post.post_id, members=members))

    return clusters, len(assigned)


def run_semantic_dedup(
    *,
    start_at: datetime,
    end_at: datetime,
    threshold: float,
    top_k: int,
    dry_run: bool,
) -> DedupStats:
    started_at = time.monotonic()

    clusters, posts_assigned = _build_clusters(
        start_at=start_at,
        end_at=end_at,
        threshold=threshold,
        top_k=top_k,
    )
    skipped_no_embedding = count_posts_without_embedding(start_at=start_at, end_at=end_at)

    largest_cluster_size = max((len(cluster.members) for cluster in clusters), default=0)
    average_cluster_size = (
        float(sum(len(cluster.members) for cluster in clusters)) / len(clusters)
        if clusters
        else 0.0
    )

    if not dry_run:
        window = get_or_create_window(start_at=start_at, end_at=end_at)
        clear_clusters_for_window(window.id)

        for cluster in clusters:
            db_cluster = create_cluster(
                window_id=window.id,
                representative_post_id=cluster.representative_post_id,
                label=None,
            )
            add_cluster_posts(cluster_id=db_cluster.id, posts=cluster.members)

        set_window_status(window.id, "deduped")

    duration_seconds = time.monotonic() - started_at
    top_clusters = sorted(clusters, key=lambda cluster: len(cluster.members), reverse=True)[:10]

    return DedupStats(
        clusters_created=len(clusters),
        posts_assigned=posts_assigned,
        posts_skipped_no_embedding=skipped_no_embedding,
        largest_cluster_size=largest_cluster_size,
        average_cluster_size=average_cluster_size,
        duration_seconds=duration_seconds,
        top_clusters=top_clusters,
    )
