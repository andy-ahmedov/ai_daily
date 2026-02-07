from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from aidigest.config import get_settings
from aidigest.db.models import Channel
from aidigest.db.repo_digest import (
    DigestClusterRecord,
    DigestPostRecord,
    get_active_channels,
    get_cluster_records,
    get_posts_for_digest,
)
from aidigest.ingest.normalize import normalize_text


@dataclass(slots=True)
class DigestHeader:
    digest_date: str
    timezone: str
    start_at: datetime
    end_at: datetime


@dataclass(slots=True)
class DigestTopCluster:
    title: str
    why: str
    tags: list[str]
    sources: list[str]
    max_importance: int
    size: int
    freshest_posted_at: datetime


@dataclass(slots=True)
class DigestPostItem:
    post_id: int
    posted_at: datetime
    tags: list[str]
    importance: int
    key_point: str
    why_it_matters: str
    permalink: str | None
    content_hash: str
    source: str


@dataclass(slots=True)
class DigestChannelSection:
    channel_id: int
    channel_name: str
    posts_count: int
    channel_summary: list[str] = field(default_factory=list)
    posts: list[DigestPostItem] = field(default_factory=list)


@dataclass(slots=True)
class DigestData:
    header: DigestHeader
    top_clusters: list[DigestTopCluster]
    per_channel: list[DigestChannelSection]
    top_limit: int


def _truncate(text: str, limit: int) -> str:
    compact = (text or "").strip()
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 1)].rstrip() + "…"


def _source_name(username: str | None, title: str) -> str:
    if username:
        return f"@{username.lstrip('@')}"
    return title


def _coalesce_summary(
    *,
    key_point: str | None,
    why_it_matters: str | None,
    tags: list[str] | None,
    importance: int | None,
    text: str | None,
) -> tuple[str, str, list[str], int]:
    normalized_text = normalize_text(text or "")
    fallback_key_point = _truncate(normalized_text or "Пост без текста", 120)

    normalized_key_point = (key_point or "").strip() or fallback_key_point
    normalized_why = (why_it_matters or "").strip()
    normalized_tags = [tag.strip() for tag in (tags or []) if tag and tag.strip()] or ["News"]
    normalized_importance = int(importance if importance is not None else 2)
    normalized_importance = min(5, max(1, normalized_importance))
    return normalized_key_point, normalized_why, normalized_tags, normalized_importance


def _to_post_item(record: DigestPostRecord) -> DigestPostItem:
    key_point, why, tags, importance = _coalesce_summary(
        key_point=record.key_point,
        why_it_matters=record.why_it_matters,
        tags=record.tags,
        importance=record.importance,
        text=record.text,
    )
    return DigestPostItem(
        post_id=record.post_id,
        posted_at=record.posted_at,
        tags=tags,
        importance=importance,
        key_point=key_point,
        why_it_matters=why,
        permalink=record.permalink,
        content_hash=record.content_hash,
        source=_source_name(record.channel_username, record.channel_title),
    )


def _build_channel_summary(posts: list[DigestPostItem]) -> list[str]:
    ranked = sorted(posts, key=lambda item: (item.importance, item.posted_at), reverse=True)
    limit = min(6, max(3, len(ranked))) if ranked else 0

    bullets: list[str] = []
    seen: set[str] = set()
    for post in ranked:
        primary_tag = post.tags[0] if post.tags else "News"
        bullet = f"[{primary_tag}][⭐{post.importance}] {post.key_point}"
        if bullet in seen:
            continue
        seen.add(bullet)
        bullets.append(bullet)
        if len(bullets) >= limit:
            break
    return bullets


def _build_per_channel(
    channels: list[Channel],
    posts_by_channel: dict[int, list[DigestPostItem]],
) -> list[DigestChannelSection]:
    sections: list[DigestChannelSection] = []
    for channel in channels:
        channel_posts = sorted(
            posts_by_channel.get(channel.id, []), key=lambda item: item.posted_at
        )
        channel_name = _source_name(channel.username, channel.title)
        sections.append(
            DigestChannelSection(
                channel_id=channel.id,
                channel_name=channel_name,
                posts_count=len(channel_posts),
                channel_summary=_build_channel_summary(channel_posts),
                posts=channel_posts,
            )
        )
    return sections


def _cluster_rank_key(cluster_records: list[DigestClusterRecord]) -> tuple[int, int, datetime]:
    enriched = [
        _coalesce_summary(
            key_point=row.key_point,
            why_it_matters=row.why_it_matters,
            tags=row.tags,
            importance=row.importance,
            text=row.text,
        )
        for row in cluster_records
    ]
    max_importance = max((item[3] for item in enriched), default=0)
    size = len({row.post_id for row in cluster_records})
    freshest = max((row.posted_at for row in cluster_records), default=datetime.min)
    return max_importance, size, freshest


def _pick_representative(cluster_records: list[DigestClusterRecord]) -> DigestClusterRecord:
    preferred_id = cluster_records[0].representative_post_id
    if preferred_id is not None:
        for row in cluster_records:
            if row.post_id == preferred_id:
                return row
    return max(
        cluster_records,
        key=lambda row: (
            _coalesce_summary(
                key_point=row.key_point,
                why_it_matters=row.why_it_matters,
                tags=row.tags,
                importance=row.importance,
                text=row.text,
            )[3],
            row.posted_at,
        ),
    )


def _build_top_clusters_from_dedup(
    cluster_records: list[DigestClusterRecord],
    top_n: int,
) -> list[DigestTopCluster]:
    grouped: dict[int, list[DigestClusterRecord]] = {}
    for row in cluster_records:
        grouped.setdefault(row.cluster_id, []).append(row)

    ranked_clusters = sorted(
        grouped.values(),
        key=_cluster_rank_key,
        reverse=True,
    )

    top_clusters: list[DigestTopCluster] = []
    for rows in ranked_clusters[:top_n]:
        rep = _pick_representative(rows)
        rep_key_point, rep_why, rep_tags, _ = _coalesce_summary(
            key_point=rep.key_point,
            why_it_matters=rep.why_it_matters,
            tags=rep.tags,
            importance=rep.importance,
            text=rep.text,
        )
        max_importance, size, freshest = _cluster_rank_key(rows)
        sources = sorted({_source_name(row.channel_username, row.channel_title) for row in rows})
        top_clusters.append(
            DigestTopCluster(
                title=_truncate(rep_key_point, 120),
                why=rep_why,
                tags=rep_tags,
                sources=sources,
                max_importance=max_importance,
                size=size,
                freshest_posted_at=freshest,
            )
        )
    return top_clusters


def _build_top_clusters_fallback(posts: list[DigestPostItem], top_n: int) -> list[DigestTopCluster]:
    ranked = sorted(posts, key=lambda post: (post.importance, post.posted_at), reverse=True)
    selected: list[DigestPostItem] = []
    seen_hashes: set[str] = set()

    for post in ranked:
        if post.content_hash in seen_hashes:
            continue
        seen_hashes.add(post.content_hash)
        selected.append(post)
        if len(selected) >= top_n:
            break

    top_clusters: list[DigestTopCluster] = []
    for post in selected:
        top_clusters.append(
            DigestTopCluster(
                title=_truncate(post.key_point, 120),
                why=post.why_it_matters,
                tags=post.tags,
                sources=[post.source],
                max_importance=post.importance,
                size=1,
                freshest_posted_at=post.posted_at,
            )
        )
    return top_clusters


def build_digest_data(
    *,
    start_at: datetime,
    end_at: datetime,
    window_id: int | None,
    top_n: int = 10,
) -> DigestData:
    settings = get_settings()
    timezone = ZoneInfo(settings.timezone)

    post_records = get_posts_for_digest(start_at=start_at, end_at=end_at)
    active_channels = get_active_channels()

    posts_by_channel: dict[int, list[DigestPostItem]] = {}
    all_posts: list[DigestPostItem] = []
    for record in post_records:
        post_item = _to_post_item(record)
        posts_by_channel.setdefault(record.channel_id, []).append(post_item)
        all_posts.append(post_item)

    top_clusters: list[DigestTopCluster]
    cluster_records = get_cluster_records(window_id) if window_id is not None else []
    if cluster_records:
        top_clusters = _build_top_clusters_from_dedup(cluster_records=cluster_records, top_n=top_n)
    else:
        top_clusters = _build_top_clusters_fallback(posts=all_posts, top_n=top_n)

    return DigestData(
        header=DigestHeader(
            digest_date=end_at.astimezone(timezone).date().isoformat(),
            timezone=settings.timezone,
            start_at=start_at,
            end_at=end_at,
        ),
        top_clusters=top_clusters,
        per_channel=_build_per_channel(channels=active_channels, posts_by_channel=posts_by_channel),
        top_limit=top_n,
    )
