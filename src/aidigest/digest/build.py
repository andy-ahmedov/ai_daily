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

_ALLOWED_CATEGORIES = {
    "LLM_RELEASE",
    "PRACTICE_INSIGHT",
    "ANALYSIS_OPINION",
    "DEALS",
    "OTHER_USEFUL",
    "NOISE",
}
_WHY_FALLBACK = "Откройте пост, чтобы быстро понять, есть ли практическая польза для ваших задач."


@dataclass(slots=True)
class DigestHeader:
    digest_date: str
    timezone: str
    start_at: datetime
    end_at: datetime


@dataclass(slots=True)
class DigestTopCluster:
    post_id: int
    posted_at: datetime
    category: str
    importance: int
    why_it_matters: str
    permalink: str | None
    source: str
    key_point: str


@dataclass(slots=True)
class DigestPostItem:
    post_id: int
    posted_at: datetime
    category: str
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
    hidden_posts: int = 0
    total_posts: int = 0
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


def _normalize_category(category: str | None) -> str:
    normalized = str(category or "").strip().upper()
    if normalized in _ALLOWED_CATEGORIES:
        return normalized
    return "OTHER_USEFUL"


def _coalesce_summary(
    *,
    key_point: str | None,
    why_it_matters: str | None,
    importance: int | None,
    category: str | None,
    text: str | None,
) -> tuple[str, str, int, str]:
    normalized_text = normalize_text(text or "")
    fallback_key_point = _truncate(normalized_text or "Пост без текста", 120)

    normalized_key_point = (key_point or "").strip() or fallback_key_point
    normalized_why = (why_it_matters or "").strip() or _WHY_FALLBACK
    normalized_importance = int(importance if importance is not None else 2)
    normalized_importance = min(5, max(1, normalized_importance))
    normalized_category = _normalize_category(category)
    return normalized_key_point, normalized_why, normalized_importance, normalized_category


def _to_post_item(record: DigestPostRecord) -> DigestPostItem:
    key_point, why, importance, category = _coalesce_summary(
        key_point=record.key_point,
        why_it_matters=record.why_it_matters,
        importance=record.importance,
        category=record.category,
        text=record.text,
    )
    return DigestPostItem(
        post_id=record.post_id,
        posted_at=record.posted_at,
        category=category,
        importance=importance,
        key_point=key_point,
        why_it_matters=why,
        permalink=record.permalink,
        content_hash=record.content_hash,
        source=_source_name(record.channel_username, record.channel_title),
    )


def _to_top_item(record: DigestClusterRecord) -> DigestTopCluster:
    key_point, why, importance, category = _coalesce_summary(
        key_point=record.key_point,
        why_it_matters=record.why_it_matters,
        importance=record.importance,
        category=record.category,
        text=record.text,
    )
    return DigestTopCluster(
        post_id=record.post_id,
        posted_at=record.posted_at,
        category=category,
        importance=importance,
        why_it_matters=why,
        permalink=record.permalink,
        source=_source_name(record.channel_username, record.channel_title),
        key_point=_truncate(key_point, 80),
    )


def _is_signal(*, category: str, importance: int, min_importance: int) -> bool:
    return category != "NOISE" and importance >= min_importance


def _pick_representative(cluster_records: list[DigestClusterRecord]) -> DigestClusterRecord:
    preferred_id = cluster_records[0].representative_post_id
    if preferred_id is not None:
        for row in cluster_records:
            if row.post_id == preferred_id:
                return row
    return max(
        cluster_records,
        key=lambda row: (
            int(row.importance) if row.importance is not None else 0,
            row.posted_at,
            row.post_id,
        ),
    )


def _build_per_channel(
    channels: list[Channel],
    posts_by_channel: dict[int, list[DigestPostItem]],
    *,
    top_k_per_channel: int,
    min_importance_channel: int,
) -> list[DigestChannelSection]:
    sections: list[DigestChannelSection] = []
    for channel in channels:
        all_posts = posts_by_channel.get(channel.id, [])
        ranked_signal = sorted(
            [
                post
                for post in all_posts
                if _is_signal(
                    category=post.category,
                    importance=post.importance,
                    min_importance=min_importance_channel,
                )
            ],
            key=lambda item: (item.importance, item.posted_at),
            reverse=True,
        )
        shown_posts = ranked_signal[:top_k_per_channel]
        hidden_posts = max(0, len(all_posts) - len(shown_posts))
        channel_name = _source_name(channel.username, channel.title)
        sections.append(
            DigestChannelSection(
                channel_id=channel.id,
                channel_name=channel_name,
                posts_count=len(shown_posts),
                hidden_posts=hidden_posts,
                total_posts=len(all_posts),
                posts=shown_posts,
            )
        )
    return sections


def _build_top_clusters_from_dedup(
    cluster_records: list[DigestClusterRecord],
    *,
    top_n: int,
    min_importance_global: int,
) -> list[DigestTopCluster]:
    grouped: dict[int, list[DigestClusterRecord]] = {}
    for row in cluster_records:
        grouped.setdefault(row.cluster_id, []).append(row)

    selected: list[DigestTopCluster] = []
    for rows in grouped.values():
        representative = _pick_representative(rows)
        item = _to_top_item(representative)
        if _is_signal(
            category=item.category,
            importance=item.importance,
            min_importance=min_importance_global,
        ):
            selected.append(item)

    ranked = sorted(selected, key=lambda item: (item.importance, item.posted_at), reverse=True)
    return ranked[:top_n]


def _build_top_clusters_fallback(
    posts: list[DigestPostItem],
    *,
    top_n: int,
    min_importance_global: int,
) -> list[DigestTopCluster]:
    ranked = sorted(posts, key=lambda post: (post.importance, post.posted_at), reverse=True)
    selected: list[DigestTopCluster] = []
    seen_hashes: set[str] = set()

    for post in ranked:
        if post.content_hash in seen_hashes:
            continue
        seen_hashes.add(post.content_hash)
        if not _is_signal(
            category=post.category,
            importance=post.importance,
            min_importance=min_importance_global,
        ):
            continue
        selected.append(
            DigestTopCluster(
                post_id=post.post_id,
                posted_at=post.posted_at,
                category=post.category,
                importance=post.importance,
                why_it_matters=post.why_it_matters,
                permalink=post.permalink,
                source=post.source,
                key_point=_truncate(post.key_point, 80),
            )
        )
        if len(selected) >= top_n:
            break
    return selected


def build_digest_data(
    *,
    start_at: datetime,
    end_at: datetime,
    window_id: int | None,
    top_n: int | None = None,
) -> DigestData:
    settings = get_settings()
    timezone = ZoneInfo(settings.timezone)
    top_limit = int(top_n if top_n is not None else settings.top_k_global)

    post_records = get_posts_for_digest(start_at=start_at, end_at=end_at)
    active_channels = get_active_channels()

    posts_by_channel: dict[int, list[DigestPostItem]] = {}
    all_posts: list[DigestPostItem] = []
    for record in post_records:
        post_item = _to_post_item(record)
        posts_by_channel.setdefault(record.channel_id, []).append(post_item)
        all_posts.append(post_item)

    cluster_records = get_cluster_records(window_id) if window_id is not None else []
    if cluster_records:
        top_clusters = _build_top_clusters_from_dedup(
            cluster_records=cluster_records,
            top_n=top_limit,
            min_importance_global=settings.min_importance_global,
        )
    else:
        top_clusters = _build_top_clusters_fallback(
            posts=all_posts,
            top_n=top_limit,
            min_importance_global=settings.min_importance_global,
        )

    return DigestData(
        header=DigestHeader(
            digest_date=end_at.astimezone(timezone).date().isoformat(),
            timezone=settings.timezone,
            start_at=start_at,
            end_at=end_at,
        ),
        top_clusters=top_clusters,
        per_channel=_build_per_channel(
            channels=active_channels,
            posts_by_channel=posts_by_channel,
            top_k_per_channel=settings.top_k_per_channel,
            min_importance_channel=settings.min_importance_channel,
        ),
        top_limit=top_limit,
    )
