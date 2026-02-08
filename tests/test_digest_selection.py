from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from aidigest.db.repo_digest import DigestClusterRecord, DigestPostRecord
from aidigest.digest import build as build_module


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 2, 7, hour, minute, tzinfo=timezone.utc)


def _settings(**overrides: object) -> SimpleNamespace:
    defaults: dict[str, object] = {
        "timezone": "Europe/Riga",
        "top_k_per_channel": 2,
        "min_importance_channel": 3,
        "top_k_global": 2,
        "min_importance_global": 4,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _post(
    *,
    post_id: int,
    channel_id: int,
    posted_at: datetime,
    importance: int,
    category: str,
    content_hash: str,
) -> DigestPostRecord:
    return DigestPostRecord(
        post_id=post_id,
        channel_id=channel_id,
        channel_title=f"Channel {channel_id}",
        channel_username=f"chan{channel_id}",
        posted_at=posted_at,
        text=f"text-{post_id}",
        permalink=f"https://t.me/c/{channel_id}/{post_id}",
        content_hash=content_hash,
        key_point=f"kp-{post_id}",
        why_it_matters=f"why-{post_id}.",
        tags=["News"],
        importance=importance,
        category=category,
    )


def _cluster(
    *,
    cluster_id: int,
    representative_post_id: int | None,
    post_id: int,
    posted_at: datetime,
    importance: int,
    category: str,
) -> DigestClusterRecord:
    return DigestClusterRecord(
        cluster_id=cluster_id,
        representative_post_id=representative_post_id,
        post_id=post_id,
        similarity=0.9,
        channel_title=f"Channel {cluster_id}",
        channel_username=f"chan{cluster_id}",
        posted_at=posted_at,
        text=f"text-{post_id}",
        permalink=f"https://t.me/c/{cluster_id}/{post_id}",
        content_hash=f"h-{post_id}",
        key_point=f"kp-{post_id}",
        why_it_matters=f"why-{post_id}.",
        tags=["News"],
        importance=importance,
        category=category,
    )


def test_build_digest_data_applies_top_k_and_hidden_counts(monkeypatch) -> None:
    channels = [
        SimpleNamespace(id=1, title="Channel 1", username="chan1"),
        SimpleNamespace(id=2, title="Channel 2", username="chan2"),
    ]
    posts = [
        _post(
            post_id=1,
            channel_id=1,
            posted_at=_dt(10, 0),
            importance=4,
            category="OTHER_USEFUL",
            content_hash="h1",
        ),
        _post(
            post_id=2,
            channel_id=1,
            posted_at=_dt(11, 0),
            importance=4,
            category="PRACTICE_INSIGHT",
            content_hash="h2",
        ),
        _post(
            post_id=3,
            channel_id=1,
            posted_at=_dt(12, 0),
            importance=5,
            category="LLM_RELEASE",
            content_hash="h3",
        ),
        _post(
            post_id=4,
            channel_id=1,
            posted_at=_dt(12, 30),
            importance=5,
            category="NOISE",
            content_hash="h4",
        ),
        _post(
            post_id=5,
            channel_id=1,
            posted_at=_dt(13, 0),
            importance=2,
            category="OTHER_USEFUL",
            content_hash="h5",
        ),
        _post(
            post_id=6,
            channel_id=2,
            posted_at=_dt(9, 0),
            importance=2,
            category="OTHER_USEFUL",
            content_hash="h6",
        ),
        _post(
            post_id=7,
            channel_id=2,
            posted_at=_dt(9, 30),
            importance=1,
            category="NOISE",
            content_hash="h7",
        ),
    ]

    monkeypatch.setattr(build_module, "get_settings", lambda: _settings())
    monkeypatch.setattr(build_module, "get_active_channels", lambda: channels)
    monkeypatch.setattr(build_module, "get_posts_for_digest", lambda **_: posts)
    monkeypatch.setattr(build_module, "get_cluster_records", lambda _window_id: [])

    digest = build_module.build_digest_data(
        start_at=_dt(0, 0),
        end_at=_dt(23, 0),
        window_id=None,
    )

    channel_one = digest.per_channel[0]
    assert [post.post_id for post in channel_one.posts] == [3, 2]
    assert channel_one.posts_count == 2
    assert channel_one.hidden_posts == 3
    assert channel_one.total_posts == 5

    channel_two = digest.per_channel[1]
    assert channel_two.posts == []
    assert channel_two.posts_count == 0
    assert channel_two.hidden_posts == 2
    assert channel_two.total_posts == 2


def test_build_digest_data_global_top_uses_cluster_representatives(monkeypatch) -> None:
    cluster_records = [
        _cluster(
            cluster_id=1,
            representative_post_id=11,
            post_id=11,
            posted_at=_dt(10, 0),
            importance=5,
            category="LLM_RELEASE",
        ),
        _cluster(
            cluster_id=2,
            representative_post_id=21,
            post_id=21,
            posted_at=_dt(11, 0),
            importance=5,
            category="NOISE",
        ),
        _cluster(
            cluster_id=3,
            representative_post_id=31,
            post_id=31,
            posted_at=_dt(12, 0),
            importance=4,
            category="ANALYSIS_OPINION",
        ),
        _cluster(
            cluster_id=4,
            representative_post_id=41,
            post_id=41,
            posted_at=_dt(13, 0),
            importance=3,
            category="OTHER_USEFUL",
        ),
        _cluster(
            cluster_id=5,
            representative_post_id=51,
            post_id=51,
            posted_at=_dt(14, 0),
            importance=3,
            category="OTHER_USEFUL",
        ),
        _cluster(
            cluster_id=5,
            representative_post_id=51,
            post_id=52,
            posted_at=_dt(14, 5),
            importance=5,
            category="LLM_RELEASE",
        ),
    ]

    monkeypatch.setattr(build_module, "get_settings", lambda: _settings(top_k_global=5))
    monkeypatch.setattr(build_module, "get_active_channels", lambda: [])
    monkeypatch.setattr(build_module, "get_posts_for_digest", lambda **_: [])
    monkeypatch.setattr(build_module, "get_cluster_records", lambda _window_id: cluster_records)

    digest = build_module.build_digest_data(
        start_at=_dt(0, 0),
        end_at=_dt(23, 0),
        window_id=123,
    )

    assert [item.post_id for item in digest.top_clusters] == [11, 31]
    assert all(item.category != "NOISE" for item in digest.top_clusters)
    assert all(item.importance >= 4 for item in digest.top_clusters)
