from __future__ import annotations

from datetime import datetime, timezone

from aidigest.digest.build import (
    DigestChannelSection,
    DigestData,
    DigestHeader,
    DigestPostItem,
    DigestTopCluster,
)
from aidigest.digest.format import render_digest_html


def test_render_digest_signal_only_format_includes_hidden_and_no_useful_message() -> None:
    data = DigestData(
        header=DigestHeader(
            digest_date="2026-02-07",
            timezone="Europe/Riga",
            start_at=datetime(2026, 2, 6, 11, 0, tzinfo=timezone.utc),
            end_at=datetime(2026, 2, 7, 11, 0, tzinfo=timezone.utc),
        ),
        top_clusters=[
            DigestTopCluster(
                post_id=1,
                posted_at=datetime(2026, 2, 7, 12, 0, tzinfo=timezone.utc),
                category="LLM_RELEASE",
                importance=5,
                why_it_matters="Откройте пост, чтобы понять влияние релиза на ваш стек.",
                permalink="https://t.me/a/1",
                source="@a",
                key_point="Релиз",
            )
        ],
        per_channel=[
            DigestChannelSection(
                channel_id=1,
                channel_name="@a",
                posts_count=1,
                hidden_posts=2,
                total_posts=3,
                posts=[
                    DigestPostItem(
                        post_id=2,
                        posted_at=datetime(2026, 2, 7, 10, 0, tzinfo=timezone.utc),
                        category="PRACTICE_INSIGHT",
                        importance=4,
                        key_point="Практика",
                        why_it_matters="Откройте пост, чтобы применить прием в текущем пайплайне.",
                        permalink="https://t.me/a/2",
                        content_hash="h2",
                        source="@a",
                    )
                ],
            ),
            DigestChannelSection(
                channel_id=2,
                channel_name="@b",
                posts_count=0,
                hidden_posts=1,
                total_posts=1,
                posts=[],
            ),
        ],
        top_limit=10,
    )

    messages = render_digest_html(data)
    rendered = "\n".join(messages)

    assert "[LLM_RELEASE][⭐5]" in rendered
    assert "[PRACTICE_INSIGHT][⭐4]" in rendered
    assert "Hidden: 2 low-value posts" in rendered
    assert "Нет полезных постов по критериям за окно." in rendered
    assert "Сводка:" not in rendered
    assert "Посты:" not in rendered
