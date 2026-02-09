from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from aidigest.bot_commands.handlers import (
    _build_channel_description,
    _build_channel_descriptions_with_llm,
    _parse_channel_command_args,
    _render_channel_top_line,
    _select_channel_useful_posts,
    _split_lines_for_telegram,
)
from aidigest.db.repo_digest import DigestPostRecord


def _record(
    *,
    post_id: int,
    posted_at: datetime,
    importance: int | None,
    category: str | None,
) -> DigestPostRecord:
    return DigestPostRecord(
        post_id=post_id,
        channel_id=1,
        channel_title="Channel",
        channel_username="channel",
        posted_at=posted_at,
        text="text",
        permalink="https://t.me/test/1",
        content_hash=f"h{post_id}",
        key_point="key point",
        why_it_matters="why it matters.",
        tags=["News"],
        importance=importance,
        category=category,
    )


def test_parse_channel_command_args() -> None:
    assert _parse_channel_command_args("@whackdoor top-5") == ("@whackdoor", 5)
    assert _parse_channel_command_args("@whackdoor top-10") == ("@whackdoor", 10)
    assert _parse_channel_command_args("  @whackdoor   top-3 ") == ("@whackdoor", 3)
    assert _parse_channel_command_args("@whackdoor") is None
    assert _parse_channel_command_args("@whackdoor top-0") is None
    assert _parse_channel_command_args(None) is None


def test_select_channel_useful_posts_filters_noise_and_importance() -> None:
    posts = [
        _record(
            post_id=1,
            posted_at=datetime(2026, 2, 7, 10, 0, tzinfo=timezone.utc),
            importance=4,
            category="ANALYSIS_OPINION",
        ),
        _record(
            post_id=2,
            posted_at=datetime(2026, 2, 7, 11, 0, tzinfo=timezone.utc),
            importance=5,
            category="NOISE",
        ),
        _record(
            post_id=3,
            posted_at=datetime(2026, 2, 7, 12, 0, tzinfo=timezone.utc),
            importance=3,
            category="OTHER_USEFUL",
        ),
        _record(
            post_id=4,
            posted_at=datetime(2026, 2, 7, 13, 0, tzinfo=timezone.utc),
            importance=2,
            category="OTHER_USEFUL",
        ),
    ]

    selected = _select_channel_useful_posts(posts=posts, min_importance=3, top_n=2)

    assert [item.post_id for item in selected] == [1, 3]


def test_split_lines_for_telegram_respects_limit() -> None:
    lines = [f"line {idx}" for idx in range(400)]
    chunks = _split_lines_for_telegram(lines, limit=120)

    assert len(chunks) > 1
    assert all(len(chunk) <= 120 for chunk in chunks)


def test_render_channel_top_line_contains_date_and_clickable_link_text() -> None:
    line = _render_channel_top_line(
        record=_record(
            post_id=11,
            posted_at=datetime(2026, 2, 8, 4, 25, tzinfo=timezone.utc),
            importance=5,
            category="LLM_RELEASE",
        ),
        tz=ZoneInfo("Asia/Dubai"),
    )

    assert "2026-02-08 08:25" in line
    assert "[LLM_RELEASE][⭐5]" in line
    assert ">ссылка<" in line


def test_build_channel_description_expands_for_long_posts() -> None:
    record = _record(
        post_id=99,
        posted_at=datetime(2026, 2, 8, 4, 25, tzinfo=timezone.utc),
        importance=4,
        category="PRACTICE_INSIGHT",
    )
    record.why_it_matters = "Откройте пост, чтобы понять, как применить подход в продакшене."
    record.key_point = "Автор показывает пошаговый процесс интеграции и частые ошибки."
    record.text = " ".join([f"слово{i}" for i in range(1, 170)])

    description = _build_channel_description(record)
    words = len(description.split())

    assert 40 <= words <= 60


def test_build_channel_descriptions_with_llm_truncates_to_max(monkeypatch) -> None:
    record = _record(
        post_id=77,
        posted_at=datetime(2026, 2, 8, 4, 25, tzinfo=timezone.utc),
        importance=5,
        category="LLM_RELEASE",
    )
    record.text = " ".join([f"слово{i}" for i in range(1, 180)])

    long_description = " ".join([f"w{i}" for i in range(1, 80)])
    monkeypatch.setattr("aidigest.bot_commands.handlers.make_client", lambda _settings: object())
    monkeypatch.setattr(
        "aidigest.bot_commands.handlers.chat_json",
        lambda **kwargs: {"description": long_description},
    )
    monkeypatch.setattr("aidigest.bot_commands.handlers.time.sleep", lambda *_args, **_kwargs: None)

    settings = SimpleNamespace(
        yandex_api_key="x",
        yandex_folder_id="y",
        yandex_model_uri="gpt://folder/model",
    )
    result = _build_channel_descriptions_with_llm([record], settings)

    assert record.post_id in result
    words = len(result[record.post_id].split())
    assert words <= 60
