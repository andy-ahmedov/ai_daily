from __future__ import annotations

import pytest

from aidigest.nlp.summarize import _normalize_summary_payload


@pytest.mark.parametrize(
    ("category", "raw_importance", "expected_importance"),
    [
        ("LLM_RELEASE", 1, 5),
        ("PRACTICE_INSIGHT", 2, 4),
        ("ANALYSIS_OPINION", 5, 4),
        ("DEALS", 5, 4),
        ("DEALS", 2, 3),
        ("OTHER_USEFUL", 5, 3),
        ("NOISE", 5, 2),
        ("NOISE", 0, 1),
    ],
)
def test_category_importance_mapping(category: str, raw_importance: int, expected_importance: int) -> None:
    payload = {
        "key_point": "Короткое описание обновления.",
        "why_it_matters": "Откройте пост, чтобы быстро понять пользу для вашей работы.",
        "tags": ["News"],
        "category": category,
        "importance": raw_importance,
    }

    summary = _normalize_summary_payload(payload, post_text="AI release notes")

    assert summary.category == category
    assert summary.importance == expected_importance


def test_unknown_category_defaults_to_other_useful() -> None:
    payload = {
        "key_point": "Полезное обновление инструмента.",
        "why_it_matters": "Откройте пост, чтобы оценить практическую пользу. Вторая фраза не нужна!",
        "tags": ["Tools"],
        "category": "SOMETHING_ELSE",
        "importance": 1,
    }

    summary = _normalize_summary_payload(payload, post_text="tool update for AI workflows")

    assert summary.category == "OTHER_USEFUL"
    assert summary.importance == 3
    assert summary.why_it_matters.count(".") == 1
    assert "Вторая фраза" not in summary.why_it_matters


def test_noise_detection_overrides_category_and_importance() -> None:
    payload = {
        "key_point": "Розыгрыш с мемами и призами.",
        "why_it_matters": "Откройте пост и выиграйте.",
        "tags": ["Business"],
        "category": "PRACTICE_INSIGHT",
        "importance": 5,
    }

    summary = _normalize_summary_payload(
        payload,
        post_text="Большой розыгрыш, мемы и конкурс среди подписчиков",
    )

    assert summary.category == "NOISE"
    assert 1 <= summary.importance <= 2


def test_why_it_matters_uses_fallback_when_copying_source_fragment() -> None:
    source = "Новая модель уже доступна сегодня в API и поддерживает длинный контекст для сложных задач."
    payload = {
        "key_point": "Релиз новой модели.",
        "why_it_matters": source,
        "tags": ["News"],
        "category": "LLM_RELEASE",
        "importance": 5,
    }

    summary = _normalize_summary_payload(payload, post_text=source)

    assert summary.why_it_matters.startswith("Откройте пост, чтобы")
    assert summary.why_it_matters.endswith(".")
