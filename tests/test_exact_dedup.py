from __future__ import annotations

from datetime import datetime, timezone

from aidigest.ingest.normalize import compute_content_hash, normalize_text


def test_content_hash_same_for_whitespace_variants() -> None:
    text_a = "Привет,   мир!\n\nНовая строка."
    text_b = "Привет, мир!\r\n\r\nНовая строка.   "

    normalized_a = normalize_text(text_a)
    normalized_b = normalize_text(text_b)
    assert normalized_a == normalized_b

    posted_at = datetime(2026, 2, 7, 10, 30, tzinfo=timezone.utc)
    hash_a = compute_content_hash(
        normalized_a,
        has_media=False,
        permalink="https://t.me/test/1",
        posted_at=posted_at,
    )
    hash_b = compute_content_hash(
        normalized_b,
        has_media=False,
        permalink="https://t.me/test/1",
        posted_at=posted_at,
    )
    assert hash_a == hash_b


def test_content_hash_same_when_trailing_promo_removed() -> None:
    base = "Важная новость дня\nПодробности внутри."
    with_tail = f"{base}\n\nПодписывайтесь на канал @example"

    normalized_base = normalize_text(base)
    normalized_tail = normalize_text(with_tail)
    assert normalized_base == normalized_tail

    posted_at = datetime(2026, 2, 7, 10, 30, tzinfo=timezone.utc)
    hash_base = compute_content_hash(
        normalized_base,
        has_media=False,
        permalink="https://t.me/test/2",
        posted_at=posted_at,
    )
    hash_tail = compute_content_hash(
        normalized_tail,
        has_media=False,
        permalink="https://t.me/test/2",
        posted_at=posted_at,
    )
    assert hash_base == hash_tail
