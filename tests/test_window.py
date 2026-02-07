from __future__ import annotations

from datetime import date, timedelta

from aidigest.ingest.window import compute_window


def test_compute_window_riga_fixed_date() -> None:
    start_at, end_at = compute_window(
        target_date=date(2026, 2, 7),
        tz="Europe/Riga",
        start_hour=13,
    )

    assert start_at.tzinfo is not None
    assert end_at.tzinfo is not None
    assert start_at.isoformat().endswith("+02:00")
    assert end_at.isoformat().endswith("+02:00")

    assert start_at.date() == date(2026, 2, 6)
    assert end_at.date() == date(2026, 2, 7)
    assert start_at.hour == 13
    assert end_at.hour == 13
    assert start_at < end_at
    assert end_at - start_at == timedelta(days=1)
