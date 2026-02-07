from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def compute_window(
    target_date: date,
    tz: str,
    start_hour: int = 13,
) -> tuple[datetime, datetime]:
    if not 0 <= start_hour <= 23:
        raise ValueError("start_hour must be in range 0..23")

    timezone = ZoneInfo(tz)
    end_at = datetime.combine(target_date, time(hour=start_hour), tzinfo=timezone)
    start_at = end_at - timedelta(days=1)
    return start_at, end_at
