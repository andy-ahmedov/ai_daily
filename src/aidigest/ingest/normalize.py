from __future__ import annotations

import hashlib
import re
from datetime import datetime


_ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    without_zero_width = _ZERO_WIDTH_RE.sub("", text)
    return _WHITESPACE_RE.sub(" ", without_zero_width).strip()


def compute_content_hash(
    text: str | None,
    *,
    has_media: bool,
    message_id: int,
    posted_at: datetime,
) -> str:
    normalized = normalize_text(text or "")
    if normalized:
        payload = normalized
    elif has_media:
        payload = f"media:{message_id}:{posted_at.isoformat()}"
    else:
        payload = f"empty:{message_id}:{posted_at.isoformat()}"

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

