from __future__ import annotations

import hashlib
import re
from datetime import datetime


_ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_INLINE_WHITESPACE_RE = re.compile(r"[ \t]+")
_EMPTY_LINES_RE = re.compile(r"\n{3,}")

# Keep this list explicit and easy to extend with new recurring tails.
_TAIL_STOP_PATTERNS = [
    re.compile(r"^подписывай(тесь|ся)\b.*", re.IGNORECASE),
    re.compile(r"^реклама\b.*", re.IGNORECASE),
    re.compile(r"^источник:?.*", re.IGNORECASE),
    re.compile(r"^читайте также\b.*", re.IGNORECASE),
    re.compile(r"^(https?://)?t\.me/\S+$", re.IGNORECASE),
    re.compile(r"^поддерж(ать|ите) канал\b.*", re.IGNORECASE),
    re.compile(r".*\bdonate\b.*", re.IGNORECASE),
]


def _is_tail_stop_line(line: str) -> bool:
    for pattern in _TAIL_STOP_PATTERNS:
        if pattern.match(line):
            return True
    return False


def normalize_text(text: str) -> str:
    without_zero_width = _ZERO_WIDTH_RE.sub("", text or "")
    normalized_newlines = without_zero_width.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized_newlines.split("\n")

    compact_lines: list[str] = []
    for raw_line in lines:
        line = _INLINE_WHITESPACE_RE.sub(" ", raw_line).strip()
        if line == "":
            if compact_lines and compact_lines[-1] == "":
                continue
            compact_lines.append("")
            continue
        compact_lines.append(line)

    while compact_lines and compact_lines[-1] == "":
        compact_lines.pop()

    while compact_lines and _is_tail_stop_line(compact_lines[-1]):
        compact_lines.pop()
        while compact_lines and compact_lines[-1] == "":
            compact_lines.pop()

    while compact_lines and compact_lines[0] == "":
        compact_lines.pop(0)

    joined = "\n".join(compact_lines).strip()
    return _EMPTY_LINES_RE.sub("\n\n", joined)


def compute_content_hash(
    text_norm: str | None,
    *,
    has_media: bool,
    permalink: str | None,
    posted_at: datetime,
) -> str:
    normalized = (text_norm or "").strip()
    if normalized:
        payload = normalized
    elif has_media:
        payload = f"media-only:{str(posted_at)}:{str(permalink or '')}"
    else:
        payload = f"empty:{str(posted_at)}:{str(permalink or '')}"

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
