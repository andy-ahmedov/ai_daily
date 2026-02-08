# Digest Format Guide

Digest messages are rendered as Telegram HTML blocks in `src/aidigest/digest/format.py`.

## Output structure
- Header:
  - `<b>AI Digest</b> — YYYY-MM-DD`
  - Window line with timezone
  - `Top-N` section title
- Top section:
  - Ranked clusters `1)`, `2)`, ...
  - Tag in brackets, title, optional reason, source list
- Per-channel sections:
  - Channel title and post count
  - Channel summary bullets
  - Post bullets with local time, tag, importance, key point, optional link and "why it matters"

## Formatting constraints
- Telegram `parse_mode=HTML`.
- Maximum message size target: `3900` chars (`MAX_MESSAGE_LEN`).
- Long blocks are split by newline when possible; otherwise hard-split.
- User text is HTML-escaped before rendering.

## Editing rules
- Keep rendered output compact and scannable; avoid long paragraphs.
- Preserve fallback behavior:
  - No top data -> `Нет данных для Top дня`
  - Empty channel/post lists -> `Нет постов за окно`
- If you add fields, ensure tests for splitting/formatting still pass (`tests/test_format_split.py`).
