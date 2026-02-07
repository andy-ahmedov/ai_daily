from __future__ import annotations

from html import escape
from zoneinfo import ZoneInfo

from aidigest.digest.build import DigestChannelSection, DigestData, DigestPostItem, DigestTopCluster


MAX_MESSAGE_LEN = 3900


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _split_block(block: str, limit: int) -> list[str]:
    if len(block) <= limit:
        return [block]

    chunks: list[str] = []
    rest = block
    while len(rest) > limit:
        split_at = rest.rfind("\n", 0, limit)
        if split_at < limit // 3:
            split_at = limit
        chunk = rest[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        rest = rest[split_at:].lstrip("\n")
    if rest.strip():
        chunks.append(rest.strip())
    return chunks or [_truncate(block, limit)]


def _append_block(messages: list[str], current: str, block: str) -> str:
    for chunk in _split_block(block, MAX_MESSAGE_LEN):
        if not current:
            current = chunk
            continue
        candidate = f"{current}\n\n{chunk}"
        if len(candidate) <= MAX_MESSAGE_LEN:
            current = candidate
            continue
        messages.append(current)
        current = chunk
    return current


def _format_tags(tags: list[str]) -> str:
    if not tags:
        return "News"
    return tags[0]


def _render_top_cluster(rank: int, cluster: DigestTopCluster) -> str:
    tag = escape(_format_tags(cluster.tags))
    title = escape(cluster.title)
    lines = [f"{rank}) <b>[{tag}]</b> {title}"]
    if cluster.why:
        lines.append(f"   — {escape(cluster.why)}")
    if cluster.sources:
        lines.append(f"   Источники: {', '.join(escape(source) for source in cluster.sources)}")
    return "\n".join(lines)


def _render_post(post: DigestPostItem, tz: ZoneInfo) -> str:
    posted_time = post.posted_at.astimezone(tz).strftime("%H:%M")
    tag = escape(_format_tags(post.tags))
    key_point = escape(post.key_point)
    line = f"• <b>{posted_time}</b> [{tag}][⭐{post.importance}] {key_point}"
    if post.permalink:
        line += f' <a href="{escape(post.permalink, quote=True)}">🔗</a>'
    if post.why_it_matters:
        line += f"\n  — {escape(post.why_it_matters)}"
    return line


def _render_channel_section(section: DigestChannelSection, tz: ZoneInfo) -> list[str]:
    blocks: list[str] = []
    blocks.append(f"<b>{escape(section.channel_name)}</b> — {section.posts_count} постов")

    summary_lines = ["<i>Сводка:</i>"]
    if section.channel_summary:
        for bullet in section.channel_summary:
            summary_lines.append(f"• {escape(bullet)}")
    else:
        summary_lines.append("• Нет постов за окно")
    blocks.append("\n".join(summary_lines))

    posts_lines = ["<i>Посты:</i>"]
    if section.posts:
        posts_lines.extend(_render_post(post, tz) for post in section.posts)
    else:
        posts_lines.append("• Нет постов за окно")
    blocks.append("\n".join(posts_lines))
    return blocks


def render_digest_html(digest_data: DigestData) -> list[str]:
    tz = ZoneInfo(digest_data.header.timezone)
    start = digest_data.header.start_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    end = digest_data.header.end_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    title = (
        f"<b>AI Digest</b> — {escape(digest_data.header.digest_date)}\n"
        f"<i>Окно: {escape(start)} → {escape(end)} ({escape(digest_data.header.timezone)})</i>\n\n"
        f"<b>Top-{digest_data.top_limit} дня (без дублей)</b>"
    )

    messages: list[str] = []
    current = title

    if not digest_data.top_clusters:
        current = _append_block(messages, current, "— Нет данных для Top дня")
    else:
        for idx, cluster in enumerate(digest_data.top_clusters, start=1):
            current = _append_block(messages, current, _render_top_cluster(idx, cluster))

    for section in digest_data.per_channel:
        for block in _render_channel_section(section, tz):
            current = _append_block(messages, current, block)

    if current:
        messages.append(current)
    return messages
