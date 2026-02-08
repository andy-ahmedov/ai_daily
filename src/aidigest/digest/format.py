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


def _render_signal_line(
    *,
    posted_at,
    category: str,
    importance: int,
    why_it_matters: str,
    permalink: str | None,
    tz: ZoneInfo,
) -> str:
    posted_time = posted_at.astimezone(tz).strftime("%H:%M")
    line = (
        f"• <b>{escape(posted_time)}</b> "
        f"[{escape(category)}][⭐{importance}] {escape(_truncate(why_it_matters, 220))}"
    )
    if permalink:
        line += f' <a href="{escape(permalink, quote=True)}">🔗</a>'
    return line


def _render_top_cluster(cluster: DigestTopCluster, tz: ZoneInfo) -> str:
    return _render_signal_line(
        posted_at=cluster.posted_at,
        category=cluster.category,
        importance=cluster.importance,
        why_it_matters=cluster.why_it_matters,
        permalink=cluster.permalink,
        tz=tz,
    )


def _render_post(post: DigestPostItem, tz: ZoneInfo) -> str:
    return _render_signal_line(
        posted_at=post.posted_at,
        category=post.category,
        importance=post.importance,
        why_it_matters=post.why_it_matters,
        permalink=post.permalink,
        tz=tz,
    )


def _render_channel_section(section: DigestChannelSection, tz: ZoneInfo) -> str:
    lines: list[str] = [
        f"<b>{escape(section.channel_name)}</b> — показано {section.posts_count} из {section.total_posts}",
    ]
    if section.posts:
        lines.extend(_render_post(post, tz) for post in section.posts)
    else:
        lines.append("Нет полезных постов по критериям за окно.")
    if section.hidden_posts > 0:
        lines.append(f"<i>Hidden: {section.hidden_posts} low-value posts</i>")
    return "\n".join(lines)


def render_digest_html(digest_data: DigestData) -> list[str]:
    tz = ZoneInfo(digest_data.header.timezone)
    start = digest_data.header.start_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    end = digest_data.header.end_at.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    title = (
        f"<b>AI Digest</b> — {escape(digest_data.header.digest_date)}\n"
        f"<i>Окно: {escape(start)} → {escape(end)} ({escape(digest_data.header.timezone)})</i>\n\n"
        f"<b>Global Top-{digest_data.top_limit}</b>"
    )

    messages: list[str] = []
    current = title

    if not digest_data.top_clusters:
        current = _append_block(messages, current, "— Нет полезных постов для Global Top.")
    else:
        global_block = "\n".join(_render_top_cluster(cluster, tz) for cluster in digest_data.top_clusters)
        current = _append_block(messages, current, global_block)

    for section in digest_data.per_channel:
        current = _append_block(messages, current, _render_channel_section(section, tz))

    if current:
        messages.append(current)
    return messages
