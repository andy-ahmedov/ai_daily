from __future__ import annotations

from aidigest.db.models import Post

ALLOWED_TAGS = (
    "News",
    "Research",
    "Tools",
    "Product",
    "Opinion",
    "Safety",
    "Policy",
    "Business",
)

SYSTEM_PROMPT = (
    "You are a strict summarizer. Return only valid JSON. "
    "Do not include markdown. Keep key_point concise and factual."
)


def build_post_prompt(post: Post) -> str:
    channel_title = getattr(post, "channel_title", "") or ""
    text = (post.text or "").strip()
    text_block = text if text else "<EMPTY_TEXT>"
    permalink = post.permalink or ""

    return (
        "Summarize this Telegram post in Russian.\n"
        "Return JSON with keys: key_point, why_it_matters, tags, importance.\n"
        "Rules:\n"
        "- key_point: required, <= 160 chars.\n"
        "- why_it_matters: optional, <= 200 chars, empty string allowed.\n"
        f"- tags: array, allowed values only: {', '.join(ALLOWED_TAGS)}.\n"
        "- importance: integer 1..5.\n"
        "Post metadata:\n"
        f"- post_id: {post.id}\n"
        f"- channel_title: {channel_title}\n"
        f"- posted_at: {post.posted_at.isoformat()}\n"
        f"- has_media: {str(bool(post.has_media)).lower()}\n"
        f"- permalink: {permalink}\n"
        "Post text:\n"
        f"{text_block}\n"
    )
