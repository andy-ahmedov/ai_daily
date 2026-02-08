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

CATEGORIES = (
    "LLM_RELEASE",
    "PRACTICE_INSIGHT",
    "ANALYSIS_OPINION",
    "DEALS",
    "OTHER_USEFUL",
    "NOISE",
)

SYSTEM_PROMPT = (
    "You are a strict summarizer. Return only valid JSON. "
    "Do not include markdown. Keep key_point concise and factual. "
    "why_it_matters must be exactly one sentence in Russian and must explain why the user should open the post."
)


def build_post_prompt(post: Post) -> str:
    channel_title = getattr(post, "channel_title", "") or ""
    text = (post.text or "").strip()
    text_block = text if text else "<EMPTY_TEXT>"
    permalink = post.permalink or ""

    return (
        "Summarize this Telegram post in Russian for an AI-news digest.\n"
        "Return JSON with keys: key_point, why_it_matters, tags, category, importance.\n"
        "Rules:\n"
        "- key_point: required, <= 160 chars.\n"
        "- why_it_matters: required, exactly ONE sentence, <= 200 chars.\n"
        "- why_it_matters must explain why user should open the post now.\n"
        "- why_it_matters must NOT include quotes, excerpts, or copied fragments from the post.\n"
        f"- tags: array, allowed values only: {', '.join(ALLOWED_TAGS)}.\n"
        f"- category: one of {', '.join(CATEGORIES)}.\n"
        "- category rules:\n"
        "  * LLM_RELEASE for new model/product release announcements from AI vendors.\n"
        "  * PRACTICE_INSIGHT for practical workflows, implementation tips, prompts, evals.\n"
        "  * ANALYSIS_OPINION for analysis, commentary, comparisons, long-form opinions.\n"
        "  * DEALS for discounts, promo access, paid offers.\n"
        "  * OTHER_USEFUL for useful AI updates that do not fit above.\n"
        "  * NOISE for ads, giveaways, games, memes, and non-AI topics.\n"
        "- importance must follow category mapping:\n"
        "  * LLM_RELEASE => 5\n"
        "  * PRACTICE_INSIGHT => 4\n"
        "  * ANALYSIS_OPINION => 4\n"
        "  * DEALS => 3..4\n"
        "  * OTHER_USEFUL => 3\n"
        "  * NOISE => 1..2\n"
        "Post metadata:\n"
        f"- post_id: {post.id}\n"
        f"- channel_title: {channel_title}\n"
        f"- posted_at: {post.posted_at.isoformat()}\n"
        f"- has_media: {str(bool(post.has_media)).lower()}\n"
        f"- permalink: {permalink}\n"
        "Post text:\n"
        f"{text_block}\n"
    )
