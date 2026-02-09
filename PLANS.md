# PLANS.md — Digest v2 (Signal-only)

## Goal
Make the digest skimmable: per channel show up to TOP_K (max 5) valuable posts for the daily window (13:00→13:00 Europe/Riga).
Each item: HH:MM + category + ⭐ + ONE sentence "why it deserves my attention" + link.
No quotes/excerpts.

## Non-negotiables
- Window logic: Europe/Riga, 13:00 previous day -> 13:00 current day
- Telethon = user session only (no bot token)
- Bot API = publishing + bot commands
- Yandex embeddings dim=256, DB VECTOR(256)
- Publish idempotency by window_id

## User preference ranking (high -> low)
1) Major LLM releases/updates (OpenAI, Anthropic, DeepSeek, Qwen, etc.) => category=LLM_RELEASE, importance=5
2) Author insights / workflows / practical experience => PRACTICE_INSIGHT, importance=4
3) Analytical commentary / opinionated analysis of news => ANALYSIS_OPINION, importance=4
4) Deals/free access/subscriptions/credits => DEALS, importance=3-4
5) Other useful AI-related items => OTHER_USEFUL, importance=3
Noise (memes/ads/giveaways/games/non-AI) => NOISE, importance=1-2

## Output rules
- Per channel: show up to TOP_K=5 posts with importance >= MIN_IMPORTANCE_CHANNEL (default 3)
  - If none match: print "Нет полезных постов по критериям за окно."
  - Also print "Hidden: N low-value posts" if there were other posts in the channel.
- Global Top: show up to TOP_K_GLOBAL=10 items with importance >= MIN_IMPORTANCE_GLOBAL (default 4), deduped by clusters/hash.

## New bot command
- `/channel <ref> top-<N>`:
  - returns top N posts for this channel for the current window
  - same format: time + category + ⭐ + one-sentence why + link
  - If no useful posts: say so.

## Implementation steps
1) Update summarization prompt to output strict JSON including `category` and one-sentence `why_it_matters`.
2) (Optional) Add `category` column to post_summaries via Alembic if needed. Otherwise encode in tags.
3) Update digest builder/formatter to show only top items and hide noise.
4) Add bot handler for `/channel ... top-N`.
5) Add tests for selection logic.
