# Yandex AI Guide

Yandex endpoints are used for summarization and embeddings through the OpenAI-compatible client.

## Required configuration
- Shared: `YANDEX_API_KEY`, `YANDEX_FOLDER_ID`
- Summarization: `YANDEX_MODEL_URI` (example: `gpt://<folder_id>/aliceai-llm`)
- Embeddings: `YANDEX_EMBED_MODEL_URI` (example: `emb://<folder_id>/text-search-doc/latest`)
- Vector size: `EMBED_DIM=256` (must match DB `VECTOR(256)` column)

## Commands
- Summaries: `aidigest summarize --limit 50`
- Embeddings: `aidigest embed --limit 200 --batch-size 10`

## Runtime behavior
- Summarize stage reuses exact-duplicate summaries by `content_hash` before calling LLM.
- Embedding stage sends **one text per API request** (multi-input batches are split).
- Retries are applied for network/timeouts, `429`, and `5xx`.

## Validation checks
- `aidigest doctor` verifies major settings and DB connectivity.
- Embedding payloads are validated for numeric/finite values and exact dimension length.

## Safety
- Never commit `.env` values or API keys.
- Do not log raw secrets in debug output.
