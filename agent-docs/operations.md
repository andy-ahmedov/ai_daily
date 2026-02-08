# Operations Runbook

## First-time setup
1. `python -m venv .venv && source .venv/bin/activate`
2. `pip install -U pip && pip install -e ".[dev]"`
3. `docker compose up -d`
4. `cp .env.example .env` and fill required variables
5. `alembic upgrade head`
6. `aidigest doctor`

## Daily operations
- Manual full run: `aidigest run-once --date YYYY-MM-DD`
- Scheduled service mode: `aidigest scheduler:run`
- Bot-triggered run: `/digest-now` in Telegram bot chat.

## Health checks
- DB reachable and schema migrated: `aidigest doctor`
- Active channels: `aidigest tg:list`
- Pipeline status snapshot: `/status` in bot chat.

## Common incidents
- No posts ingested: re-check Telethon user session with `aidigest tg:whoami`.
- Summaries/embeddings not produced: verify Yandex env vars and model URIs.
- Publish skipped unexpectedly: digest may already be published for the same window (idempotent behavior).
- Publish errors: verify `BOT_TOKEN` and `DIGEST_CHANNEL_ID` format (`-100...` channel id).

## Release checklist (minimal)
1. `ruff check .`
2. `pytest`
3. Run one real or dry-run pipeline command for the target window.
4. Include migration files in the same change when DB schema is modified.
