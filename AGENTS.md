# Repository Guidelines

## Project Structure & Module Organization
Core code lives in `src/aidigest/` and is split by domain:
- `ingest/`, `nlp/`, `digest/`, `telegram/`, `scheduler/`, `db/`, and `bot_commands/`.
- CLI entrypoint: `src/aidigest/cli.py` (`aidigest` command).
- Database migrations: `alembic/` and `alembic/versions/`.
- Tests: `tests/` (pytest, file names like `test_window.py`).
- Runtime artifacts: `data/` (for example `telethon.session`), local env in `.env`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate`: create and activate local environment.
- `pip install -U pip && pip install -e ".[dev]"`: install app + dev tooling.
- `docker compose up -d`: start PostgreSQL/pgvector.
- `alembic upgrade head`: apply schema migrations.
- `aidigest doctor`: validate config and DB connectivity.
- `ruff check .` / `ruff format .`: lint and format code.
- `pytest`: run test suite.
- `aidigest run-once --date 2026-02-07`: run full pipeline for a specific window.

## Coding Style & Naming Conventions
- Python 3.11+, 4-space indentation, explicit type hints for new/changed code.
- Keep lines readable; Ruff target line length is 100.
- Ruff lint rules enabled: `E`, `F`, `I` (import sorting included).
- Naming: `snake_case` for modules/functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.
- Prefer small, focused functions around pipeline stages and DB repositories.

## Testing Guidelines
- Framework: `pytest` (`[tool.pytest.ini_options]` in `pyproject.toml`).
- Place tests in `tests/` with `test_*.py` and `test_*` function names.
- Add tests for each bug fix and behavior change, especially date/window logic, dedup, and CLI paths.
- No strict coverage gate is configured; keep coverage improving for touched modules.

## Commit & Pull Request Guidelines
- Follow existing commit style: `<area>: <imperative summary>` (examples: `fix: ...`, `nlp: ...`, `scheduler: ...`).
- Keep commits scoped and atomic; include migrations in the same PR when schema changes.
- PRs should include:
  - clear problem/solution summary,
  - linked issue (if any),
  - test evidence (`pytest`, `ruff check .`),
  - notes for `.env` or operational changes,
  - sample output/screenshots for user-visible bot or digest changes.

## Security & Configuration Tips
- Never commit secrets from `.env`, API keys, or session files in `data/`.
- Validate new config via `aidigest doctor` before running scheduled jobs.

## Critical Gotchas (do not break)
- Telethon (variant B) MUST authenticate as a **user** (phone login). Never use bot token with Telethon.
  If session is bot-authenticated or broken: delete `data/telethon.session*` and run `aidigest tg:whoami` again.
- Yandex embeddings (`YANDEX_EMBED_MODEL_URI=emb://.../text-search-doc/latest`) uses **EMBED_DIM=256**.
  DB column must stay `VECTOR(256)` and `EMBED_DIM` must match.
- Yandex OpenAI-compatible embeddings may require **single input string per request** (batching can cause 400).
- Telegram Bot API: HTTP **401 Unauthorized** means invalid/revoked `BOT_TOKEN`. Preflight with `getMe` before debugging channel permissions.
- Digest window logic is fixed: **Europe/Riga, 13:00 previous day → 13:00 current day**, run at 13:10.

## Operational E2E Checklist
1) `docker compose up -d`
2) `alembic upgrade head`
3) `aidigest doctor`
4) `aidigest tg:whoami` (must NOT prompt for login)
5) `aidigest ingest && aidigest summarize && aidigest embed && aidigest dedup && aidigest publish`
6) `aidigest run-once --date YYYY-MM-DD` for a full pipeline check

## Task-specific docs
Additional short runbooks live in `agent-docs/`:
- `agent-docs/execplans.md`: recommended command sequences for daily runs, backfills, dry-runs, and failure recovery.
- `agent-docs/telegram.md`: Telethon vs Bot API setup, bot commands, access control, and Telegram troubleshooting.
- `agent-docs/yandex-ai.md`: required Yandex AI env vars, summarize/embed behavior, and validation checks.
- `agent-docs/digest-format.md`: HTML digest output structure, message length limits, and formatting guardrails.
- `agent-docs/operations.md`: setup, day-2 operations, health checks, and incident checklist.
