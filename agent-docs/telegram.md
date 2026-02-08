# Telegram Guide

This project uses two Telegram integrations with different credentials.

## Telethon user client (ingest/channel management)
- Required env: `TG_API_ID`, `TG_API_HASH`, optional `TG_SESSION_PATH`.
- First login: `aidigest tg:whoami` (interactive, stores session in `data/`).
- Add/list channels:
  - `aidigest tg:add @channel`
  - `aidigest tg:list`
- Session must be a **user** account, not a bot account.

## Bot API publisher and control bot
- Required env: `BOT_TOKEN`, `DIGEST_CHANNEL_ID`.
- Start bot command interface: `aidigest bot:run`
- Supported bot commands: `/add`, `/remove`, `/list`, `/list_all`, `/status`, `/digest-now`.

## Access control
- Set `ADMIN_TG_USER_ID` for single-admin mode, or `ALLOWED_USER_IDS` (comma-separated) for multi-user mode.
- Unauthorized users get `Access denied`.

## Troubleshooting
- `401 Unauthorized` on publish usually means invalid/revoked `BOT_TOKEN`.
- Telethon auth errors: remove broken session file (`data/telethon.session*`) and run `aidigest tg:whoami` again.
- Invalid channel refs should be retried in `@username` or `https://t.me/...` form.
