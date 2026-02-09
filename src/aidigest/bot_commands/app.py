from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from loguru import logger
from sqlalchemy import text

from aidigest.bot_commands.handlers import router
from aidigest.config import get_settings
from aidigest.db.engine import get_engine
from aidigest.telegram.user_client import UserTelegramClient


def _make_client() -> UserTelegramClient:
    settings = get_settings()
    if not settings.tg_api_id or not settings.tg_api_hash:
        raise RuntimeError("Missing TG_API_ID/TG_API_HASH. Fill them in .env.")
    return UserTelegramClient(
        api_id=settings.tg_api_id,
        api_hash=settings.tg_api_hash,
        session_path=settings.tg_session_path,
    )


async def _on_startup(dispatcher: Dispatcher, client: UserTelegramClient) -> None:
    logger.info("Starting Telethon client for bot")
    await client.connect(allow_interactive_login=False)


async def _on_shutdown(dispatcher: Dispatcher, client: UserTelegramClient) -> None:
    logger.info("Stopping Telethon client for bot")
    await client.disconnect()


async def run_bot() -> None:
    settings = get_settings()
    if not settings.bot_token:
        raise RuntimeError("Missing BOT_TOKEN. Fill it in .env.")
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise RuntimeError(
            "Database is unavailable. Start PostgreSQL (`docker compose up -d postgres`) "
            "and apply migrations (`alembic upgrade head`)."
        ) from exc

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    client = _make_client()
    dp["tg_client"] = client
    dp.include_router(router)

    async def on_startup(dispatcher: Dispatcher) -> None:
        await _on_startup(dispatcher, client)

    async def on_shutdown(dispatcher: Dispatcher) -> None:
        await _on_shutdown(dispatcher, client)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("Bot polling started")
    await dp.start_polling(bot)


def run_bot_sync() -> None:
    asyncio.run(run_bot())
