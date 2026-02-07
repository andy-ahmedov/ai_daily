from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from loguru import logger
from sqlalchemy import text

from aidigest.bot_commands.auth import is_user_allowed
from aidigest.config import get_settings
from aidigest.db.repo_dedup_clusters import get_or_create_window
from aidigest.db.repo_digests import get_digest_by_window
from aidigest.db.engine import get_engine
from aidigest.db.repo_channels import (
    get_channel_by_peer_id,
    get_channel_by_username,
    list_channels,
    set_channel_active,
    upsert_channel,
)
from aidigest.db.repo_stats import (
    count_channels,
    count_clusters,
    count_missing_embeddings,
    count_missing_summaries,
    count_posts_in_window,
    get_last_published_digest,
    get_window_by_range,
)
from aidigest.ingest.window import compute_window
from aidigest.scheduler.jobs import run_daily_pipeline
from aidigest.telegram.user_client import UserTelegramClient


router = Router()
_digest_now_task: asyncio.Task[None] | None = None


async def _ensure_allowed(message: Message, allow_bootstrap: bool = False) -> bool:
    user = message.from_user
    if not user:
        return False
    if not is_user_allowed(user.id, allow_bootstrap=allow_bootstrap):
        await message.answer("Access denied")
        return False
    return True


def _format_channel_line(channel: Any) -> str:
    username = f"@{channel.username}" if channel.username else "—"
    return f"{channel.title} | {username} | {channel.tg_peer_id}"


def _build_telegram_message_link(chat_id: int, message_id: int) -> str | None:
    if chat_id >= 0:
        return None
    channel = str(abs(chat_id))
    if not channel.startswith("100"):
        return None
    return f"https://t.me/c/{channel[3:]}/{message_id}"


def _format_datetime(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _current_window(settings: Any) -> tuple[Any, Any, Any]:
    timezone = ZoneInfo(settings.timezone)
    effective_date = datetime.now(timezone).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    return effective_date, start_at, end_at


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if not await _ensure_allowed(message, allow_bootstrap=True):
        return

    channels = list_channels(active_only=True)
    lines = []
    for channel in channels[:10]:
        lines.append(_format_channel_line(channel))

    text_lines = [
        "Aidigest bot",
        "",
        "Commands:",
        "/add <ref>",
        "/remove <ref_or_peer_id>",
        "/list",
        "/list_all",
        "/status",
        "/digest-now",
    ]
    if lines:
        text_lines.append("")
        text_lines.append("Channels:")
        text_lines.extend(lines)
        if len(channels) > 10:
            text_lines.append(f"... and {len(channels) - 10} more")

    await message.answer("\n".join(text_lines))


@router.message(Command("add"))
async def cmd_add(
    message: Message,
    command: CommandObject,
    tg_client: UserTelegramClient,
) -> None:
    if not await _ensure_allowed(message):
        return

    if not command.args:
        await message.answer("Usage: /add <ref>")
        return

    ref = command.args.strip()
    try:
        entity = await tg_client.ensure_join(ref)
        info = tg_client._entity_info(entity)
        channel = upsert_channel(
            tg_peer_id=info["tg_peer_id"],
            username=info["username"],
            title=info["title"],
            is_active=True,
        )
    except Exception as exc:
        logger.warning("Add failed: {}", exc)
        await message.answer(f"Error: {exc}")
        return

    username = f"@{channel.username}" if channel.username else "—"
    await message.answer(f"Added/Updated: {channel.title} ({username}) [{channel.tg_peer_id}]")


@router.message(Command("remove"))
async def cmd_remove(message: Message, command: CommandObject) -> None:
    if not await _ensure_allowed(message):
        return

    if not command.args:
        await message.answer("Usage: /remove <ref_or_peer_id>")
        return

    raw = command.args.strip()
    channel = None
    if raw.isdigit():
        channel = get_channel_by_peer_id(int(raw))
    else:
        username = raw.lstrip("@")
        channel = get_channel_by_username(username)

    if not channel:
        await message.answer("Channel not found")
        return

    try:
        channel = set_channel_active(channel, False)
    except Exception as exc:
        logger.warning("Remove failed: {}", exc)
        await message.answer("Failed to disable channel")
        return

    await message.answer(f"Removed (disabled): {channel.title}")


@router.message(Command("list"))
async def cmd_list(message: Message) -> None:
    if not await _ensure_allowed(message):
        return

    channels = list_channels(active_only=True)
    if not channels:
        await message.answer("No active channels")
        return

    lines = [_format_channel_line(channel) for channel in channels[:50]]
    if len(channels) > 50:
        lines.append(f"... showing 50 of {len(channels)}")
    await message.answer("\n".join(lines))


@router.message(Command("list_all"))
async def cmd_list_all(message: Message) -> None:
    if not await _ensure_allowed(message):
        return

    channels = list_channels(active_only=False)
    if not channels:
        await message.answer("No channels")
        return

    lines = []
    for channel in channels[:50]:
        status = "active" if channel.is_active else "disabled"
        username = f"@{channel.username}" if channel.username else "—"
        lines.append(f"{channel.title} | {username} | {channel.tg_peer_id} | {status}")

    if len(channels) > 50:
        lines.append(f"... showing 50 of {len(channels)}")

    await message.answer("\n".join(lines))


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not await _ensure_allowed(message):
        return

    settings = get_settings()
    db_status = "OK"
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("DB status check failed: {}", exc)
        db_status = f"ERROR ({exc})"

    _, start_at, end_at = _current_window(settings)

    def safe_value(fn: Any) -> str:
        try:
            return str(fn())
        except Exception as exc:  # pragma: no cover - defensive for runtime DB issues
            logger.warning("Status metric failed: {}", exc)
            return "N/A"

    active_channels = safe_value(lambda: count_channels(active_only=True))
    total_channels = safe_value(lambda: count_channels(active_only=False))
    posts_in_window = safe_value(lambda: count_posts_in_window(start_at=start_at, end_at=end_at))
    missing_summaries = safe_value(lambda: count_missing_summaries(start_at=start_at, end_at=end_at))
    missing_embeddings = safe_value(lambda: count_missing_embeddings(start_at=start_at, end_at=end_at))

    window_row = None
    clusters = "N/A"
    try:
        window_row = get_window_by_range(start_at=start_at, end_at=end_at)
        if window_row is not None:
            clusters = str(count_clusters(window_row.id))
    except Exception as exc:
        logger.warning("Status window/clusters metric failed: {}", exc)

    last_digest_line = "N/A"
    try:
        last_digest = get_last_published_digest()
        if last_digest is not None:
            message_count = len(last_digest.message_ids)
            last_digest_line = (
                f"{last_digest.start_at.date()}→{last_digest.end_at.date()} | "
                f"{_format_datetime(last_digest.published_at)} | messages={message_count}"
            )
    except Exception as exc:
        logger.warning("Status last digest metric failed: {}", exc)

    window_line = f"{start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})"
    schedule_line = f"{settings.run_at_hour:02d}:{settings.run_at_minute:02d} ({settings.timezone})"

    lines = [
        f"DB: {db_status}",
        f"Window: {window_line}",
        f"Channels active/total: {active_channels}/{total_channels}",
        f"Posts in window: {posts_in_window}",
        f"Missing summaries: {missing_summaries}",
        f"Missing embeddings: {missing_embeddings}",
        f"Dedup clusters (current window): {clusters}",
        f"Last published digest: {last_digest_line}",
        f"Schedule: {schedule_line}",
        f"DIGEST_CHANNEL_ID: {settings.digest_channel_id or 'not set'}",
    ]
    await message.answer("\n".join(lines))


@router.message(Command("digest-now"))
async def cmd_digest_now(message: Message) -> None:
    global _digest_now_task

    if not await _ensure_allowed(message):
        return

    if _digest_now_task is not None and not _digest_now_task.done():
        await message.answer("Пайплайн уже выполняется. Подождите завершения текущего запуска.")
        return

    settings = get_settings()
    effective_date, start_at, end_at = _current_window(settings)
    try:
        window = get_or_create_window(start_at=start_at, end_at=end_at)
        existing = get_digest_by_window(window.id)
    except Exception as exc:
        logger.warning("digest-now precheck failed: {}", exc)
        await message.answer(f"Ошибка при проверке окна: {exc}")
        return

    if existing is not None and existing.published_at is not None:
        message_ids = list(existing.message_ids or [])
        details = (
            f"Уже опубликовано: {_format_datetime(existing.published_at)} "
            f"(messages: {', '.join(str(mid) for mid in message_ids) if message_ids else '—'})"
        )
        first_link = (
            _build_telegram_message_link(int(existing.channel_id), int(message_ids[0]))
            if message_ids
            else None
        )
        if first_link:
            details += f"\n{first_link}"
        await message.answer(details)
        return

    await message.answer(
        f"Запускаю пайплайн для окна {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})"
    )

    async def _run_pipeline_task() -> None:
        global _digest_now_task
        try:
            await asyncio.to_thread(run_daily_pipeline, effective_date)
            window_after = get_window_by_range(start_at=start_at, end_at=end_at)
            digest_after = (
                get_digest_by_window(window_after.id) if window_after is not None else None
            )
            if window_after is not None and str(window_after.status).lower() == "failed":
                await message.answer("Ошибка: пайплайн завершился со статусом failed.")
                return
            if digest_after is not None and digest_after.published_at is not None:
                sent = len(digest_after.message_ids or [])
                await message.answer(f"Готово: опубликовано {sent} сообщений.")
            else:
                await message.answer("Готово: запуск завершен, публикация не выполнена.")
        except Exception as exc:  # pragma: no cover - defensive around thread/runtime issues
            logger.exception("digest-now failed: {}", exc)
            await message.answer(f"Ошибка: {exc}")
        finally:
            _digest_now_task = None

    _digest_now_task = asyncio.create_task(_run_pipeline_task())
