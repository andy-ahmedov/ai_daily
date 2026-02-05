from __future__ import annotations

from typing import Any

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from loguru import logger
from sqlalchemy import text

from aidigest.bot_commands.auth import is_user_allowed
from aidigest.config import get_settings
from aidigest.db.engine import get_engine
from aidigest.db.repo_channels import (
    get_channel_by_peer_id,
    get_channel_by_username,
    list_channels,
    set_channel_active,
    upsert_channel,
)
from aidigest.telegram.user_client import UserTelegramClient


router = Router()


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
    active = list_channels(active_only=True)
    total = list_channels(active_only=False)

    db_status = "OK"
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("DB status check failed: {}", exc)
        db_status = "ERROR"

    window = f"{settings.window_start_hour:02d}:00->{settings.window_end_hour:02d}:00 {settings.timezone}"
    digest_channel = settings.digest_channel_id or "not set"

    lines = [
        f"Active channels: {len(active)}",
        f"Total channels: {len(total)}",
        f"Window: {window}",
        f"DIGEST_CHANNEL_ID: {digest_channel}",
        f"DB: {db_status}",
    ]
    await message.answer("\n".join(lines))
