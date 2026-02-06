from __future__ import annotations

import asyncio
from datetime import date, datetime
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from sqlalchemy import inspect, text

from aidigest import __version__
from aidigest.config import get_settings
from aidigest.db.engine import get_engine
from aidigest.db.repo_channels import list_channels, upsert_channel
from aidigest.ingest import ingest_posts_for_date
from aidigest.ingest.window import compute_window
from aidigest.logging import configure_logging
from aidigest.bot_commands.app import run_bot_sync
from aidigest.telegram.user_client import UserTelegramClient


console = Console()


def _redact_database_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.password:
        return url

    netloc = parsed.netloc.replace(parsed.password, "***")
    return parsed._replace(netloc=netloc).geturl()


def _run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError as exc:
        if "asyncio.run()" in str(exc):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(coro)
        raise


def _ensure_telegram_settings() -> UserTelegramClient:
    settings = get_settings()
    if not settings.tg_api_id or not settings.tg_api_hash:
        console.print("Missing TG_API_ID/TG_API_HASH. Fill them in .env.")
        raise SystemExit(1)
    return UserTelegramClient(
        api_id=settings.tg_api_id,
        api_hash=settings.tg_api_hash,
        session_path=settings.tg_session_path,
    )


def _parse_target_date(
    ctx: click.Context,  # noqa: ARG001
    param: click.Parameter,  # noqa: ARG001
    value: str | None,
) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise click.BadParameter("Expected format YYYY-MM-DD.") from exc


@click.group()
def main() -> None:
    """Aidigest CLI."""
    load_dotenv()
    configure_logging()


@main.command()
def version() -> None:
    """Print package version."""
    console.print(__version__)


@main.command()
def doctor() -> None:
    """Check configuration and database connectivity."""
    settings = get_settings()

    table = Table(title="Config")
    table.add_column("Key", style="bold")
    table.add_column("Value")

    safe_values = {
        "TG_API_ID": settings.tg_api_id,
        "TG_SESSION_PATH": settings.tg_session_path,
        "DIGEST_CHANNEL_ID": settings.digest_channel_id,
        "TIMEZONE": settings.timezone,
        "WINDOW_START_HOUR": settings.window_start_hour,
        "WINDOW_END_HOUR": settings.window_end_hour,
        "RUN_AT_HOUR": settings.run_at_hour,
        "RUN_AT_MINUTE": settings.run_at_minute,
        "DATABASE_URL": _redact_database_url(settings.database_url),
        "EMBED_DIM": settings.embed_dim,
        "DEDUP_THRESHOLD": settings.dedup_threshold,
    }

    for key, value in safe_values.items():
        table.add_row(key, str(value))

    console.print("OK")
    console.print(table)

    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        console.print(f"DB check failed: {exc}")
        raise SystemExit(1)

    console.print("DB: OK")

    schema_ok = False
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            schema_ok = inspector.has_table("channels")
    except Exception:
        schema_ok = False

    console.print("DB schema: OK" if schema_ok else "DB schema: not migrated")


@main.command(name="tg:whoami")
def tg_whoami() -> None:
    """Authorize Telethon and print current user."""

    async def _run() -> None:
        client = _ensure_telegram_settings()
        await client.connect()
        try:
            who = await client.whoami()
            console.print(who)
        finally:
            await client.disconnect()

    try:
        _run_async(_run())
    except Exception as exc:
        console.print(f"Error: {exc}")
        raise SystemExit(1) from exc


@main.command(name="tg:resolve")
@click.argument("ref")
def tg_resolve(ref: str) -> None:
    """Resolve channel reference into peer info."""

    async def _run() -> None:
        client = _ensure_telegram_settings()
        await client.connect()
        try:
            info = await client.get_channel_info(ref)
            console.print(f"{info['tg_peer_id']} {info['title']} {info['username'] or ''}")
        finally:
            await client.disconnect()

    try:
        _run_async(_run())
    except Exception as exc:
        console.print(f"Error: {exc}")
        raise SystemExit(1) from exc


@main.command(name="tg:add")
@click.argument("ref")
def tg_add(ref: str) -> None:
    """Join channel if needed and upsert it into DB."""

    async def _run() -> None:
        client = _ensure_telegram_settings()
        await client.connect()
        try:
            entity = await client.ensure_join(ref)
            info = client._entity_info(entity)
        finally:
            await client.disconnect()

        channel = upsert_channel(
            tg_peer_id=info["tg_peer_id"],
            username=info["username"],
            title=info["title"],
            is_active=True,
        )
        console.print(f"Added/Updated: {channel.title} ({channel.tg_peer_id})")

    try:
        _run_async(_run())
    except Exception as exc:
        console.print(f"Error: {exc}")
        raise SystemExit(1) from exc


@main.command(name="tg:list")
@click.option("--all", "show_all", is_flag=True, help="Show inactive channels too.")
def tg_list(show_all: bool) -> None:
    """List channels from database."""
    channels = list_channels(active_only=not show_all)
    if not channels:
        console.print("No channels found.")
        return

    table = Table(title="Channels")
    table.add_column("tg_peer_id", style="bold")
    table.add_column("title")
    table.add_column("username")
    table.add_column("active")

    for channel in channels:
        table.add_row(
            str(channel.tg_peer_id),
            channel.title,
            channel.username or "",
            "yes" if channel.is_active else "no",
        )

    console.print(table)


@main.command(name="bot:run")
def bot_run() -> None:
    """Run Telegram bot (polling)."""
    try:
        run_bot_sync()
    except Exception as exc:
        console.print(f"Error: {exc}")
        raise SystemExit(1) from exc


@main.command(name="ingest")
@click.option("--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD.")
@click.option("--dry-run", is_flag=True, help="Fetch posts and print stats without writing to DB.")
def ingest(target_date: date | None, dry_run: bool) -> None:
    """Ingest channel posts for daily [13:00, 13:00) window."""

    async def _run() -> None:
        settings = get_settings()
        effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
        start_at, end_at = compute_window(
            target_date=effective_date,
            tz=settings.timezone,
            start_hour=settings.window_start_hour,
        )

        console.print(
            f"Window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})"
        )

        client = _ensure_telegram_settings()
        await client.connect(allow_interactive_login=False)
        try:
            summary = await ingest_posts_for_date(
                client=client,
                target_date=effective_date,
                timezone=settings.timezone,
                start_hour=settings.window_start_hour,
                dry_run=dry_run,
            )
        finally:
            await client.disconnect()

        table_title = "Ingest (dry-run)" if dry_run else "Ingest"
        table = Table(title=table_title)
        table.add_column("channel_id", style="bold")
        table.add_column("title")
        table.add_column("fetched")
        table.add_column("inserted" if not dry_run else "would_insert")
        table.add_column("updated" if not dry_run else "would_update")
        table.add_column("status")

        for channel in summary.per_channel:
            table.add_row(
                str(channel.channel_id),
                channel.title,
                str(channel.fetched),
                str(channel.inserted),
                str(channel.updated),
                channel.error or "ok",
            )

        console.print(table)
        console.print(
            "Summary: "
            f"channels={summary.channels_processed}, "
            f"fetched={summary.posts_fetched}, "
            f"inserted={summary.posts_inserted}, "
            f"updated={summary.posts_updated}, "
            f"duration={summary.duration_seconds:.2f}s"
        )

    try:
        _run_async(_run())
    except Exception as exc:
        console.print(f"Error: {exc}")
        raise SystemExit(1) from exc
