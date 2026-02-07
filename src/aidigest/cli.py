from __future__ import annotations

import asyncio
import random
import time
from datetime import date, datetime
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import click
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table
from sqlalchemy import inspect, text

from aidigest import __version__
from aidigest.config import get_settings
from aidigest.db.repo_embeddings import get_posts_missing_embedding, update_post_embedding
from aidigest.db.repo_dedup import top_hash_groups_in_window
from aidigest.db.engine import get_engine
from aidigest.db.repo_channels import list_channels, upsert_channel
from aidigest.ingest import ingest_posts_for_date
from aidigest.ingest.window import compute_window
from aidigest.logging import configure_logging
from aidigest.nlp.summarize import summarize_window
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
        "YANDEX_FOLDER_ID": settings.yandex_folder_id,
        "YANDEX_MODEL_URI": settings.yandex_model_uri,
        "YANDEX_EMBED_MODEL_URI": settings.yandex_embed_model_uri,
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

    if settings.yandex_api_key and settings.yandex_folder_id and settings.yandex_model_uri:
        try:
            from aidigest.nlp.yandex_llm import chat_json, make_client

            client = make_client(settings)
            response = chat_json(
                client=client,
                model_uri=settings.yandex_model_uri,
                messages=[
                    {"role": "system", "content": "Respond with JSON only."},
                    {"role": "user", "content": 'Respond with JSON: {"ok":true}'},
                ],
                post_id=0,
            )
            if response.get("ok") is True:
                console.print("Yandex LLM: OK")
            else:
                console.print("Yandex LLM: ERROR unexpected response")
        except Exception as exc:
            console.print(f"Yandex LLM: ERROR {exc}")
    else:
        console.print("Yandex LLM: not configured")

    if settings.yandex_api_key and settings.yandex_folder_id and settings.yandex_embed_model_uri:
        try:
            from aidigest.nlp.embed import embed_texts, make_yandex_client, validate_embedding

            make_yandex_client(settings)
            vectors = embed_texts(["ping"])
            if len(vectors) != 1:
                raise RuntimeError(f"unexpected vectors count: {len(vectors)}")
            validate_embedding(vectors[0])
            console.print("Yandex Embeddings: OK")
        except Exception as exc:
            console.print(f"Yandex Embeddings: ERROR {exc}")
    else:
        console.print("Yandex Embeddings: SKIPPED")


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


@main.command(name="dedup:report")
@click.option("--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD.")
def dedup_report(target_date: date | None) -> None:
    """Show top exact-duplicate groups by content_hash for ingest window."""
    settings = get_settings()
    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    console.print(f"Window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})")

    groups = top_hash_groups_in_window(start_at=start_at, end_at=end_at, limit=10)
    if not groups:
        console.print("No exact duplicates found for this window.")
        return

    table = Table(title="Exact Dedup Report (top 10)")
    table.add_column("content_hash", style="bold")
    table.add_column("duplicates")
    table.add_column("channels")

    for group in groups:
        table.add_row(group.content_hash, str(group.duplicates), ", ".join(group.channel_titles))

    console.print(table)


@main.command(name="summarize")
@click.option("--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD.")
@click.option("--limit", default=100, show_default=True, type=int, help="Max posts to process.")
@click.option("--dry-run", is_flag=True, help="Calculate actions without writing summaries.")
def summarize(target_date: date | None, limit: int, dry_run: bool) -> None:
    """Summarize posts with Alice AI LLM and exact-dedup reuse."""
    if limit <= 0:
        raise click.BadParameter("--limit must be > 0")

    settings = get_settings()
    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    console.print(f"Window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})")

    stats = summarize_window(
        start_at=start_at,
        end_at=end_at,
        limit=limit,
        dry_run=dry_run,
    )

    mode = "Summarize (dry-run)" if dry_run else "Summarize"
    table = Table(title=mode)
    table.add_column("metric", style="bold")
    table.add_column("value")
    table.add_row("total candidates", str(stats.total_candidates))
    table.add_row("skipped_existing", str(stats.skipped_existing))
    table.add_row("copied_exact_dup", str(stats.copied_exact_dup))
    table.add_row("summarized", str(stats.summarized))
    table.add_row("errors", str(stats.errors))
    console.print(table)


@main.command(name="embed")
@click.option("--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD.")
@click.option("--limit", default=200, show_default=True, type=int, help="Max posts to process.")
@click.option(
    "--batch-size",
    default=16,
    show_default=True,
    type=int,
    help="Embedding API batch size.",
)
@click.option("--dry-run", is_flag=True, help="Calculate actions without writing embeddings.")
def embed(target_date: date | None, limit: int, batch_size: int, dry_run: bool) -> None:
    """Compute embeddings for posts without vectors in ingest window."""
    if limit <= 0:
        raise click.BadParameter("--limit must be > 0")
    if batch_size <= 0:
        raise click.BadParameter("--batch-size must be > 0")

    settings = get_settings()
    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    console.print(f"Window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})")

    posts = get_posts_missing_embedding(start_at=start_at, end_at=end_at, limit=limit)
    total_candidates = len(posts)

    if dry_run:
        console.print(f"Dry-run: would process {total_candidates} posts.")
        return

    if total_candidates == 0:
        console.print("No posts missing embeddings in this window.")
        return

    if not settings.yandex_api_key or not settings.yandex_folder_id:
        raise click.ClickException("YANDEX_API_KEY and YANDEX_FOLDER_ID must be set for embed.")
    if not settings.yandex_embed_model_uri:
        raise click.ClickException("YANDEX_EMBED_MODEL_URI must be set for embed.")

    from aidigest.nlp.embed import embed_texts, make_yandex_client, validate_embedding

    make_yandex_client(settings)

    embedded = 0
    failed_batches = 0
    failed_posts = 0

    for offset in range(0, total_candidates, batch_size):
        batch = posts[offset : offset + batch_size]
        texts = [str(post.text or "") for post in batch]
        if not texts:
            continue

        try:
            vectors = embed_texts(texts)
            if len(vectors) != len(batch):
                raise RuntimeError(
                    f"batch size mismatch: expected {len(batch)}, got {len(vectors)}"
                )
            for post, vector in zip(batch, vectors):
                update_post_embedding(post.id, validate_embedding(vector))
                embedded += 1
        except Exception as exc:
            failed_batches += 1
            failed_posts += len(batch)
            logger.error(
                "embedding batch failed offset={} size={} error={}",
                offset,
                len(batch),
                exc,
            )
        finally:
            if offset + batch_size < total_candidates:
                time.sleep(random.uniform(0.1, 0.3))

    table = Table(title="Embed")
    table.add_column("metric", style="bold")
    table.add_column("value")
    table.add_row("total candidates", str(total_candidates))
    table.add_row("embedded", str(embedded))
    table.add_row("failed batches", str(failed_batches))
    table.add_row("failed posts", str(failed_posts))
    console.print(table)
