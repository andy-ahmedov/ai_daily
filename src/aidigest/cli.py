from __future__ import annotations

import asyncio
import random
import re
import time
from datetime import date, datetime, timezone
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import click
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table
from sqlalchemy import inspect, text

from aidigest import __version__
from aidigest.bot_commands.app import run_bot_sync
from aidigest.config import get_settings
from aidigest.db.engine import get_engine
from aidigest.db.repo_channels import list_channels, upsert_channel
from aidigest.db.repo_dedup import top_hash_groups_in_window
from aidigest.db.repo_dedup_clusters import get_or_create_window
from aidigest.db.repo_digest import get_window_by_range
from aidigest.db.repo_digests import get_digest_by_window, upsert_digest
from aidigest.db.repo_embeddings import get_posts_missing_embedding, update_post_embedding
from aidigest.digest.build import build_digest_data
from aidigest.digest.format import render_digest_html
from aidigest.ingest import ingest_posts_for_date
from aidigest.ingest.window import compute_window
from aidigest.logging import configure_logging
from aidigest.nlp.dedup import run_semantic_dedup
from aidigest.nlp.summarize import summarize_window
from aidigest.scheduler import run_daily_pipeline, run_scheduler
from aidigest.telegram.bot_client import DigestPublisher
from aidigest.telegram.user_client import UserTelegramClient

console = Console()
_VECTOR_TYPE_RE = re.compile(r"^vector\((\d+)\)$", re.IGNORECASE)


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


def _try_parse_chat_id(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        return None


def _build_telegram_message_link(chat_id: int, message_id: int) -> str | None:
    if chat_id >= 0:
        return None
    channel = str(abs(chat_id))
    if not channel.startswith("100"):
        return None
    return f"https://t.me/c/{channel[3:]}/{message_id}"


def _parse_vector_dimension(type_name: str | None) -> int | None:
    if type_name is None:
        return None
    match = _VECTOR_TYPE_RE.match(type_name.strip())
    if not match:
        return None
    return int(match.group(1))


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
    db_embed_dim: int | None = None
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            schema_ok = inspector.has_table("channels")
            embedding_type = conn.execute(
                text(
                    """
                    SELECT format_type(a.atttypid, a.atttypmod)
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'posts'
                      AND n.nspname = current_schema()
                      AND a.attname = 'embedding'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    """
                )
            ).scalar_one_or_none()
            db_embed_dim = _parse_vector_dimension(
                str(embedding_type) if embedding_type is not None else None
            )
    except Exception:
        schema_ok = False

    console.print("DB schema: OK" if schema_ok else "DB schema: not migrated")
    if db_embed_dim is not None:
        if db_embed_dim == settings.embed_dim:
            console.print(f"DB embedding dim: OK ({db_embed_dim})")
        else:
            console.print(
                f"DB embedding dim: MISMATCH env={settings.embed_dim} db={db_embed_dim}"
            )
            raise SystemExit(1)
    else:
        console.print("DB embedding dim: unknown")

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
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
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
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
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


@main.command(name="dedup")
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
@click.option("--threshold", type=float, help="Cosine similarity threshold (0..1).")
@click.option(
    "--top-k", default=80, show_default=True, type=int, help="Top similar posts per cluster center."
)
@click.option("--dry-run", is_flag=True, help="Calculate clusters without writing to DB.")
def dedup(target_date: date | None, threshold: float | None, top_k: int, dry_run: bool) -> None:
    """Cluster semantically similar posts in ingest window."""
    if top_k <= 0:
        raise click.BadParameter("--top-k must be > 0")

    settings = get_settings()
    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    threshold_value = settings.dedup_threshold if threshold is None else threshold
    if not 0 <= threshold_value <= 1:
        raise click.BadParameter("--threshold must be in range 0..1")

    console.print(f"Window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone})")
    console.print(
        f"Semantic dedup params: threshold={threshold_value:.4f}, top_k={top_k}, dry_run={dry_run}"
    )

    stats = run_semantic_dedup(
        start_at=start_at,
        end_at=end_at,
        threshold=threshold_value,
        top_k=top_k,
        dry_run=dry_run,
    )

    table_title = "Semantic Dedup (dry-run)" if dry_run else "Semantic Dedup"
    table = Table(title=table_title)
    table.add_column("metric", style="bold")
    table.add_column("value")
    table.add_row("clusters_created", str(stats.clusters_created))
    table.add_row("posts_assigned", str(stats.posts_assigned))
    table.add_row("posts_skipped_no_embedding", str(stats.posts_skipped_no_embedding))
    table.add_row("largest_cluster_size", str(stats.largest_cluster_size))
    table.add_row("avg_cluster_size", f"{stats.average_cluster_size:.2f}")
    table.add_row("duration", f"{stats.duration_seconds:.2f}s")
    console.print(table)

    if stats.top_clusters:
        top_table = Table(title="Top Clusters by Size")
        top_table.add_column("#", style="bold")
        top_table.add_column("representative_post_id")
        top_table.add_column("size")
        for idx, cluster in enumerate(stats.top_clusters, start=1):
            top_table.add_row(
                str(idx),
                str(cluster.representative_post_id),
                str(len(cluster.members)),
            )
        console.print(top_table)


@main.command(name="digest")
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
@click.option(
    "--top", default=10, show_default=True, type=int, help="Top-N clusters in first message."
)
@click.option("--dry-run", is_flag=True, help="No DB writes (digest currently read-only).")
def digest(target_date: date | None, top: int, dry_run: bool) -> None:
    """Build Telegram HTML digest and print it to stdout."""
    if top <= 0:
        raise click.BadParameter("--top must be > 0")

    settings = get_settings()
    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    window = get_window_by_range(start_at=start_at, end_at=end_at)
    window_id = window.id if window is not None else None

    digest_data = build_digest_data(
        start_at=start_at,
        end_at=end_at,
        window_id=window_id,
        top_n=top,
    )
    messages = render_digest_html(digest_data)

    click.echo(
        f"Digest window: {start_at.isoformat()} -> {end_at.isoformat()} ({settings.timezone}) "
        f"top={top} dry_run={dry_run}"
    )
    if window_id is None:
        click.echo("Window not found in DB; using fallback Top list (content_hash dedup).")

    total = len(messages)
    for idx, message in enumerate(messages, start=1):
        click.echo(f"----- MESSAGE {idx}/{total} -----")
        click.echo(message)


@main.command(name="publish")
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
@click.option(
    "--force", is_flag=True, help="Publish again even if this window was published before."
)
def publish(target_date: date | None, force: bool) -> None:
    """Publish rendered digest to Telegram channel and persist message ids."""
    settings = get_settings()
    if not settings.bot_token:
        raise click.ClickException("BOT_TOKEN must be set for publish.")
    chat_id = _try_parse_chat_id(settings.digest_channel_id)
    if chat_id is None:
        raise click.ClickException("DIGEST_CHANNEL_ID must be a Telegram chat_id (e.g. -100...).")

    effective_date = target_date or datetime.now(ZoneInfo(settings.timezone)).date()
    start_at, end_at = compute_window(
        target_date=effective_date,
        tz=settings.timezone,
        start_hour=settings.window_start_hour,
    )
    window = get_or_create_window(start_at=start_at, end_at=end_at)
    existing = get_digest_by_window(window.id)
    if existing is not None and existing.published_at is not None and not force:
        console.print(
            "Already published for window "
            f"{start_at.isoformat()} -> {end_at.isoformat()} (window_id={window.id})."
        )
        return

    digest_data = build_digest_data(
        start_at=start_at,
        end_at=end_at,
        window_id=window.id,
        top_n=10,
    )
    messages = render_digest_html(digest_data)
    if not messages:
        raise click.ClickException("Digest rendering produced no messages.")

    with DigestPublisher(settings.bot_token) as publisher:
        message_ids = publisher.send_html_messages(chat_id=chat_id, messages=messages)

    stats = {
        "messages": len(messages),
        "top_clusters": len(digest_data.top_clusters),
        "channels": len(digest_data.per_channel),
        "posts": sum(channel.posts_count for channel in digest_data.per_channel),
    }
    content = "\n\n----- MESSAGE BREAK -----\n\n".join(messages)
    published_at = datetime.now(timezone.utc)

    upsert_digest(
        window_id=window.id,
        channel_id=chat_id,
        message_ids=message_ids,
        content=content,
        stats=stats,
        published_at=published_at,
    )

    console.print(
        f"Published {len(message_ids)} messages to {chat_id} "
        f"for window {start_at.isoformat()} -> {end_at.isoformat()}."
    )
    for idx, message_id in enumerate(message_ids, start=1):
        link = _build_telegram_message_link(chat_id, message_id)
        if link:
            console.print(f"{idx}. message_id={message_id} link={link}")
        else:
            console.print(f"{idx}. message_id={message_id}")


@main.command(name="run-once")
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
def run_once(target_date: date | None) -> None:
    """Run full daily pipeline once."""
    stats = run_daily_pipeline(target_date=target_date)

    table = Table(title="Pipeline Run Once")
    table.add_column("metric", style="bold")
    table.add_column("value")
    table.add_row("status", "failed" if stats.failed else "ok")
    table.add_row("duration", f"{stats.total_duration_seconds:.2f}s")
    table.add_row("messages_sent", str(stats.messages_sent))
    table.add_row("ingest fetched", str(stats.ingest.posts_fetched if stats.ingest else 0))
    table.add_row("summarized", str(stats.summarize.summarized if stats.summarize else 0))
    table.add_row("embedded", str(stats.embed.embedded if stats.embed else 0))
    table.add_row("clusters", str(stats.dedup.clusters_created if stats.dedup else 0))
    if stats.error:
        table.add_row("error", stats.error)
    console.print(table)
    if stats.failed:
        raise click.ClickException("Pipeline failed. See logs above.")


@main.command(name="scheduler:run")
def scheduler_run() -> None:
    """Run APScheduler loop for daily pipeline."""
    run_scheduler()


@main.command(name="summarize")
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
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
@click.option(
    "--date", "target_date", callback=_parse_target_date, help="Target date in YYYY-MM-DD."
)
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
