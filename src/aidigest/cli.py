from __future__ import annotations

from urllib.parse import urlparse

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from sqlalchemy import inspect, text

from aidigest import __version__
from aidigest.config import get_settings
from aidigest.db.engine import get_engine
from aidigest.logging import configure_logging


console = Console()


def _redact_database_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.password:
        return url

    netloc = parsed.netloc.replace(parsed.password, "***")
    return parsed._replace(netloc=netloc).geturl()


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
