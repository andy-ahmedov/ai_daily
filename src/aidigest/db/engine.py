from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from aidigest.config import get_settings


def get_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url)
