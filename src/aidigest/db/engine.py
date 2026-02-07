from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from aidigest.config import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    engine_kwargs: dict[str, object] = {"pool_pre_ping": True}
    if settings.database_url.startswith("postgresql"):
        engine_kwargs["pool_recycle"] = 300
        engine_kwargs["connect_args"] = {"connect_timeout": 5}
    return create_engine(settings.database_url, **engine_kwargs)
