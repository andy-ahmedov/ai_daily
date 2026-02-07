from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy.orm import Session, sessionmaker

from aidigest.db.engine import get_engine

SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False)


@contextmanager
def get_session() -> Iterator[Session]:
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
