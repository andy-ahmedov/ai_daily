from __future__ import annotations

from loguru import logger
from sqlalchemy import select

from aidigest.config import get_settings
from aidigest.db.models import Setting
from aidigest.db.session import get_session


_ADMIN_SETTING_KEY = "admin_user_id"


def is_user_allowed(user_id: int, allow_bootstrap: bool = False) -> bool:
    settings = get_settings()
    if settings.admin_tg_user_id:
        return user_id == settings.admin_tg_user_id

    if settings.allowed_user_ids:
        return user_id in settings.allowed_user_ids

    existing = _get_admin_user_id()
    if existing is None:
        if allow_bootstrap:
            _set_admin_user_id(user_id)
            logger.info("Pinned first admin user id {}", user_id)
            return True
        return False

    return user_id == existing


def _get_admin_user_id() -> int | None:
    with get_session() as session:
        row = session.execute(
            select(Setting).where(Setting.key == _ADMIN_SETTING_KEY)
        ).scalar_one_or_none()
        if not row:
            return None
        try:
            return int(row.value)
        except ValueError:
            return None


def _set_admin_user_id(user_id: int) -> None:
    with get_session() as session:
        row = session.execute(
            select(Setting).where(Setting.key == _ADMIN_SETTING_KEY)
        ).scalar_one_or_none()
        if row:
            row.value = str(user_id)
        else:
            session.add(Setting(key=_ADMIN_SETTING_KEY, value=str(user_id)))
