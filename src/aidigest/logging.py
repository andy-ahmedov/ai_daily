from __future__ import annotations

from loguru import logger
from rich.logging import RichHandler
from rich.traceback import install as rich_traceback_install


def configure_logging(level: str = "INFO") -> None:
    rich_traceback_install()

    logger.remove()
    handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_time=False,
        show_level=True,
        show_path=False,
    )
    logger.add(handler, level=level, format="{message}")
