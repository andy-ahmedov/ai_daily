from __future__ import annotations

import signal
import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from aidigest.config import get_settings
from aidigest.scheduler.jobs import run_daily_pipeline


def _install_signal_handlers(stop_event: threading.Event) -> None:
    def _handle(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info("scheduler signal received signum={} stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)


def run_scheduler() -> None:
    settings = get_settings()
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    scheduler = BackgroundScheduler(timezone=settings.timezone)
    trigger = CronTrigger(
        hour=settings.run_at_hour,
        minute=settings.run_at_minute,
        timezone=settings.timezone,
    )
    scheduler.add_job(
        func=run_daily_pipeline,
        trigger=trigger,
        id="aidigest-daily-pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    scheduler.start()
    logger.info(
        "scheduler started timezone={} run_at={:02d}:{:02d}",
        settings.timezone,
        settings.run_at_hour,
        settings.run_at_minute,
    )

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        logger.info("scheduler shutting down")
        scheduler.shutdown(wait=False)
        logger.info("scheduler stopped")
