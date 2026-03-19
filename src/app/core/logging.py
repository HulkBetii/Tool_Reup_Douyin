from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.core.paths import get_logs_dir

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_ROOT_CONFIGURED = False


def get_job_log_path(job_id: str, appdata_dir: Path | None = None) -> Path:
    return get_logs_dir(appdata_dir) / f"job_{job_id}.log"


def configure_logging(level: int = logging.INFO, appdata_dir: Path | None = None) -> None:
    global _ROOT_CONFIGURED

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if _ROOT_CONFIGURED:
        return

    formatter = logging.Formatter(LOG_FORMAT)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.set_name("console")
    root_logger.addHandler(console_handler)

    app_log_path = get_logs_dir(appdata_dir) / "app.log"
    app_file_handler = RotatingFileHandler(
        app_log_path,
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    app_file_handler.setFormatter(formatter)
    app_file_handler.set_name("app-file")
    root_logger.addHandler(app_file_handler)

    _ROOT_CONFIGURED = True


def _has_named_handler(logger: logging.Logger, handler_name: str) -> bool:
    return any(getattr(handler, "name", "") == handler_name for handler in logger.handlers)


def get_logger(name: str, job_id: str | None = None, appdata_dir: Path | None = None) -> logging.Logger:
    configure_logging(appdata_dir=appdata_dir)
    logger = logging.getLogger(name)

    if not job_id:
        return logger

    handler_name = f"job:{job_id}"
    if _has_named_handler(logger, handler_name):
        return logger

    formatter = logging.Formatter(LOG_FORMAT)
    job_handler = RotatingFileHandler(
        get_job_log_path(job_id, appdata_dir),
        maxBytes=1_000_000,
        backupCount=2,
        encoding="utf-8",
    )
    job_handler.setFormatter(formatter)
    job_handler.set_name(handler_name)
    logger.addHandler(job_handler)
    return logger

