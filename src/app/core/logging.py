from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.core.paths import get_logs_dir

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_ROOT_CONFIGURED = False


class SafeConsoleHandler(logging.StreamHandler):
    """Best-effort console logging for windowed Windows bundles.

    Frozen PySide builds may run without a writable stderr/stdout, and some
    third-party libraries emit Unicode that cannot be encoded by legacy Windows
    code pages. Console logging must never break the actual job flow.
    """

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - exercised via tests
        stream = self.stream
        if stream is None or not hasattr(stream, "write"):
            return

        try:
            message = self.format(record)
            terminator = getattr(self, "terminator", "\n")
            try:
                stream.write(message + terminator)
            except UnicodeEncodeError:
                encoding = getattr(stream, "encoding", None) or "utf-8"
                fallback_text = (message + terminator).encode(
                    encoding,
                    errors="backslashreplace",
                ).decode(encoding, errors="ignore")
                stream.write(fallback_text)
            if hasattr(stream, "flush"):
                stream.flush()
        except RecursionError:
            raise
        except Exception:
            # Console logging is best-effort only. File/job handlers still
            # capture the real error, so avoid cascading failures here.
            return


def _resolve_console_stream():
    for stream in (sys.stderr, sys.stdout):
        if stream is not None and hasattr(stream, "write"):
            return stream
    return None


def get_job_log_path(job_id: str, appdata_dir: Path | None = None) -> Path:
    return get_logs_dir(appdata_dir) / f"job_{job_id}.log"


def configure_logging(level: int = logging.INFO, appdata_dir: Path | None = None) -> None:
    global _ROOT_CONFIGURED

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if _ROOT_CONFIGURED:
        return

    formatter = logging.Formatter(LOG_FORMAT)

    console_stream = _resolve_console_stream()
    if console_stream is not None:
        console_handler = SafeConsoleHandler(console_stream)
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
