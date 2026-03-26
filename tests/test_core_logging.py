from __future__ import annotations

import io
import logging
import sys
from pathlib import Path

from app.core import logging as logging_module


class _StrictCp1252Stream:
    encoding = "cp1252"

    def __init__(self) -> None:
        self.buffer = io.BytesIO()

    def write(self, text: str) -> int:
        encoded = text.encode(self.encoding, errors="strict")
        return self.buffer.write(encoded)

    def flush(self) -> None:
        return None

    def getvalue(self) -> str:
        return self.buffer.getvalue().decode(self.encoding, errors="strict")


def _reset_logging_state() -> None:
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    logging_module._ROOT_CONFIGURED = False


def test_configure_logging_skips_missing_console_stream(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sys, "stderr", None)
    monkeypatch.setattr(sys, "stdout", None)
    _reset_logging_state()

    logging_module.configure_logging(appdata_dir=tmp_path)
    logger = logging_module.get_logger("test.no_console", appdata_dir=tmp_path)
    logger.info("hello")

    root_logger = logging.getLogger()
    assert not any(getattr(handler, "name", "") == "console" for handler in root_logger.handlers)


def test_configure_logging_handles_unencodable_console_unicode(monkeypatch, tmp_path: Path) -> None:
    stream = _StrictCp1252Stream()
    monkeypatch.setattr(sys, "stderr", stream)
    monkeypatch.setattr(sys, "stdout", None)
    _reset_logging_state()

    logging_module.configure_logging(appdata_dir=tmp_path)
    logger = logging_module.get_logger("test.cp1252", appdata_dir=tmp_path)
    logger.info("Loaded 📢 voices")

    assert "\\U0001f4e2" in stream.getvalue()

