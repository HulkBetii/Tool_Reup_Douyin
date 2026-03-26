from __future__ import annotations

import os

from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.settings import build_default_settings
from app.ui.main_window import MainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def test_main_window_exposes_subtitle_subtext_toggle(tmp_path):
    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    app.processEvents()

    assert window._subtitle_subtext_toggle is not None  # type: ignore[attr-defined]
    assert window._subtitle_subtext_toggle.text() == "Subtext gốc"  # type: ignore[attr-defined]
    assert window._subtitle_subtext_toggle.isChecked() is False  # type: ignore[attr-defined]

    window.close()
    app.processEvents()
