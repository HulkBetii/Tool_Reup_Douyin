from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.logging import configure_logging, get_logger
from app.core.settings import load_settings
from app.ui.main_window import MainWindow
from app.version import APP_NAME, APP_VERSION, ORGANIZATION_NAME


def main() -> int:
    configure_logging()
    logger = get_logger("app.main")
    settings = load_settings()

    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)
    app.setOrganizationName(ORGANIZATION_NAME)

    window = MainWindow(settings=settings, job_manager=JobManager())
    window.show()

    logger.info("Application started")
    return app.exec()

