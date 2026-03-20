from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.logging import configure_logging, get_logger
from app.ops.doctor import run_doctor
from app.project.bootstrap import open_project
from app.core.settings import load_settings
from app.ui.main_window import MainWindow
from app.version import APP_NAME, APP_VERSION, ORGANIZATION_NAME


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reup Video desktop app")
    parser.add_argument("--doctor-report", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--doctor-stages", nargs="*", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    logger = get_logger("app.main")
    settings = load_settings()
    args = _parse_args(list(argv if argv is not None else sys.argv[1:]))

    if args.doctor_report is not None:
        workspace = None
        if args.project_root is not None:
            workspace = open_project(args.project_root.expanduser().resolve())
        report = run_doctor(
            settings=settings,
            workspace=workspace,
            requested_stages=args.doctor_stages,
        )
        args.doctor_report.parent.mkdir(parents=True, exist_ok=True)
        args.doctor_report.write_text(
            json.dumps(report.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Doctor report written to %s", args.doctor_report)
        return 0

    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)
    app.setOrganizationName(ORGANIZATION_NAME)

    window = MainWindow(settings=settings, job_manager=JobManager())
    window.show()

    logger.info("Application started")
    return app.exec()
