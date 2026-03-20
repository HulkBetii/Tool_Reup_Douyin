from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.ops.models import DoctorCheckResult, DoctorReport


def test_main_doctor_mode_writes_report_without_launching_ui(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app import main as app_main

    report_path = tmp_path / "doctor-report.json"
    monkeypatch.setattr(
        app_main,
        "run_doctor",
        lambda **_kwargs: DoctorReport(
            generated_at="2026-03-20T00:00:00+00:00",
            checks=[DoctorCheckResult(name="ffmpeg", status="ok", message="ready")],
            requested_stages=(),
        ),
    )
    monkeypatch.setattr(
        app_main,
        "QApplication",
        lambda _argv: (_ for _ in ()).throw(AssertionError("QApplication should not be created in doctor mode")),
    )

    exit_code = app_main.main(["--doctor-report", str(report_path)])

    assert exit_code == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["checks"][0]["name"] == "ffmpeg"
