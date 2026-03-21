from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from app.ops.release_validation import build_clean_machine_validation_report, prepare_validation_kit, wait_for_path


def _write_file(path: Path, content: str = "demo") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_prepare_validation_kit_copies_bundle_projects_and_writes_manifest(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "dist" / "reup-video"
    short_project = tmp_path / "workspace" / "short-project"
    long_project = tmp_path / "workspace" / "long-project"
    kit_root = tmp_path / "workspace" / "validation-kit"

    _write_file(bundle_dir / "reup-video.exe")
    _write_file(bundle_dir / "dependencies" / "ffmpeg" / "ffmpeg.exe")
    _write_file(short_project / "project.json", "{}")
    _write_file(short_project / "project.db")
    _write_file(long_project / "project.json", "{}")
    _write_file(long_project / "project.db")

    manifest = prepare_validation_kit(
        bundle_dir=bundle_dir,
        short_project_root=short_project,
        long_project_root=long_project,
        kit_root=kit_root,
    )

    assert manifest.bundle_dir.exists()
    assert (manifest.bundle_dir / "reup-video.exe").exists()
    assert manifest.smoke_script_path.exists()
    assert manifest.instructions_path.exists()
    assert manifest.report_template_path.exists()
    assert (kit_root / "validation_kit_manifest.json").exists()
    assert any(entry.label == "short" for entry in manifest.projects)
    assert any(entry.label == "long" for entry in manifest.projects)
    smoke_script = manifest.smoke_script_path.read_text(encoding="utf-8")
    assert "WaitSeconds" in smoke_script
    assert "libmpv-2.dll" in smoke_script
    assert "DoctorStages" in smoke_script
    assert "Bundle smoke bi block boi doctor" in smoke_script

    template_payload = json.loads(manifest.report_template_path.read_text(encoding="utf-8"))
    assert template_payload["bundle_dir"] == str(manifest.bundle_dir)
    assert template_payload["preview_project_label"] == "short"


def test_build_clean_machine_validation_report_tracks_project_paths(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle" / "reup-video"
    short_project = tmp_path / "projects" / "short"
    long_project = tmp_path / "projects" / "long"
    summary_short = tmp_path / "reports" / "short-summary.json"
    summary_long = tmp_path / "reports" / "long-summary.json"
    doctor_report = tmp_path / "reports" / "doctor.json"
    smoke_log = tmp_path / "logs" / "smoke.log"

    _write_file(bundle_dir / "reup-video.exe")
    _write_file(short_project / "project.json", "{}")
    _write_file(short_project / "project.db")
    _write_file(long_project / "project.json", "{}")
    _write_file(long_project / "project.db")
    _write_file(summary_short, "{}")
    _write_file(summary_long, "{}")
    _write_file(doctor_report, "{}")
    _write_file(smoke_log, "ok")

    manifest = prepare_validation_kit(
        bundle_dir=bundle_dir,
        short_project_root=short_project,
        long_project_root=long_project,
        kit_root=tmp_path / "kit",
    )
    report = build_clean_machine_validation_report(
        manifest=manifest,
        machine_label="win11-vm",
        windows_version="Windows 11 24H2",
        bundle_smoke_passed=True,
        preview_passed=True,
        short_project_summary_path=summary_short,
        long_project_summary_path=summary_long,
        short_output_video_path=tmp_path / "out-short.mp4",
        long_output_video_path=tmp_path / "out-long.mp4",
        bundle_doctor_report_path=doctor_report,
        bundle_smoke_log_path=smoke_log,
        blockers=["none"],
        notes=["manual validation complete"],
    )

    payload = report.to_dict()
    assert payload["machine_label"] == "win11-vm"
    assert payload["bundle_smoke_passed"] is True
    assert payload["preview_passed"] is True
    assert payload["project_results"][0]["rerun_passed"] is True
    assert payload["project_results"][1]["rerun_passed"] is True
    assert payload["bundle_doctor_report_path"] == str(doctor_report)


def test_wait_for_path_handles_delayed_report_creation(tmp_path: Path) -> None:
    report_path = tmp_path / "delayed-report.json"

    def _writer() -> None:
        time.sleep(0.2)
        report_path.write_text("{}", encoding="utf-8")

    thread = threading.Thread(target=_writer, daemon=True)
    thread.start()
    assert wait_for_path(report_path, timeout_seconds=2.0, poll_interval_seconds=0.05) is True
    thread.join(timeout=1.0)
