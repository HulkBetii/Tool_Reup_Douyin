from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "finalize_clean_machine_validation.py"
    spec = importlib.util.spec_from_file_location("finalize_clean_machine_validation", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_finalize_clean_machine_validation_writes_reports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_script_module()
    kit_root = tmp_path / "kit"
    reports_dir = kit_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    manifest_payload = {
        "created_at": "2026-03-20T00:00:00+00:00",
        "kit_root": str(kit_root),
        "bundle_dir": str(kit_root / "bundle" / "reup-video"),
        "smoke_script_path": str(kit_root / "run_bundle_smoke.ps1"),
        "instructions_path": str(kit_root / "CLEAN_MACHINE_README.md"),
        "report_template_path": str(reports_dir / "clean_machine_validation_report.template.json"),
        "local_smoke_log_path": str(kit_root / "logs" / "local_smoke_bundle.log"),
        "local_doctor_report_path": str(reports_dir / "local_bundle_doctor_report.json"),
        "projects": [
            {
                "label": "short",
                "source_path": str(tmp_path / "short-src"),
                "copied_path": str(kit_root / "projects" / "short"),
            },
            {
                "label": "long",
                "source_path": str(tmp_path / "long-src"),
                "copied_path": str(kit_root / "projects" / "long"),
            },
        ],
    }
    (kit_root / "validation_kit_manifest.json").write_text(json.dumps(manifest_payload), encoding="utf-8")
    short_summary = reports_dir / "short-summary.json"
    long_summary = reports_dir / "long-summary.json"
    short_summary.write_text("{}", encoding="utf-8")
    long_summary.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "_parse_args",
        lambda: SimpleNamespace(
            kit_root=kit_root,
            machine_label="win11-vm",
            windows_version="Windows 11 24H2",
            bundle_smoke_passed=True,
            preview_passed=True,
            short_project_summary=short_summary,
            long_project_summary=long_summary,
            short_output_video=None,
            long_output_video=None,
            bundle_doctor_report=None,
            bundle_smoke_log=None,
            blocker=[],
            note=["pass"],
        ),
    )

    exit_code = module.main()

    assert exit_code == 0
    json_report = json.loads((reports_dir / "clean_machine_validation_report.json").read_text(encoding="utf-8"))
    assert json_report["machine_label"] == "win11-vm"
    assert json_report["bundle_smoke_passed"] is True
    assert (reports_dir / "clean_machine_validation_report.md").exists()
