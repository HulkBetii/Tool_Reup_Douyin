from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "prepare_clean_machine_validation.py"
    spec = importlib.util.spec_from_file_location("prepare_clean_machine_validation", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_prepare_clean_machine_validation_runs_build_and_smoke(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_script_module()
    bundle_dir = tmp_path / "dist" / module.APP_SLUG
    short_project = tmp_path / "short-project"
    long_project = tmp_path / "long-project"
    short_project.mkdir(parents=True, exist_ok=True)
    long_project.mkdir(parents=True, exist_ok=True)
    commands: list[tuple[str, list[str]]] = []

    def _fake_run(script_path: Path, arguments: list[str], *, extra_env=None):
        commands.append((script_path.name, list(arguments), dict(extra_env or {})))
        if script_path.name == "build_pyinstaller.ps1":
            bundle_dir.mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def _fake_prepare(**kwargs):
        kit_root = Path(kwargs["kit_root"])
        kit_root.mkdir(parents=True, exist_ok=True)
        local_smoke_log_path = kit_root / "logs" / "local_smoke_bundle.log"
        local_smoke_log_path.parent.mkdir(parents=True, exist_ok=True)
        local_doctor_report_path = kit_root / "reports" / "local_bundle_doctor_report.json"
        local_doctor_report_path.parent.mkdir(parents=True, exist_ok=True)
        local_doctor_report_path.write_text("{}", encoding="utf-8")
        (kit_root / "validation_kit_manifest.json").write_text("{}", encoding="utf-8")
        (kit_root / "CLEAN_MACHINE_README.md").write_text("readme", encoding="utf-8")
        report_template_path = kit_root / "reports" / "clean_machine_validation_report.template.json"
        report_template_path.write_text("{}", encoding="utf-8")
        return SimpleNamespace(
            kit_root=kit_root,
            bundle_dir=Path(kwargs["bundle_dir"]),
            instructions_path=kit_root / "CLEAN_MACHINE_README.md",
            report_template_path=report_template_path,
            local_smoke_log_path=local_smoke_log_path,
            local_doctor_report_path=local_doctor_report_path,
            projects=[],
        )

    monkeypatch.setattr(module, "_run_powershell", _fake_run)
    monkeypatch.setattr(module, "prepare_validation_kit", _fake_prepare)
    monkeypatch.setattr(
        module,
        "_parse_args",
        lambda: SimpleNamespace(
            short_project_root=short_project,
            long_project_root=long_project,
            kit_root=tmp_path / "kit",
            bundle_dir=bundle_dir,
            python_exe=Path("C:/Python/python.exe"),
            clean_build=True,
            skip_build=False,
            skip_local_smoke=False,
        ),
    )

    exit_code = module.main()

    assert exit_code == 0
    assert commands[0][0] == "build_pyinstaller.ps1"
    assert "-Clean" in commands[0][1]
    assert commands[1][0] == "smoke_release_bundle.ps1"
    assert "-DoctorStages" in commands[1][1]
    assert commands[1][1][-1] == "preview,tts,voice_track,mixdown,export_video"
    assert commands[1][2]["APPDATA"].endswith("kit\\sandbox_appdata")
    assert commands[1][2]["LOCALAPPDATA"].endswith("kit\\sandbox_localappdata")
    summary_path = tmp_path / "kit" / "prepare_clean_machine_validation_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["local_smoke"]["status"] == "passed"
    assert payload["local_smoke"]["appdata_root"].endswith("kit\\sandbox_appdata")
    assert payload["local_smoke"]["localappdata_root"].endswith("kit\\sandbox_localappdata")
