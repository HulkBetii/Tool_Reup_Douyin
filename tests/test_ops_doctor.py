from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.ffmpeg import FFmpegInstallation, ToolProbe
from app.core.settings import AppSettings, DependencyPaths
from app.ops.doctor import run_doctor
from app.tts.models import VoicePreset
from app.tts.vieneu_engine import VieneuEnvironment


@pytest.fixture
def writable_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    appdata_dir = tmp_path / "appdata"
    appdata_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("app.ops.doctor.get_appdata_dir", lambda: appdata_dir)
    monkeypatch.setattr("app.ops.doctor._is_writable_directory", lambda _path: True)
    monkeypatch.setattr(
        "app.ops.doctor.shutil.disk_usage",
        lambda _path: shutil._ntuple_diskusage(total=10 * 1024**3, used=2 * 1024**3, free=8 * 1024**3),
    )
    monkeypatch.setattr(
        "app.ops.doctor.resolve_mpv_dll_path",
        lambda _value: tmp_path / "mpv-2.dll",
    )
    monkeypatch.setattr(
        "app.ops.doctor.importlib.util.find_spec",
        lambda name: object() if name == "mpv" else None,
    )
    monkeypatch.setattr(
        "app.ops.doctor.detect_ffmpeg_installation",
        lambda _settings: FFmpegInstallation(
            ffmpeg=ToolProbe(name="ffmpeg", executable="ffmpeg.exe", available=True, version_line="ffmpeg ok"),
            ffprobe=ToolProbe(name="ffprobe", executable="ffprobe.exe", available=True, version_line="ffprobe ok"),
        ),
    )
    monkeypatch.setattr(
        "app.ops.doctor.detect_vieneu_installation",
        lambda: VieneuEnvironment(
            package_installed=True,
            package_version="1.0.0",
            espeak_path=tmp_path / "espeak-ng.exe",
            detail="VieNeu local san sang",
        ),
    )


def _settings(tmp_path: Path, *, openai_api_key: str | None = "key") -> AppSettings:
    settings = AppSettings(
        dependency_paths=DependencyPaths(),
        model_cache_dir=str(tmp_path / "models"),
        default_translation_model="gpt-4.1-mini",
    )
    settings.openai_api_key = openai_api_key
    return settings


def test_run_doctor_blocks_translation_when_openai_key_missing(
    tmp_path: Path,
    writable_environment: None,
) -> None:
    workspace = SimpleNamespace(root_dir=tmp_path / "workspace", cache_dir=tmp_path / "workspace" / "cache")
    workspace.root_dir.mkdir(parents=True, exist_ok=True)
    workspace.cache_dir.mkdir(parents=True, exist_ok=True)

    report = run_doctor(
        settings=_settings(tmp_path, openai_api_key=None),
        workspace=workspace,
        requested_stages=["translate"],
    )

    blocking_names = {item.name for item in report.blocking_checks_for(["translate"])}
    assert "openai_api_key" in blocking_names


def test_run_doctor_blocks_media_stages_when_ffmpeg_missing(
    tmp_path: Path,
    writable_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.ops.doctor.detect_ffmpeg_installation",
        lambda _settings: FFmpegInstallation(
            ffmpeg=ToolProbe(name="ffmpeg", executable=None, available=False, error="Not found"),
            ffprobe=ToolProbe(name="ffprobe", executable=None, available=False, error="Not found"),
        ),
    )
    workspace = SimpleNamespace(root_dir=tmp_path / "workspace", cache_dir=tmp_path / "workspace" / "cache")
    workspace.root_dir.mkdir(parents=True, exist_ok=True)
    workspace.cache_dir.mkdir(parents=True, exist_ok=True)

    report = run_doctor(
        settings=_settings(tmp_path),
        workspace=workspace,
        requested_stages=["extract_audio"],
    )

    blocking_names = {item.name for item in report.blocking_checks_for(["extract_audio"])}
    assert {"ffmpeg", "ffprobe"} <= blocking_names


def test_run_doctor_blocks_tts_when_vieneu_local_missing_espeak(
    tmp_path: Path,
    writable_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.ops.doctor.detect_vieneu_installation",
        lambda: VieneuEnvironment(
            package_installed=True,
            package_version="1.0.0",
            espeak_path=None,
            detail="Da cai VieNeu nhung chua tim thay eSpeak NG",
        ),
    )
    workspace = SimpleNamespace(root_dir=tmp_path / "workspace", cache_dir=tmp_path / "workspace" / "cache")
    workspace.root_dir.mkdir(parents=True, exist_ok=True)
    workspace.cache_dir.mkdir(parents=True, exist_ok=True)
    preset = VoicePreset(
        voice_preset_id="vieneu-default-vi",
        name="VieNeu",
        engine="vieneu",
        sample_rate=24000,
        engine_options={"mode": "local"},
    )

    report = run_doctor(
        settings=_settings(tmp_path),
        workspace=workspace,
        requested_stages=["tts"],
        voice_preset=preset,
    )

    blocking_names = {item.name for item in report.blocking_checks_for(["tts"])}
    assert "vieneu_local" in blocking_names
