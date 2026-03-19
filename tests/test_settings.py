from __future__ import annotations

import json
from pathlib import Path

from app.core.settings import AppSettings, load_settings, save_settings


def test_settings_roundtrip(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    appdata_dir = tmp_path / "appdata"

    settings = AppSettings(
        ui_language="vi",
        model_cache_dir=str(tmp_path / "models"),
    )
    settings.dependency_paths.ffmpeg_path = "C:/ffmpeg/bin/ffmpeg.exe"
    settings.openai_api_key = "sk-test-secret"
    save_settings(settings, settings_path=settings_path, appdata_dir=appdata_dir)
    payload = json.loads(settings_path.read_text(encoding="utf-8"))

    loaded = load_settings(settings_path=settings_path, appdata_dir=appdata_dir)

    assert loaded.ui_language == "vi"
    assert loaded.dependency_paths.ffmpeg_path == "C:/ffmpeg/bin/ffmpeg.exe"
    assert loaded.model_cache_dir == str(tmp_path / "models")
    assert loaded.openai_api_key == "sk-test-secret"
    assert payload["openai_api_key_encrypted"]
    assert "openai_api_key" not in payload


def test_load_settings_creates_defaults_when_missing(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    appdata_dir = tmp_path / "appdata"

    loaded = load_settings(settings_path=settings_path, appdata_dir=appdata_dir)

    assert settings_path.exists()
    assert loaded.ui_language == "vi"
    assert loaded.model_cache_dir is not None


def test_load_settings_migrates_legacy_plaintext_openai_key(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    appdata_dir = tmp_path / "appdata"
    settings_path.write_text(
        json.dumps(
            {
                "ui_language": "vi",
                "dependency_paths": {},
                "model_cache_dir": str(tmp_path / "models"),
                "openai_api_key": "sk-legacy-secret",
                "default_asr_model": "small",
                "default_translation_model": "gpt-4.1-mini",
                "gpu_enabled": False,
                "telemetry_opt_out": True,
                "recent_projects": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded = load_settings(settings_path=settings_path, appdata_dir=appdata_dir)
    migrated_payload = json.loads(settings_path.read_text(encoding="utf-8"))

    assert loaded.openai_api_key == "sk-legacy-secret"
    assert migrated_payload["openai_api_key_encrypted"]
    assert "openai_api_key" not in migrated_payload
