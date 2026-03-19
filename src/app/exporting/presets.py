from __future__ import annotations

import json
from pathlib import Path

from .models import ExportPreset, WatermarkProfile


def _export_presets_dir(project_root: Path) -> Path:
    return project_root / "presets" / "exports"


def _watermark_profiles_dir(project_root: Path) -> Path:
    return project_root / "presets" / "watermarks"


def list_export_presets(project_root: Path) -> list[ExportPreset]:
    presets_dir = _export_presets_dir(project_root)
    if not presets_dir.exists():
        return []

    presets: list[ExportPreset] = []
    for path in sorted(presets_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            presets.append(ExportPreset.model_validate(payload))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
    return presets


def get_export_preset(project_root: Path, preset_id: str | None = None) -> ExportPreset | None:
    presets = list_export_presets(project_root)
    if not presets:
        return None
    if preset_id:
        for preset in presets:
            if preset.export_preset_id == preset_id:
                return preset
    return presets[0]


def list_watermark_profiles(project_root: Path) -> list[WatermarkProfile]:
    profiles_dir = _watermark_profiles_dir(project_root)
    if not profiles_dir.exists():
        return []

    profiles: list[WatermarkProfile] = []
    for path in sorted(profiles_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            profiles.append(WatermarkProfile.model_validate(payload))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
    return profiles


def find_watermark_profile_path(project_root: Path, watermark_profile_id: str) -> Path | None:
    profiles_dir = _watermark_profiles_dir(project_root)
    if not profiles_dir.exists():
        return None

    for path in sorted(profiles_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("watermark_profile_id") == watermark_profile_id:
            return path
    return None


def save_watermark_profile(project_root: Path, profile: WatermarkProfile) -> Path:
    profiles_dir = _watermark_profiles_dir(project_root)
    profiles_dir.mkdir(parents=True, exist_ok=True)
    path = find_watermark_profile_path(project_root, profile.watermark_profile_id)
    if path is None:
        path = profiles_dir / f"{profile.watermark_profile_id}.json"
    path.write_text(
        json.dumps(profile.model_dump(mode="json"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path
