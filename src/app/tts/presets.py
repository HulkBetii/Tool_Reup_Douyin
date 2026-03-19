from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .models import VoicePreset

_AUDIO_SUFFIXES = {".wav", ".mp3", ".m4a", ".aac", ".flac", ".ogg"}


@dataclass(slots=True)
class BatchVoicePresetImportReport:
    imported_presets: list[VoicePreset] = field(default_factory=list)
    skipped_missing_text: list[Path] = field(default_factory=list)
    skipped_empty_text: list[Path] = field(default_factory=list)


def _voice_presets_dir(project_root: Path) -> Path:
    return project_root / "presets" / "voices"


def _slugify_token(raw_value: str, *, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", raw_value.strip().lower()).strip("-")
    return slug or fallback


def _normalize_preset_reference_path(project_root: Path, path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(project_root.resolve()))
    except ValueError:
        return str(resolved)


def _build_unique_preset_id(project_root: Path, base_id: str) -> str:
    existing_ids = {preset.voice_preset_id for preset in list_voice_presets(project_root)}
    if base_id not in existing_ids:
        return base_id
    suffix = 2
    while f"{base_id}-{suffix}" in existing_ids:
        suffix += 1
    return f"{base_id}-{suffix}"


def list_voice_presets(project_root: Path) -> list[VoicePreset]:
    presets_dir = _voice_presets_dir(project_root)
    if not presets_dir.exists():
        return []

    presets: list[VoicePreset] = []
    for path in sorted(presets_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            presets.append(VoicePreset.model_validate(payload))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
    return presets


def find_voice_preset_path(project_root: Path, voice_preset_id: str) -> Path | None:
    presets_dir = _voice_presets_dir(project_root)
    if not presets_dir.exists():
        return None

    for path in sorted(presets_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("voice_preset_id") == voice_preset_id:
            return path
    return None


def save_voice_preset(project_root: Path, preset: VoicePreset) -> Path:
    presets_dir = _voice_presets_dir(project_root)
    presets_dir.mkdir(parents=True, exist_ok=True)
    path = find_voice_preset_path(project_root, preset.voice_preset_id)
    if path is None:
        path = presets_dir / f"{preset.voice_preset_id}.json"
    path.write_text(
        json.dumps(preset.model_dump(mode="json"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def delete_voice_preset(project_root: Path, voice_preset_id: str) -> Path | None:
    path = find_voice_preset_path(project_root, voice_preset_id)
    if path is None or not path.exists():
        return None
    path.unlink()
    return path


def batch_import_voice_clone_presets(
    project_root: Path,
    *,
    template_preset: VoicePreset,
    source_dir: Path | None = None,
) -> BatchVoicePresetImportReport:
    voices_dir = (source_dir or (project_root / "assets" / "voices")).expanduser().resolve()
    report = BatchVoicePresetImportReport()
    if not voices_dir.exists():
        return report

    base_engine_options = dict(template_preset.engine_options)
    base_engine_options.setdefault("mode", "local")
    base_engine_options.pop("ref_audio_path", None)
    base_engine_options.pop("ref_text", None)

    audio_files = sorted(
        path for path in voices_dir.rglob("*") if path.is_file() and path.suffix.lower() in _AUDIO_SUFFIXES
    )
    for audio_path in audio_files:
        text_path = audio_path.with_suffix(".txt")
        if not text_path.exists():
            report.skipped_missing_text.append(audio_path)
            continue
        ref_text = text_path.read_text(encoding="utf-8").strip()
        if not ref_text:
            report.skipped_empty_text.append(audio_path)
            continue

        stem = audio_path.stem
        preset_name = f"{template_preset.name} - {stem}".strip(" -")
        preset_id = _build_unique_preset_id(
            project_root,
            f"vieneu-clone-{_slugify_token(stem, fallback='voice')}",
        )
        preset = template_preset.model_copy(
            update={
                "voice_preset_id": preset_id,
                "name": preset_name,
                "engine": "vieneu",
                "sample_rate": template_preset.sample_rate or 24000,
                "language": template_preset.language or "vi",
                "engine_options": {
                    **base_engine_options,
                    "mode": str(base_engine_options.get("mode", "local") or "local"),
                    "ref_audio_path": _normalize_preset_reference_path(project_root, audio_path),
                    "ref_text": ref_text,
                },
            }
        )
        save_voice_preset(project_root, preset)
        report.imported_presets.append(preset)
    return report
