from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.project.database import ProjectDatabase
from app.project.models import (
    MediaAssetRecord,
    ProjectInitRequest,
    ProjectRecord,
    ProjectWorkspace,
)
from app.translate.presets import default_translation_mode_for_languages, ensure_prompt_templates

PROJECT_DIRS = (
    "assets",
    "assets/bgm",
    "assets/logos",
    "assets/voices",
    "cache/extract_audio",
    "cache/asr",
    "cache/translate",
    "cache/subs",
    "cache/tts",
    "cache/mix",
    "cache/export",
    "exports",
    "logs",
    "presets/prompts",
    "presets/styles",
    "presets/voices",
    "presets/exports",
    "presets/watermarks",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_default_presets(project_root: Path) -> None:
    export_preset = {
        "export_preset_id": "youtube-16x9",
        "name": "YouTube 16:9",
        "container": "mp4",
        "video_codec": "h264",
        "audio_codec": "aac",
        "resolution_mode": "keep",
        "target_aspect": "16:9",
        "target_width": 1920,
        "target_height": 1080,
        "crf": 18,
        "burn_subtitles": True,
        "watermark_enabled": False,
        "watermark_path": None,
        "watermark_position": "top-right",
        "watermark_opacity": 0.85,
        "watermark_scale": 0.16,
        "watermark_margin": 24,
        "notes": "Preset co ban cho YouTube ngang 16:9",
    }
    shorts_export_preset = {
        "export_preset_id": "shorts-9x16",
        "name": "Shorts 9:16",
        "container": "mp4",
        "video_codec": "h264",
        "audio_codec": "aac",
        "resolution_mode": "pad",
        "target_aspect": "9:16",
        "target_width": 1080,
        "target_height": 1920,
        "crf": 20,
        "burn_subtitles": True,
        "watermark_enabled": False,
        "watermark_path": None,
        "watermark_position": "top-right",
        "watermark_opacity": 0.85,
        "watermark_scale": 0.18,
        "watermark_margin": 28,
        "notes": "Preset shorts/doc cho nen tang dung 9:16, giu frame bang pad.",
    }
    watermark_none_profile = {
        "watermark_profile_id": "watermark-none",
        "name": "Khong watermark",
        "watermark_enabled": False,
        "watermark_path": None,
        "watermark_position": "top-right",
        "watermark_opacity": 0.85,
        "watermark_scale": 0.16,
        "watermark_margin": 24,
        "notes": "Tat watermark/logo cho lan export nay.",
    }
    watermark_logo_profile = {
        "watermark_profile_id": "watermark-logo-top-right",
        "name": "Logo top-right",
        "watermark_enabled": True,
        "watermark_path": "assets/logos/logo.png",
        "watermark_position": "top-right",
        "watermark_opacity": 0.85,
        "watermark_scale": 0.16,
        "watermark_margin": 24,
        "notes": "Mau reusable profile. Hay thay assets/logos/logo.png bang logo cua ban.",
    }
    style_preset = {
        "style_preset_id": "default-ass",
        "name": "Mac dinh ASS",
        "description": "Outline dam, phu hop hard-sub co ban",
        "ass_style_json": {
            "FontName": "Arial",
            "FontSize": 42,
            "Outline": 2,
            "Shadow": 0,
            "Alignment": 2,
            "MarginV": 48,
        },
    }
    voice_preset = {
        "voice_preset_id": "default-sapi",
        "name": "Windows SAPI Default",
        "engine": "sapi",
        "voice_id": "default",
        "speed": 1.0,
        "volume": 1.0,
        "pitch": 0.0,
        "sample_rate": 22050,
        "notes": "Fallback local offline TTS bang Windows SAPI.",
    }
    vieneu_voice_preset = {
        "voice_preset_id": "vieneu-default-vi",
        "name": "VieNeu Vietnamese",
        "engine": "vieneu",
        "voice_id": "default",
        "speed": 1.0,
        "volume": 1.0,
        "pitch": 0.0,
        "sample_rate": 24000,
        "language": "vi",
        "engine_options": {"mode": "local"},
        "notes": "TTS tieng Viet local bang VieNeu. Can cai package vieneu va eSpeak NG.",
    }
    vieneu_clone_preset = {
        "voice_preset_id": "vieneu-clone-template",
        "name": "VieNeu Voice Clone",
        "engine": "vieneu",
        "voice_id": "default",
        "speed": 1.0,
        "volume": 1.0,
        "pitch": 0.0,
        "sample_rate": 24000,
        "language": "vi",
        "engine_options": {
            "mode": "local",
            "ref_audio_path": "assets/voices/reference.wav",
            "ref_text": "",
        },
        "notes": "Preset clone giong mau. Hay thay ref_audio_path/ref_text bang mau cua ban trong tab Long tieng & Audio.",
    }

    (project_root / "presets" / "exports" / "default_hardsub.json").write_text(
        json.dumps(export_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "exports" / "shorts_9x16.json").write_text(
        json.dumps(shorts_export_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "watermarks" / "none.json").write_text(
        json.dumps(watermark_none_profile, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "watermarks" / "logo_top_right.json").write_text(
        json.dumps(watermark_logo_profile, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "styles" / "default_ass_style.json").write_text(
        json.dumps(style_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "voices" / "default_voice.json").write_text(
        json.dumps(voice_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "voices" / "vieneu_vi.json").write_text(
        json.dumps(vieneu_voice_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (project_root / "presets" / "voices" / "vieneu_clone_template.json").write_text(
        json.dumps(vieneu_clone_preset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def sync_project_snapshot(workspace: ProjectWorkspace) -> None:
    database = ProjectDatabase(workspace.database_path)
    project_row = database.get_project()
    if not project_row:
        return

    payload = dict(project_row)
    workspace.project_json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if payload.get("video_asset_id"):
        video_row = database.get_media_asset(payload["video_asset_id"])
        workspace.video_asset_id = payload["video_asset_id"]
        workspace.source_video_path = Path(video_row["path"]) if video_row else None
    else:
        workspace.video_asset_id = None
        workspace.source_video_path = None


def bootstrap_project(request: ProjectInitRequest) -> ProjectWorkspace:
    root_dir = request.root_dir.expanduser().resolve()
    root_dir.mkdir(parents=True, exist_ok=True)
    if (root_dir / "project.db").exists() or (root_dir / "project.json").exists():
        raise FileExistsError("Thu muc nay da co du an. Hay mo du an hien co hoac chon thu muc moi.")

    for relative_dir in PROJECT_DIRS:
        (root_dir / relative_dir).mkdir(parents=True, exist_ok=True)

    project_id = str(uuid4())
    now = utc_now_iso()
    database_path = root_dir / "project.db"
    project_json_path = root_dir / "project.json"

    asset_record = None
    if request.source_video_path and request.source_video_path.exists():
        source_video_path = request.source_video_path.resolve()
        asset_record = MediaAssetRecord(
            asset_id=str(uuid4()),
            project_id=project_id,
            asset_type="video",
            path=str(source_video_path),
            sha256=calculate_sha256(source_video_path),
            created_at=now,
        )

    database = ProjectDatabase(database_path)
    database.initialize()
    translation_mode = request.translation_mode or default_translation_mode_for_languages(
        request.source_language,
        request.target_language,
    )

    project_record = ProjectRecord(
        project_id=project_id,
        name=request.name,
        root_dir=str(root_dir),
        source_language=request.source_language,
        target_language=request.target_language,
        translation_mode=translation_mode,
        created_at=now,
        updated_at=now,
        video_asset_id=asset_record.asset_id if asset_record else None,
        active_voice_preset_id="default-sapi",
        active_export_preset_id="youtube-16x9",
        active_watermark_profile_id="watermark-none",
    )
    database.insert_project(project_record)
    database.ensure_canonical_subtitle_track(project_id, updated_at=now)

    if asset_record:
        database.insert_media_asset(asset_record)

    _write_default_presets(root_dir)
    ensure_prompt_templates(root_dir, request.source_language, request.target_language)

    workspace = ProjectWorkspace(
        project_id=project_id,
        name=request.name,
        root_dir=root_dir,
        database_path=database_path,
        project_json_path=project_json_path,
        logs_dir=root_dir / "logs",
        cache_dir=root_dir / "cache",
        exports_dir=root_dir / "exports",
        video_asset_id=asset_record.asset_id if asset_record else None,
        source_video_path=Path(asset_record.path) if asset_record else None,
    )
    sync_project_snapshot(workspace)
    return workspace


def open_project(root_dir: Path) -> ProjectWorkspace:
    resolved_root = root_dir.expanduser().resolve()
    project_json_path = resolved_root / "project.json"
    database_path = resolved_root / "project.db"
    if not project_json_path.exists() or not database_path.exists():
        raise FileNotFoundError("Thu muc du an khong co project.json hoac project.db")

    payload = json.loads(project_json_path.read_text(encoding="utf-8"))
    database = ProjectDatabase(database_path)
    database.initialize()
    ensure_prompt_templates(
        resolved_root,
        str(payload.get("source_language", "auto")),
        str(payload.get("target_language", "vi")),
    )
    workspace = ProjectWorkspace(
        project_id=payload["project_id"],
        name=payload["name"],
        root_dir=resolved_root,
        database_path=database_path,
        project_json_path=project_json_path,
        logs_dir=resolved_root / "logs",
        cache_dir=resolved_root / "cache",
        exports_dir=resolved_root / "exports",
        video_asset_id=payload.get("video_asset_id"),
    )
    sync_project_snapshot(workspace)
    return workspace
