from __future__ import annotations

import json
from pathlib import Path
from sqlite3 import Row

from app.core.hashing import build_stage_hash
from app.project.database import ProjectDatabase
from app.project.models import ProjectWorkspace

from .models import TranslationPromptTemplate


def build_translation_stage_hash(
    *,
    segments: list[Row],
    template: TranslationPromptTemplate,
    model: str,
    source_language: str,
    target_language: str,
) -> str:
    segment_fingerprint = [
        {
            "segment_id": row["segment_id"],
            "source_text": row["source_text"],
            "start_ms": row["start_ms"],
            "end_ms": row["end_ms"],
        }
        for row in segments
    ]
    return build_stage_hash(
        {
            "stage": "translate",
            "segments": segment_fingerprint,
            "template": template.model_dump(mode="json"),
            "model": model,
            "source_language": source_language,
            "target_language": target_language,
            "version": 1,
        }
    )


def persist_translations(
    workspace: ProjectWorkspace,
    *,
    translated_items: list[dict[str, str]],
    stage_hash: str,
    template: TranslationPromptTemplate,
    model: str,
    source_language: str,
    target_language: str,
) -> Path:
    cache_dir = workspace.cache_dir / "translate" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "segments_translated.json"

    payload = {
        "stage_hash": stage_hash,
        "template_id": template.template_id,
        "model": model,
        "source_language": source_language,
        "target_language": target_language,
        "items": translated_items,
    }
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    database = ProjectDatabase(workspace.database_path)
    database.apply_segment_translations(workspace.project_id, translated_items)
    database.sync_canonical_subtitle_track(workspace.project_id)
    return cache_path


def load_cached_translations(
    workspace: ProjectWorkspace,
    stage_hash: str,
) -> list[dict[str, str]] | None:
    cache_path = workspace.cache_dir / "translate" / stage_hash / "segments_translated.json"
    if not cache_path.exists():
        return None
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    return payload.get("items")
