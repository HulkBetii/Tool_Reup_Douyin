from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from app.project.models import CharacterProfileRecord, RelationshipProfileRecord, SceneMemoryRecord, SegmentAnalysisRecord

from .models import ContextualRunMetrics, SceneRouteDecision, SceneTermEntitySheet, TranslationPromptTemplate


@dataclass(slots=True)
class ContextualTranslationCheckpointState:
    scenes: list[SceneMemoryRecord]
    character_profiles: list[CharacterProfileRecord]
    relationship_profiles: list[RelationshipProfileRecord]
    analyses: list[SegmentAnalysisRecord]
    route_decisions: list[SceneRouteDecision]
    term_entity_sheets: list[SceneTermEntitySheet]
    metrics: ContextualRunMetrics
    completed_scene_ids: list[str]
    completed_scene_count: int
    total_scene_count: int


def _cache_dir(workspace, stage_hash: str) -> Path:
    return workspace.cache_dir / "translate_contextual" / stage_hash


def checkpoint_path(workspace, stage_hash: str) -> Path:
    return _cache_dir(workspace, stage_hash) / "contextual_translation.partial.json"


def _restore_record_list(payload_items: list[dict[str, object]], record_type):
    return [record_type(**item) for item in payload_items]


def _build_checkpoint_payload(
    *,
    stage_hash: str,
    selected_template: TranslationPromptTemplate,
    scenes: list[SceneMemoryRecord],
    character_profiles: list[CharacterProfileRecord],
    relationship_profiles: list[RelationshipProfileRecord],
    analyses: list[SegmentAnalysisRecord],
    route_decisions: list[SceneRouteDecision],
    term_entity_sheets: list[SceneTermEntitySheet],
    metrics: ContextualRunMetrics,
    completed_scene_ids: list[str],
    total_scene_count: int,
) -> dict[str, object]:
    return {
        "stage_hash": stage_hash,
        "selected_template_id": selected_template.template_id,
        "selected_template_family_id": selected_template.family_id,
        "translation_mode": selected_template.translation_mode,
        "scenes": [asdict(item) for item in scenes],
        "character_profiles": [asdict(item) for item in character_profiles],
        "relationship_profiles": [asdict(item) for item in relationship_profiles],
        "segment_analyses": [asdict(item) for item in analyses],
        "route_decisions": [item.model_dump(mode="json") for item in route_decisions],
        "term_entity_sheets": [item.model_dump(mode="json") for item in term_entity_sheets],
        "metrics": metrics.model_dump(mode="json"),
        "checkpoint": {
            "completed_scene_ids": list(completed_scene_ids),
            "completed_scene_count": len(completed_scene_ids),
            "total_scene_count": total_scene_count,
        },
    }


def persist_contextual_translation_checkpoint(
    workspace,
    *,
    stage_hash: str,
    selected_template: TranslationPromptTemplate,
    scenes: list[SceneMemoryRecord],
    character_profiles: list[CharacterProfileRecord],
    relationship_profiles: list[RelationshipProfileRecord],
    analyses: list[SegmentAnalysisRecord],
    route_decisions: list[SceneRouteDecision],
    term_entity_sheets: list[SceneTermEntitySheet],
    metrics: ContextualRunMetrics,
    completed_scene_ids: list[str],
    total_scene_count: int,
) -> Path:
    cache_dir = _cache_dir(workspace, stage_hash)
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = _build_checkpoint_payload(
        stage_hash=stage_hash,
        selected_template=selected_template,
        scenes=scenes,
        character_profiles=character_profiles,
        relationship_profiles=relationship_profiles,
        analyses=analyses,
        route_decisions=route_decisions,
        term_entity_sheets=term_entity_sheets,
        metrics=metrics,
        completed_scene_ids=completed_scene_ids,
        total_scene_count=total_scene_count,
    )
    path = checkpoint_path(workspace, stage_hash)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def load_contextual_translation_checkpoint(
    workspace,
    *,
    stage_hash: str,
) -> ContextualTranslationCheckpointState | None:
    path = checkpoint_path(workspace, stage_hash)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    checkpoint_meta = dict(payload.get("checkpoint") or {})
    return ContextualTranslationCheckpointState(
        scenes=_restore_record_list(payload.get("scenes", []), SceneMemoryRecord),
        character_profiles=_restore_record_list(payload.get("character_profiles", []), CharacterProfileRecord),
        relationship_profiles=_restore_record_list(payload.get("relationship_profiles", []), RelationshipProfileRecord),
        analyses=_restore_record_list(payload.get("segment_analyses", []), SegmentAnalysisRecord),
        route_decisions=[SceneRouteDecision.model_validate(item) for item in payload.get("route_decisions", [])],
        term_entity_sheets=[SceneTermEntitySheet.model_validate(item) for item in payload.get("term_entity_sheets", [])],
        metrics=ContextualRunMetrics.model_validate(payload.get("metrics") or {}),
        completed_scene_ids=[str(item) for item in checkpoint_meta.get("completed_scene_ids", [])],
        completed_scene_count=int(checkpoint_meta.get("completed_scene_count", 0)),
        total_scene_count=int(checkpoint_meta.get("total_scene_count", 0)),
    )


def clear_contextual_translation_checkpoint(workspace, *, stage_hash: str) -> None:
    path = checkpoint_path(workspace, stage_hash)
    if path.exists():
        path.unlink()
