from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from sqlite3 import Row

from app.core.hashing import build_stage_hash
from app.core.jobs import JobContext
from app.project.database import ProjectDatabase
from app.project.models import (
    CharacterProfileRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
)

from .models import TranslationPromptTemplate
from .openai_engine import OpenAITranslationEngine
from .presets import resolve_prompt_family
from .relationship_memory import clone_allowed_alternates
from .scene_chunker import SceneChunk, chunk_segments_into_scenes
from .semantic_qc import analyze_segment_analyses

CONTEXTUAL_STAGE_BATCH_SIZE = 8


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_contextual_translation_stage_hash(
    *,
    segments: list[Row],
    template: TranslationPromptTemplate,
    project_root: Path | None = None,
    model: str,
    source_language: str,
    target_language: str,
) -> str:
    prompt_family = (
        resolve_prompt_family(project_root, template)
        if project_root is not None
        else {template.role: template}
    )
    return build_stage_hash(
        {
            "stage": "translate_contextual_v2",
            "segments": [
                {
                    "segment_id": row["segment_id"],
                    "segment_index": row["segment_index"],
                    "start_ms": row["start_ms"],
                    "end_ms": row["end_ms"],
                    "source_text": row["source_text"],
                }
                for row in segments
            ],
            "prompt_family": {
                role: {
                    "template_id": prompt.template_id,
                    "family_id": prompt.family_id,
                    "translation_mode": prompt.translation_mode,
                    "role": prompt.role,
                    "system_prompt": prompt.system_prompt,
                    "user_prompt_template": prompt.user_prompt_template,
                    "output_schema_version": prompt.output_schema_version,
                    "default_constraints_json": prompt.default_constraints_json,
                }
                for role, prompt in sorted(prompt_family.items())
            },
            "model": model,
            "source_language": source_language,
            "target_language": target_language,
            "version": 3,
        }
    )


def _cache_dir(workspace, stage_hash: str) -> Path:
    return workspace.cache_dir / "translate_contextual" / stage_hash


def _chunk_scene_segments(
    segment_rows: list[Row],
    batch_size: int = CONTEXTUAL_STAGE_BATCH_SIZE,
) -> list[list[Row]]:
    if not segment_rows:
        return []
    return [segment_rows[index : index + batch_size] for index in range(0, len(segment_rows), batch_size)]


def _scene_row_payload(
    scene: SceneChunk,
    *,
    rows: list[Row] | None = None,
    batch_index: int | None = None,
    batch_count: int | None = None,
) -> dict[str, object]:
    segment_rows = rows or scene.segments
    start_segment_index = int(segment_rows[0]["segment_index"]) if segment_rows else scene.start_segment_index
    end_segment_index = int(segment_rows[-1]["segment_index"]) if segment_rows else scene.end_segment_index
    start_ms = int(segment_rows[0]["start_ms"]) if segment_rows else scene.start_ms
    end_ms = int(segment_rows[-1]["end_ms"]) if segment_rows else scene.end_ms
    payload = {
        "scene_id": scene.scene_id,
        "scene_index": scene.scene_index,
        "start_segment_index": start_segment_index,
        "end_segment_index": end_segment_index,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "duration_ms": max(0, end_ms - start_ms),
        "segment_count": len(segment_rows),
        "total_scene_segment_count": len(scene.segment_ids),
        "is_partial_batch": len(segment_rows) != len(scene.segment_ids),
        "segments": [
            {
                "segment_id": str(row["segment_id"]),
                "segment_index": int(row["segment_index"]),
                "start_ms": int(row["start_ms"]),
                "end_ms": int(row["end_ms"]),
                "source_text": row["source_text"],
            }
            for row in segment_rows
        ],
    }
    if batch_index is not None:
        payload["batch_index"] = batch_index
    if batch_count is not None:
        payload["batch_count"] = batch_count
    return payload


def _validate_stage_item_ids(
    stage_label: str,
    *,
    expected_ids: list[str],
    actual_ids: list[str],
    scene_id: str,
    batch_index: int,
    batch_count: int,
) -> None:
    expected_set = set(expected_ids)
    actual_set = set(actual_ids)
    if expected_set == actual_set:
        return
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    raise RuntimeError(
        (
            f"{stage_label} không trả về đủ segment ids "
            f"(scene={scene_id}, batch={batch_index}/{batch_count}). "
            f"Missing={missing} Extra={extra}"
        )
    )


def _scene_batch_payload(
    scene: SceneChunk,
    batch_rows: list[Row],
    *,
    batch_index: int,
    batch_count: int,
) -> dict[str, object]:
    return {
        "scene": _scene_row_payload(
            scene,
            rows=batch_rows,
            batch_index=batch_index,
            batch_count=batch_count,
        ),
        "batch_segment_ids": [str(row["segment_id"]) for row in batch_rows],
    }


def _serialize_character_rows(rows: list[object]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, CharacterProfileRecord):
            payloads.append(
                {
                    "character_id": row.character_id,
                    "canonical_name_zh": row.canonical_name_zh,
                    "canonical_name_vi": row.canonical_name_vi,
                    "aliases": list(row.aliases_json),
                    "gender_hint": row.gender_hint,
                    "age_role": row.age_role,
                    "social_role": row.social_role,
                    "speech_style": row.speech_style,
                    "default_self_terms": list(row.default_self_terms_json),
                    "default_address_terms": list(row.default_address_terms_json),
                    "confidence": row.confidence,
                    "status": row.status,
                }
            )
            continue
        payloads.append(
            {
                "character_id": row["character_id"],
                "canonical_name_zh": row["canonical_name_zh"],
                "canonical_name_vi": row["canonical_name_vi"],
                "aliases": json.loads(row["aliases_json"] or "[]"),
                "gender_hint": row["gender_hint"],
                "age_role": row["age_role"],
                "social_role": row["social_role"],
                "speech_style": row["speech_style"],
                "default_self_terms": json.loads(row["default_self_terms_json"] or "[]"),
                "default_address_terms": json.loads(row["default_address_terms_json"] or "[]"),
                "confidence": float(row["confidence"] or 0.0),
                "status": row["status"],
            }
        )
    return payloads


def _serialize_relationship_rows(rows: list[object]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, RelationshipProfileRecord):
            payloads.append(
                {
                    "relationship_id": row.relationship_id,
                    "from_character_id": row.from_character_id,
                    "to_character_id": row.to_character_id,
                    "relation_type": row.relation_type,
                    "default_self_term": row.default_self_term,
                    "default_address_term": row.default_address_term,
                    "allowed_alternates": clone_allowed_alternates(row.allowed_alternates_json),
                    "status": row.status,
                }
            )
            continue
        payloads.append(
            {
                "relationship_id": row["relationship_id"],
                "from_character_id": row["from_character_id"],
                "to_character_id": row["to_character_id"],
                "relation_type": row["relation_type"],
                "default_self_term": row["default_self_term"],
                "default_address_term": row["default_address_term"],
                "allowed_alternates": clone_allowed_alternates(json.loads(row["allowed_alternates_json"] or "[]")),
                "status": row["status"],
            }
        )
    return payloads


def _build_context_payload(
    *,
    scene: SceneChunk,
    planner_summary: str = "",
    existing_character_rows: list[object],
    existing_relationship_rows: list[object],
) -> dict[str, object]:
    recent_turns = [
        {
            "segment_id": str(row["segment_id"]),
            "segment_index": int(row["segment_index"]),
            "source_text": row["source_text"],
        }
        for row in scene.segments[:6]
    ]
    return {
        "scene_id": scene.scene_id,
        "recent_turns": recent_turns,
        "scene_summary": planner_summary,
        "character_profiles": _serialize_character_rows(existing_character_rows),
        "relationship_profiles": _serialize_relationship_rows(existing_relationship_rows),
    }


def _build_glossary_payload(
    database: ProjectDatabase,
    project_id: str,
    *,
    relationship_rows: list[object] | None = None,
    narration_term_sheet: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    glossary_entries: list[dict[str, str]] = []
    rows = relationship_rows if relationship_rows is not None else database.list_relationship_profiles(project_id)
    for row in rows:
        if isinstance(row, RelationshipProfileRecord):
            if row.default_address_term or row.default_self_term:
                glossary_entries.append(
                    {
                        "relationship_id": row.relationship_id,
                        "from_character_id": row.from_character_id,
                        "to_character_id": row.to_character_id,
                        "default_self_term": row.default_self_term or "",
                        "default_address_term": row.default_address_term or "",
                    }
                )
            continue
        if row["default_address_term"] or row["default_self_term"]:
            glossary_entries.append(
                {
                    "relationship_id": row["relationship_id"],
                    "from_character_id": row["from_character_id"],
                    "to_character_id": row["to_character_id"],
                    "default_self_term": row["default_self_term"] or "",
                    "default_address_term": row["default_address_term"] or "",
                }
            )
    payload: dict[str, object] = {"relationship_glossary": glossary_entries}
    if narration_term_sheet is not None:
        payload["narration_term_sheet"] = list(narration_term_sheet)
    return payload


def _relationship_defaults_map(database: ProjectDatabase, project_id: str) -> dict[tuple[str, str], dict[str, object]]:
    defaults: dict[tuple[str, str], dict[str, object]] = {}
    for row in database.list_relationship_profiles(project_id):
        defaults[(str(row["from_character_id"]), str(row["to_character_id"]))] = {
            "default_self_term": row["default_self_term"],
            "default_address_term": row["default_address_term"],
            "allowed_alternates_json": clone_allowed_alternates(json.loads(row["allowed_alternates_json"] or "[]")),
            "status": row["status"] or "hypothesized",
        }
    return defaults


def _character_record_from_seed(project_id: str, seed, *, now: str) -> CharacterProfileRecord:
    return CharacterProfileRecord(
        character_id=seed.character_id,
        project_id=project_id,
        canonical_name_zh=seed.canonical_name_zh,
        canonical_name_vi=seed.canonical_name_vi,
        aliases_json=list(seed.aliases),
        gender_hint=seed.gender_hint,
        age_role=seed.age_role,
        social_role=seed.social_role,
        speech_style=seed.speech_style,
        default_self_terms_json=list(seed.default_self_terms),
        default_address_terms_json=list(seed.default_address_terms),
        confidence=seed.confidence,
        status=seed.status,
        evidence_segment_ids_json=list(seed.evidence_segment_ids),
        created_at=now,
        updated_at=now,
    )


def _relationship_record_from_seed(project_id: str, seed, *, now: str, scene_id: str) -> RelationshipProfileRecord:
    return RelationshipProfileRecord(
        relationship_id=seed.relationship_id,
        project_id=project_id,
        from_character_id=seed.from_character_id,
        to_character_id=seed.to_character_id,
        relation_type=seed.relation_type,
        power_delta=seed.power_delta,
        age_delta=seed.age_delta,
        intimacy_level=seed.intimacy_level,
        default_self_term=seed.default_self_term,
        default_address_term=seed.default_address_term,
        allowed_alternates_json=clone_allowed_alternates(seed.allowed_alternates),
        scope=seed.scope,
        status=seed.status,
        evidence_segment_ids_json=list(seed.evidence_segment_ids),
        last_updated_scene_id=scene_id,
        created_at=now,
        updated_at=now,
    )


def _scene_record_from_output(project_id: str, scene: SceneChunk, planner, *, now: str) -> SceneMemoryRecord:
    return SceneMemoryRecord(
        scene_id=scene.scene_id,
        project_id=project_id,
        scene_index=scene.scene_index,
        start_segment_index=scene.start_segment_index,
        end_segment_index=scene.end_segment_index,
        start_ms=scene.start_ms,
        end_ms=scene.end_ms,
        participants_json=list(planner.participants),
        location=planner.location,
        time_context=planner.time_context,
        short_scene_summary=planner.scene_summary,
        recent_turn_digest=planner.recent_turn_digest,
        active_topic=planner.active_topic,
        current_conflict=planner.current_conflict,
        current_emotional_tone=planner.current_emotional_tone,
        temporary_addressing_mode=planner.temporary_addressing_mode,
        who_knows_what_json=[item.model_dump(mode="json", by_alias=True) for item in planner.who_knows_what],
        open_ambiguities_json=list(planner.open_ambiguities),
        unresolved_references_json=list(planner.unresolved_references),
        status="planned",
        created_at=now,
        updated_at=now,
    )


def _cache_payload(
    *,
    stage_hash: str,
    selected_template: TranslationPromptTemplate,
    scenes: list[SceneMemoryRecord],
    character_profiles: list[CharacterProfileRecord],
    relationship_profiles: list[RelationshipProfileRecord],
    analyses: list[SegmentAnalysisRecord],
    route_decisions: list[object] | None = None,
    metrics: object | None = None,
    term_entity_sheets: list[object] | None = None,
) -> dict[str, object]:
    payload = {
        "stage_hash": stage_hash,
        "selected_template_id": selected_template.template_id,
        "selected_template_family_id": selected_template.family_id,
        "translation_mode": selected_template.translation_mode,
        "scenes": [asdict(item) for item in scenes],
        "character_profiles": [asdict(item) for item in character_profiles],
        "relationship_profiles": [asdict(item) for item in relationship_profiles],
        "segment_analyses": [asdict(item) for item in analyses],
    }
    if route_decisions is not None:
        payload["route_decisions"] = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in route_decisions
        ]
    if metrics is not None:
        payload["metrics"] = metrics.model_dump(mode="json") if hasattr(metrics, "model_dump") else metrics
    if term_entity_sheets is not None:
        payload["term_entity_sheets"] = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in term_entity_sheets
        ]
    return payload


def persist_contextual_translation_result(
    workspace,
    *,
    database: ProjectDatabase,
    stage_hash: str,
    selected_template: TranslationPromptTemplate,
    target_language: str,
    scenes: list[SceneMemoryRecord],
    character_profiles: list[CharacterProfileRecord],
    relationship_profiles: list[RelationshipProfileRecord],
    analyses: list[SegmentAnalysisRecord],
    route_decisions: list[object] | None = None,
    metrics: object | None = None,
    term_entity_sheets: list[object] | None = None,
) -> Path:
    cache_dir = _cache_dir(workspace, stage_hash)
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = _cache_payload(
        stage_hash=stage_hash,
        selected_template=selected_template,
        scenes=scenes,
        character_profiles=character_profiles,
        relationship_profiles=relationship_profiles,
        analyses=analyses,
        route_decisions=route_decisions,
        metrics=metrics,
        term_entity_sheets=term_entity_sheets,
    )
    cache_path = cache_dir / "contextual_translation.json"
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    database.replace_contextual_translation_state(
        workspace.project_id,
        scenes=scenes,
        analyses=analyses,
        character_profiles=character_profiles,
        relationship_profiles=relationship_profiles,
    )
    database.apply_segment_analysis_outputs(
        workspace.project_id,
        target_language=target_language,
    )
    return cache_path


def _restore_record_list(payload_items: list[dict[str, object]], record_type):
    return [record_type(**item) for item in payload_items]


def load_cached_contextual_translation(workspace, stage_hash: str) -> dict[str, object] | None:
    cache_path = _cache_dir(workspace, stage_hash) / "contextual_translation.json"
    if not cache_path.exists():
        return None
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["cache_path"] = str(cache_path)
    return payload


def restore_cached_contextual_translation(
    workspace,
    *,
    database: ProjectDatabase,
    payload: dict[str, object],
    target_language: str,
) -> Path:
    scenes = _restore_record_list(payload.get("scenes", []), SceneMemoryRecord)
    character_profiles = _restore_record_list(payload.get("character_profiles", []), CharacterProfileRecord)
    relationship_profiles = _restore_record_list(payload.get("relationship_profiles", []), RelationshipProfileRecord)
    analyses = _restore_record_list(payload.get("segment_analyses", []), SegmentAnalysisRecord)
    database.replace_contextual_translation_state(
        workspace.project_id,
        scenes=scenes,
        analyses=analyses,
        character_profiles=character_profiles,
        relationship_profiles=relationship_profiles,
    )
    database.apply_segment_analysis_outputs(
        workspace.project_id,
        target_language=target_language,
    )
    return Path(str(payload["cache_path"]))


def recompute_semantic_qc(
    database: ProjectDatabase,
    *,
    project_id: str,
    target_language: str,
) -> dict[str, object]:
    analysis_rows = database.list_segment_analyses(project_id)
    relationship_defaults = _relationship_defaults_map(database, project_id)
    normalized_rows: list[dict[str, object]] = []
    for row in analysis_rows:
        normalized_rows.append(
            {
                "segment_id": str(row["segment_id"]),
                "segment_index": int(row["segment_index"]),
                "scene_id": str(row["scene_id"]),
                "speaker_json": json.loads(row["speaker_json"] or "{}"),
                "listeners_json": json.loads(row["listeners_json"] or "[]"),
                "honorific_policy_json": json.loads(row["honorific_policy_json"] or "{}"),
                "confidence_json": json.loads(row["confidence_json"] or "{}"),
                "resolved_ellipsis_json": json.loads(row["resolved_ellipsis_json"] or "{}"),
                "risk_flags_json": json.loads(row["risk_flags_json"] or "[]"),
                "approved_subtitle_text": row["approved_subtitle_text"] or "",
                "approved_tts_text": row["approved_tts_text"] or "",
            }
        )
    report = analyze_segment_analyses(normalized_rows, relationship_defaults=relationship_defaults)
    issues_by_segment: dict[str, list[dict[str, object]]] = {}
    for issue in report.issues:
        issues_by_segment.setdefault(issue.segment_id, []).append(
            {"code": issue.code, "severity": issue.severity, "message": issue.message}
        )
    for row in analysis_rows:
        segment_id = str(row["segment_id"])
        issues = issues_by_segment.get(segment_id, [])
        has_error = any(item["severity"] == "error" for item in issues)
        reason_codes = list(json.loads(row["review_reason_codes_json"] or "[]"))
        for item in issues:
            if item["code"] not in reason_codes:
                reason_codes.append(item["code"])
        needs_review = bool(row["needs_human_review"]) or bool(issues)
        database.update_segment_analysis_review(
            project_id,
            segment_id,
            needs_human_review=needs_review,
            review_status="needs_review" if needs_review else "approved",
            review_reason_codes_json=reason_codes,
            semantic_qc_passed=not has_error,
            semantic_qc_issues_json=issues,
        )
    database.apply_segment_analysis_outputs(project_id, target_language=target_language)
    return {
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "total_segments": report.total_segments,
    }


def run_contextual_translation(
    context: JobContext,
    *,
    workspace,
    database: ProjectDatabase,
    engine: OpenAITranslationEngine,
    segments: list[Row],
    selected_template: TranslationPromptTemplate,
    source_language: str,
    target_language: str,
    model: str,
) -> dict[str, object]:
    prompt_family = resolve_prompt_family(workspace.root_dir, selected_template)
    required_roles = {"scene_planner", "semantic_pass", "dialogue_adaptation"}
    missing_roles = sorted(required_roles - set(prompt_family))
    if missing_roles:
        raise RuntimeError(f"Thiếu contextual prompt roles: {', '.join(missing_roles)}")

    scenes = chunk_segments_into_scenes(segments)
    now = _utc_now_iso()
    scene_records: list[SceneMemoryRecord] = []
    character_profiles: dict[str, CharacterProfileRecord] = {
        str(row["character_id"]): CharacterProfileRecord(
            character_id=str(row["character_id"]),
            project_id=workspace.project_id,
            canonical_name_zh=row["canonical_name_zh"] or "",
            canonical_name_vi=row["canonical_name_vi"] or "",
            aliases_json=json.loads(row["aliases_json"] or "[]"),
            gender_hint=row["gender_hint"],
            age_role=row["age_role"],
            social_role=row["social_role"],
            speech_style=row["speech_style"],
            default_register_profile_json=json.loads(row["default_register_profile_json"] or "{}"),
            default_self_terms_json=json.loads(row["default_self_terms_json"] or "[]"),
            default_address_terms_json=json.loads(row["default_address_terms_json"] or "[]"),
            forbidden_terms_json=json.loads(row["forbidden_terms_json"] or "[]"),
            evidence_segment_ids_json=json.loads(row["evidence_segment_ids_json"] or "[]"),
            confidence=float(row["confidence"] or 0.0),
            status=row["status"] or "hypothesized",
            notes=row["notes"] or "",
            created_at=row["created_at"] or now,
            updated_at=now,
        )
        for row in database.list_character_profiles(workspace.project_id)
    }
    relationship_profiles: dict[str, RelationshipProfileRecord] = {
        str(row["relationship_id"]): RelationshipProfileRecord(
            relationship_id=str(row["relationship_id"]),
            project_id=workspace.project_id,
            from_character_id=str(row["from_character_id"]),
            to_character_id=str(row["to_character_id"]),
            relation_type=row["relation_type"] or "unknown",
            power_delta=row["power_delta"],
            age_delta=row["age_delta"],
            intimacy_level=row["intimacy_level"],
            default_self_term=row["default_self_term"],
            default_address_term=row["default_address_term"],
            allowed_alternates_json=clone_allowed_alternates(json.loads(row["allowed_alternates_json"] or "[]")),
            scope=row["scope"] or "scene",
            status=row["status"] or "hypothesized",
            evidence_segment_ids_json=json.loads(row["evidence_segment_ids_json"] or "[]"),
            last_updated_scene_id=row["last_updated_scene_id"],
            notes=row["notes"] or "",
            created_at=row["created_at"] or now,
            updated_at=now,
        )
        for row in database.list_relationship_profiles(workspace.project_id)
    }
    analyses: list[SegmentAnalysisRecord] = []

    total_scenes = max(1, len(scenes))
    for scene_position, scene in enumerate(scenes, start=1):
        context.report_progress(
            min(20, int(scene_position * 20 / total_scenes)),
            f"Contextual V2: scene {scene_position}/{total_scenes}",
        )
        planner_output = engine.plan_scene(
            context,
            template=prompt_family["scene_planner"],
            scene_payload=_scene_row_payload(scene),
            source_language=source_language,
            target_language=target_language,
            context_payload=_build_context_payload(
                scene=scene,
                existing_character_rows=list(character_profiles.values()),
                existing_relationship_rows=list(relationship_profiles.values()),
            ),
            glossary_payload=_build_glossary_payload(
                database,
                workspace.project_id,
                relationship_rows=list(relationship_profiles.values()),
            ),
            model=model,
        )
        scene_records.append(_scene_record_from_output(workspace.project_id, scene, planner_output, now=now))
        for seed in planner_output.character_updates:
            character_profiles[seed.character_id] = _character_record_from_seed(
                workspace.project_id,
                seed,
                now=now,
            )
        for seed in planner_output.relationship_updates:
            relationship_profiles[seed.relationship_id] = _relationship_record_from_seed(
                workspace.project_id,
                seed,
                now=now,
                scene_id=scene.scene_id,
            )

        scene_batches = _chunk_scene_segments(scene.segments)
        semantic_items: dict[str, object] = {}
        for batch_index, batch_rows in enumerate(scene_batches, start=1):
            semantic_output = engine.analyze_semantics(
                context,
            template=prompt_family["semantic_pass"],
            batch_payload={
                "scene": _scene_row_payload(scene),
                "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
            },
            source_language=source_language,
            target_language=target_language,
            context_payload=_build_context_payload(
                scene=scene,
                planner_summary=planner_output.scene_summary,
                existing_character_rows=list(character_profiles.values()),
                existing_relationship_rows=list(relationship_profiles.values()),
            ),
            glossary_payload=_build_glossary_payload(
                database,
                workspace.project_id,
                relationship_rows=list(relationship_profiles.values()),
            ),
            model=model,
        )
        semantic_items = {item.segment_id: item for item in semantic_output.items}
        if set(semantic_items) != set(scene.segment_ids):
            raise RuntimeError("Semantic pass không trả về đủ segment ids của scene")

        adaptation_output = engine.adapt_dialogue(
            context,
            template=prompt_family["dialogue_adaptation"],
            batch_payload={
                "scene": _scene_row_payload(scene),
                "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
                "semantic_items": semantic_output.model_dump(mode="json", by_alias=True)["items"],
            },
            source_language=source_language,
            target_language=target_language,
            context_payload=_build_context_payload(
                scene=scene,
                planner_summary=planner_output.scene_summary,
                existing_character_rows=list(character_profiles.values()),
                existing_relationship_rows=list(relationship_profiles.values()),
            ),
            glossary_payload=_build_glossary_payload(
                database,
                workspace.project_id,
                relationship_rows=list(relationship_profiles.values()),
            ),
            model=model,
        )
        adaptation_items = {item.segment_id: item for item in adaptation_output.items}
        if set(adaptation_items) != set(scene.segment_ids):
            raise RuntimeError("Dialogue adaptation không trả về đủ segment ids của scene")

        critic_items: dict[str, object] = {}
        if "semantic_critic" in prompt_family:
            critic_output = engine.critique_dialogue(
                context,
                template=prompt_family["semantic_critic"],
                batch_payload={
                    "scene": _scene_row_payload(scene),
                    "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
                    "semantic_items": semantic_output.model_dump(mode="json", by_alias=True)["items"],
                    "adaptation_items": adaptation_output.model_dump(mode="json", by_alias=True)["items"],
                },
                source_language=source_language,
                target_language=target_language,
                context_payload=_build_context_payload(
                    scene=scene,
                    planner_summary=planner_output.scene_summary,
                    existing_character_rows=list(character_profiles.values()),
                    existing_relationship_rows=list(relationship_profiles.values()),
                ),
                glossary_payload=_build_glossary_payload(
                    database,
                    workspace.project_id,
                    relationship_rows=list(relationship_profiles.values()),
                ),
                model=model,
            )
            critic_items = {item.segment_id: item for item in critic_output.items}

        for row in scene.segments:
            segment_id = str(row["segment_id"])
            semantic_item = semantic_items[segment_id]
            adaptation_item = adaptation_items[segment_id]
            critic_item = critic_items.get(segment_id)
            critic_issues = []
            review_reason_codes = list(semantic_item.review_reason_codes)
            for code in adaptation_item.review_reason_codes:
                if code not in review_reason_codes:
                    review_reason_codes.append(code)
            if critic_item:
                for code in critic_item.error_codes:
                    if code not in review_reason_codes:
                        review_reason_codes.append(code)
                critic_issues = [item.model_dump(mode="json") for item in critic_item.issues]
            needs_review = (
                semantic_item.needs_human_review
                or adaptation_item.needs_human_review
                or bool(getattr(critic_item, "review_needed", False))
            )
            analyses.append(
                SegmentAnalysisRecord(
                    segment_id=segment_id,
                    project_id=workspace.project_id,
                    scene_id=scene.scene_id,
                    segment_index=int(row["segment_index"]),
                    speaker_json=semantic_item.speaker.model_dump(mode="json", by_alias=True),
                    listeners_json=[item.model_dump(mode="json", by_alias=True) for item in semantic_item.listeners],
                    register_json=semantic_item.register_data.model_dump(mode="json", by_alias=True),
                    turn_function=semantic_item.turn_function,
                    resolved_ellipsis_json=semantic_item.resolved_ellipsis.model_dump(mode="json", by_alias=True),
                    honorific_policy_json=adaptation_item.honorific_policy.model_dump(mode="json", by_alias=True),
                    semantic_translation=semantic_item.semantic_translation,
                    glossary_hits_json=list(semantic_item.glossary_hits),
                    risk_flags_json=list(
                        dict.fromkeys(list(semantic_item.risk_flags) + list(adaptation_item.risk_flags))
                    ),
                    confidence_json=semantic_item.confidence.model_dump(mode="json", by_alias=True),
                    needs_human_review=needs_review,
                    review_status="needs_review" if needs_review else "approved",
                    review_reason_codes_json=review_reason_codes,
                    review_question=semantic_item.review_question,
                    approved_subtitle_text=adaptation_item.subtitle_text.strip(),
                    approved_tts_text=adaptation_item.tts_text.strip(),
                    semantic_qc_passed=not critic_issues,
                    semantic_qc_issues_json=critic_issues,
                    source_template_family_id=selected_template.family_id or selected_template.template_id,
                    adaptation_template_family_id=prompt_family["dialogue_adaptation"].family_id
                    or prompt_family["dialogue_adaptation"].template_id,
                    created_at=now,
                    updated_at=now,
                )
            )

    relationship_defaults = _relationship_defaults_map(database, workspace.project_id)
    relationship_defaults.update(
        {
            (item.from_character_id, item.to_character_id): {
                "default_self_term": item.default_self_term,
                "default_address_term": item.default_address_term,
                "allowed_alternates_json": clone_allowed_alternates(item.allowed_alternates_json),
            }
            for item in relationship_profiles.values()
        }
    )
    qc_report = analyze_segment_analyses(
        [
            {
                "segment_id": item.segment_id,
                "segment_index": item.segment_index,
                "scene_id": item.scene_id,
                "speaker_json": item.speaker_json,
                "listeners_json": item.listeners_json,
                "honorific_policy_json": item.honorific_policy_json,
                "confidence_json": item.confidence_json,
                "resolved_ellipsis_json": item.resolved_ellipsis_json,
                "risk_flags_json": item.risk_flags_json,
                "approved_subtitle_text": item.approved_subtitle_text,
                "approved_tts_text": item.approved_tts_text,
            }
            for item in analyses
        ],
        relationship_defaults=relationship_defaults,
    )
    issues_by_segment: dict[str, list[dict[str, object]]] = {}
    for issue in qc_report.issues:
        issues_by_segment.setdefault(issue.segment_id, []).append(
            {"code": issue.code, "severity": issue.severity, "message": issue.message}
        )
    review_codes = {
        "honorific_drift",
        "directionality_mismatch",
        "pronoun_without_evidence",
        "addressee_mismatch",
        "sub_tts_pronoun_divergence",
        "low_confidence_gate",
    }
    patched_analyses: list[SegmentAnalysisRecord] = []
    for analysis in analyses:
        segment_issues = list(analysis.semantic_qc_issues_json) + issues_by_segment.get(analysis.segment_id, [])
        unique_issue_keys: set[str] = set()
        deduped_issues: list[dict[str, object]] = []
        for item in segment_issues:
            issue_key = json.dumps(item, ensure_ascii=False, sort_keys=True)
            if issue_key in unique_issue_keys:
                continue
            unique_issue_keys.add(issue_key)
            deduped_issues.append(item)
        has_error = any(item["severity"] == "error" for item in deduped_issues)
        reason_codes = list(analysis.review_reason_codes_json)
        for item in deduped_issues:
            if item["code"] not in reason_codes:
                reason_codes.append(item["code"])
        needs_review = analysis.needs_human_review or any(item["code"] in review_codes for item in deduped_issues)
        patched_analyses.append(
            SegmentAnalysisRecord(
                **{
                    **asdict(analysis),
                    "needs_human_review": needs_review,
                    "review_status": "needs_review" if needs_review else "approved",
                    "review_reason_codes_json": reason_codes,
                    "semantic_qc_passed": not has_error,
                    "semantic_qc_issues_json": deduped_issues,
                }
            )
        )

    return {
        "scenes": scene_records,
        "character_profiles": list(character_profiles.values()),
        "relationship_profiles": list(relationship_profiles.values()),
        "segment_analyses": patched_analyses,
        "semantic_qc": {
            "error_count": qc_report.error_count,
            "warning_count": qc_report.warning_count,
            "total_segments": qc_report.total_segments,
        },
    }
