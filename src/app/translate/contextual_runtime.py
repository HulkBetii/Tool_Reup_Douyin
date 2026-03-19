from __future__ import annotations

import json
import re
from dataclasses import asdict
from sqlite3 import Row
from typing import Any, Callable

from app.core.jobs import JobContext
from app.project.database import ProjectDatabase
from app.project.models import CharacterProfileRecord, RelationshipProfileRecord, SceneMemoryRecord, SegmentAnalysisRecord

from .contextual_pipeline import (
    _build_context_payload,
    _build_glossary_payload,
    _character_record_from_seed,
    _chunk_scene_segments,
    _relationship_defaults_map,
    _relationship_record_from_seed,
    _scene_record_from_output,
    _scene_row_payload,
    _scene_batch_payload,
    _utc_now_iso,
    _validate_stage_item_ids,
)
from .models import TranslationPromptTemplate
from .openai_engine import OpenAITranslationEngine
from .presets import resolve_prompt_family
from .scene_chunker import chunk_segments_into_scenes
from .semantic_qc import analyze_segment_analyses


def _normalize_review_reason_code(raw_code: str) -> str:
    text = raw_code.strip()
    if not text:
        return "unspecified_review_reason"
    lowered = text.lower()
    if "speaker" in lowered and ("uncertain" in lowered or "ambiguous" in lowered):
        return "uncertain_speaker"
    if ("listener" in lowered or "addressee" in lowered) and (
        "uncertain" in lowered or "ambiguous" in lowered
    ):
        return "uncertain_listener"
    if "damage" in lowered:
        return "ambiguous_damage_description"
    if "object" in lowered:
        return "ambiguous_object_reference"
    if "reference" in lowered:
        return "ambiguous_reference"
    if "tone" in lowered:
        return "tone_ambiguity"
    if "term" in lowered or "meaning" in lowered or "ambiguousterm" in lowered:
        return "ambiguous_term"
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized or "unspecified_review_reason"


def _run_stage_batch_with_retry(
    *,
    context: JobContext,
    stage_label: str,
    scene_id: str,
    batch_rows: list[Row],
    batch_index: int,
    batch_count: int,
    stage_runner: Callable[[list[Row], int, int], Any],
) -> dict[str, Any]:
    context.cancellation_token.raise_if_canceled()
    output = stage_runner(batch_rows, batch_index, batch_count)
    batch_items = {item.segment_id: item for item in output.items}
    try:
        _validate_stage_item_ids(
            stage_label,
            expected_ids=[str(row["segment_id"]) for row in batch_rows],
            actual_ids=list(batch_items),
            scene_id=scene_id,
            batch_index=batch_index,
            batch_count=batch_count,
        )
        return batch_items
    except RuntimeError:
        if len(batch_rows) <= 1:
            raise
        midpoint = max(1, len(batch_rows) // 2)
        left_rows = batch_rows[:midpoint]
        right_rows = batch_rows[midpoint:]
        context.logger.warning(
            "%s returned mismatched ids for scene=%s batch=%s/%s. Retrying with smaller batches (%s + %s).",
            stage_label,
            scene_id,
            batch_index,
            batch_count,
            len(left_rows),
            len(right_rows),
        )
        merged_items = _run_stage_batch_with_retry(
            context=context,
            stage_label=stage_label,
            scene_id=scene_id,
            batch_rows=left_rows,
            batch_index=batch_index,
            batch_count=batch_count,
            stage_runner=stage_runner,
        )
        if right_rows:
            merged_items.update(
                _run_stage_batch_with_retry(
                    context=context,
                    stage_label=stage_label,
                    scene_id=scene_id,
                    batch_rows=right_rows,
                    batch_index=batch_index,
                    batch_count=batch_count,
                    stage_runner=stage_runner,
                )
            )
        return merged_items


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
            allowed_alternates_json=json.loads(row["allowed_alternates_json"] or "[]"),
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
            semantic_items.update(
                _run_stage_batch_with_retry(
                    context=context,
                    stage_label="Semantic pass",
                    scene_id=scene.scene_id,
                    batch_rows=batch_rows,
                    batch_index=batch_index,
                    batch_count=len(scene_batches),
                    stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.analyze_semantics(
                        context,
                        template=prompt_family["semantic_pass"],
                        batch_payload={
                            **_scene_batch_payload(
                                scene,
                                current_rows,
                                batch_index=current_batch_index,
                                batch_count=current_batch_count,
                            ),
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
                    ),
                )
            )
        _validate_stage_item_ids(
            "Semantic pass",
            expected_ids=list(scene.segment_ids),
            actual_ids=list(semantic_items),
            scene_id=scene.scene_id,
            batch_index=len(scene_batches),
            batch_count=len(scene_batches),
        )

        adaptation_items: dict[str, object] = {}
        for batch_index, batch_rows in enumerate(scene_batches, start=1):
            adaptation_items.update(
                _run_stage_batch_with_retry(
                    context=context,
                    stage_label="Dialogue adaptation",
                    scene_id=scene.scene_id,
                    batch_rows=batch_rows,
                    batch_index=batch_index,
                    batch_count=len(scene_batches),
                    stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.adapt_dialogue(
                        context,
                        template=prompt_family["dialogue_adaptation"],
                        batch_payload={
                            **_scene_batch_payload(
                                scene,
                                current_rows,
                                batch_index=current_batch_index,
                                batch_count=current_batch_count,
                            ),
                            "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
                            "semantic_items": [
                                semantic_items[str(row["segment_id"])].model_dump(mode="json", by_alias=True)
                                for row in current_rows
                            ],
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
                    ),
                )
            )
        _validate_stage_item_ids(
            "Dialogue adaptation",
            expected_ids=list(scene.segment_ids),
            actual_ids=list(adaptation_items),
            scene_id=scene.scene_id,
            batch_index=len(scene_batches),
            batch_count=len(scene_batches),
        )

        critic_items: dict[str, object] = {}
        if "semantic_critic" in prompt_family:
            for batch_index, batch_rows in enumerate(scene_batches, start=1):
                critic_items.update(
                    _run_stage_batch_with_retry(
                        context=context,
                        stage_label="Semantic critic",
                        scene_id=scene.scene_id,
                        batch_rows=batch_rows,
                        batch_index=batch_index,
                        batch_count=len(scene_batches),
                        stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.critique_dialogue(
                            context,
                            template=prompt_family["semantic_critic"],
                            batch_payload={
                                **_scene_batch_payload(
                                    scene,
                                    current_rows,
                                    batch_index=current_batch_index,
                                    batch_count=current_batch_count,
                                ),
                                "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
                                "semantic_items": [
                                    semantic_items[str(row["segment_id"])].model_dump(mode="json", by_alias=True)
                                    for row in current_rows
                                ],
                                "adaptation_items": [
                                    adaptation_items[str(row["segment_id"])].model_dump(mode="json", by_alias=True)
                                    for row in current_rows
                                ],
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
                        ),
                    )
                )
            _validate_stage_item_ids(
                "Semantic critic",
                expected_ids=list(scene.segment_ids),
                actual_ids=list(critic_items),
                scene_id=scene.scene_id,
                batch_index=len(scene_batches),
                batch_count=len(scene_batches),
            )

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
                critic_issues = [item.model_dump(mode="json", by_alias=True) for item in critic_item.issues]
            review_reason_codes = [
                _normalize_review_reason_code(code)
                for code in review_reason_codes
            ]
            review_reason_codes = list(dict.fromkeys(review_reason_codes))
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
                "allowed_alternates_json": item.allowed_alternates_json,
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
            normalized_issue_code = _normalize_review_reason_code(str(item["code"]))
            if normalized_issue_code not in reason_codes:
                reason_codes.append(normalized_issue_code)
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
