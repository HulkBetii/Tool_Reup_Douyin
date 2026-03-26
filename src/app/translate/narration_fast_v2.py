from __future__ import annotations

import json
import math
import re
from dataclasses import asdict
from pathlib import Path
from sqlite3 import Row
from typing import Any, Callable

from app.core.jobs import JobContext
from app.project.database import ProjectDatabase
from app.project.models import (
    CharacterProfileRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
)
from app.project.profiles import load_project_profile_state
from app.translate.relationship_memory import clone_allowed_alternates

from .contextual_checkpoint import ContextualTranslationCheckpointState
from .contextual_pipeline import (
    CONTEXTUAL_STAGE_BATCH_SIZE,
    _build_context_payload,
    _build_glossary_payload,
    _character_record_from_seed,
    _chunk_scene_segments,
    _relationship_defaults_map,
    _relationship_record_from_seed,
    _scene_batch_payload,
    _scene_record_from_output,
    _scene_row_payload,
    _utc_now_iso,
    _validate_stage_item_ids,
)
from .contextual_runtime import (
    NARRATION_ROUTE_HIGH_CONFIDENCE,
    _fallback_term_anchor_segment_ids,
    _normalize_review_reason_code,
    _route_scene,
    _run_stage_batch_with_positional_retry,
    _run_stage_batch_with_retry,
)
from .models import (
    ApprovedTermMemoryEntry,
    ContextualRunMetrics,
    DialogueAdaptationBatchOutput,
    NarrationBudgetPolicy,
    NarrationSpan,
    SceneRouteDecision,
    SemanticBatchOutput,
    TranslationPromptTemplate,
)
from .openai_engine import OpenAITranslationEngine
from .presets import load_prompt_template, resolve_prompt_family
from .scene_chunker import SceneChunk, chunk_segments_into_scenes
from .semantic_qc import analyze_segment_analyses

NARRATION_FAST_V2_ROUTE = "narration_fast_v2"
DIALOGUE_LEGACY_ROUTE = "dialogue_legacy"
NARRATION_FAST_V2_ROUTER_VERSION = "narration_fast_v2"
NARRATION_FAST_V2_POSTPROCESS_VERSION = "narration_fast_v2"
NARRATION_SPAN_MAX_GAP_MS = 1500
NARRATION_SPAN_MAX_RENDER_UNITS = 48
NARRATION_SPAN_MAX_SOURCE_CHARS = 3200
NARRATION_SPAN_MAX_DURATION_MS = 90_000
TARGET_CPS = 16.0
SLOT_PRESSURE_THRESHOLD = 1.10
_DIRECT_SPEECH_MARKERS = ("“", "”", "\"", "'", "『", "』", "「", "」", "：", ":")
_HARD_ENTITY_FLAGS = {"entity_conflict", "number_sensitive", "numeric_mismatch"}
_AMBIGUITY_FLAGS = {
    "pronoun_ambiguous",
    "idiom_ambiguous",
    "cultural_reference",
    "ellipsis_ambiguous",
    "title_ambiguous",
    "unsafe_to_guess",
}
_SCIENTIFIC_NOTATION_CUE_RE = re.compile(r"10\s*(?:\^|mũ|mu|次方|的)", re.IGNORECASE)
_INCOMPLETE_SCIENTIFIC_NOTATION_RE = re.compile(r"10\s*(?:\^|mũ)\s*(?=[\.\,\!\?\;\:]|$)", re.IGNORECASE)
_ARABIC_EXPONENT_RE = re.compile(r"(?<!\d)(\d{1,3})(?!\d)")
_CHINESE_NUMERAL_RE = re.compile(r"[零〇一二两三四五六七八九十百千]+")


def _global_term_memory_path() -> Path:
    return Path(__file__).with_name("narration_term_memory_defaults.json")


def _project_term_memory_path(project_root: Path) -> Path:
    return project_root / ".ops" / "narration_term_memory.json"


def _normalize_term_key(value: str) -> str:
    return "".join(str(value or "").strip().casefold().split())


def _load_term_memory_entries(path: Path) -> list[ApprovedTermMemoryEntry]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    raw_items = payload.get("items", payload)
    if not isinstance(raw_items, list):
        return []
    entries: list[ApprovedTermMemoryEntry] = []
    for item in raw_items:
        try:
            entries.append(ApprovedTermMemoryEntry.model_validate(item))
        except Exception:
            continue
    return entries


def _save_project_term_memory(project_root: Path, entries: list[ApprovedTermMemoryEntry]) -> None:
    path = _project_term_memory_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"items": [entry.model_dump(mode="json") for entry in entries]}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _build_memory_lookup(entries: list[ApprovedTermMemoryEntry]) -> dict[str, ApprovedTermMemoryEntry]:
    lookup: dict[str, ApprovedTermMemoryEntry] = {}
    for entry in entries:
        for raw_key in (entry.normalized_form, entry.source_surface):
            key = _normalize_term_key(raw_key)
            if key and key not in lookup:
                lookup[key] = entry
    return lookup


def _merge_term_memory(
    *,
    run_local: list[ApprovedTermMemoryEntry],
    project_local: list[ApprovedTermMemoryEntry],
    global_defaults: list[ApprovedTermMemoryEntry],
) -> dict[str, ApprovedTermMemoryEntry]:
    merged: dict[str, ApprovedTermMemoryEntry] = {}
    for source in (global_defaults, project_local, run_local):
        merged.update(_build_memory_lookup(source))
    return merged


def _base_narration_speaker() -> dict[str, object]:
    return {"character_id": "narrator", "source": "narration", "confidence": 0.99}


def _base_narration_listener() -> list[dict[str, object]]:
    return [{"character_id": "audience", "role": "audience", "confidence": 0.95}]


def _base_narration_register() -> dict[str, object]:
    return {
        "politeness": "neutral",
        "power_direction": "neutral",
        "emotional_tone": "informative",
        "confidence": 0.95,
    }


def _base_narration_confidence() -> dict[str, object]:
    return {
        "overall": 0.92,
        "speaker": 0.99,
        "listener": 0.95,
        "register": 0.95,
        "relation": 0.0,
        "translation": 0.9,
    }


def _normalize_canonical_text(text: str) -> str:
    normalized = " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()
    if not normalized:
        return ""
    if normalized.endswith((".", "!", "?")):
        return normalized
    return normalized + "."


def _contains_direct_speech(scene: SceneChunk) -> bool:
    for row in scene.segments:
        text = str(row["source_text"] or "").strip()
        if text and any(marker in text for marker in _DIRECT_SPEECH_MARKERS):
            return True
    return False


def _route_scene_v2(
    scene: SceneChunk,
    *,
    prior_analysis_rows: dict[str, dict[str, object]],
    narration_family_id: str,
    dialogue_family_id: str,
    prefer_narration: bool,
) -> SceneRouteDecision:
    decision = _route_scene(
        scene,
        prior_analysis_rows=prior_analysis_rows,
        narration_family_id=narration_family_id,
        dialogue_family_id=dialogue_family_id,
        prefer_narration=prefer_narration,
    )
    if (
        decision.route_mode == "narration_fast"
        and (
            _contains_direct_speech(scene)
            or decision.prior_review_penalty > 0.34
            or decision.narration_score < NARRATION_ROUTE_HIGH_CONFIDENCE
        )
    ):
        return decision.model_copy(
            update={
                "route_mode": DIALOGUE_LEGACY_ROUTE,
                "prompt_family_id": dialogue_family_id,
                "fallback_reason": decision.fallback_reason or "mixed_dialogue_cues",
            }
        )
    return decision.model_copy(
        update={
            "route_mode": (
                NARRATION_FAST_V2_ROUTE
                if decision.route_mode == "narration_fast"
                else DIALOGUE_LEGACY_ROUTE
            ),
            "prompt_family_id": (
                narration_family_id
                if decision.route_mode == "narration_fast"
                else dialogue_family_id
            ),
        }
    )


def _build_narration_spans(
    scenes: list[SceneChunk],
    route_decisions: list[SceneRouteDecision],
) -> tuple[list[NarrationSpan], dict[str, NarrationSpan]]:
    spans: list[NarrationSpan] = []
    span_by_scene_id: dict[str, NarrationSpan] = {}
    current_scenes: list[SceneChunk] = []

    def flush() -> None:
        nonlocal current_scenes
        if not current_scenes:
            return
        span_index = len(spans)
        span = NarrationSpan(
            span_id=f"span_{span_index:04d}",
            span_index=span_index,
            scene_ids=[scene.scene_id for scene in current_scenes],
            scene_indexes=[scene.scene_index for scene in current_scenes],
            segment_ids=[segment_id for scene in current_scenes for segment_id in scene.segment_ids],
            start_ms=current_scenes[0].start_ms,
            end_ms=current_scenes[-1].end_ms,
            total_source_chars=sum(
                len(str(row["source_text"] or "").strip())
                for scene in current_scenes
                for row in scene.segments
            ),
            render_unit_count=sum(len(scene.segments) for scene in current_scenes),
        )
        spans.append(span)
        for scene_id in span.scene_ids:
            span_by_scene_id[scene_id] = span
        current_scenes = []

    for scene, decision in zip(scenes, route_decisions):
        if decision.route_mode != NARRATION_FAST_V2_ROUTE:
            flush()
            continue
        if not current_scenes:
            current_scenes = [scene]
            continue
        previous_scene = current_scenes[-1]
        proposed_render_units = sum(len(item.segments) for item in current_scenes) + len(scene.segments)
        proposed_source_chars = sum(
            len(str(row["source_text"] or "").strip())
            for item in current_scenes
            for row in item.segments
        ) + sum(len(str(row["source_text"] or "").strip()) for row in scene.segments)
        proposed_duration_ms = max(0, scene.end_ms - current_scenes[0].start_ms)
        should_break = (
            scene.start_ms - previous_scene.end_ms > NARRATION_SPAN_MAX_GAP_MS
            or proposed_render_units > NARRATION_SPAN_MAX_RENDER_UNITS
            or proposed_source_chars > NARRATION_SPAN_MAX_SOURCE_CHARS
            or proposed_duration_ms > NARRATION_SPAN_MAX_DURATION_MS
            or _contains_direct_speech(scene)
            or decision.prior_review_penalty > 0.34
            or decision.fallback_reason == "borderline_narration_score"
        )
        if should_break:
            flush()
            current_scenes = [scene]
            continue
        current_scenes.append(scene)
    flush()
    return spans, span_by_scene_id


def _span_context_payload(
    *,
    span: NarrationSpan,
    scenes: list[SceneChunk],
) -> dict[str, object]:
    recent_turns = []
    for scene in scenes:
        for row in scene.segments[:6]:
            recent_turns.append(
                {
                    "segment_id": str(row["segment_id"]),
                    "segment_index": int(row["segment_index"]),
                    "source_text": row["source_text"],
                }
            )
        if len(recent_turns) >= 6:
            break
    return {
        "span_id": span.span_id,
        "scene_ids": list(span.scene_ids),
        "recent_turns": recent_turns[:6],
        "scene_summary": f"Narration span {span.span_index} with {span.render_unit_count} render units.",
        "character_profiles": [],
        "relationship_profiles": [],
    }


def _span_payload(span: NarrationSpan, scenes: list[SceneChunk]) -> dict[str, object]:
    render_units: list[dict[str, object]] = []
    for scene in scenes:
        for row in scene.segments:
            render_units.append(
                {
                    "segment_id": str(row["segment_id"]),
                    "scene_id": scene.scene_id,
                    "segment_index": int(row["segment_index"]),
                    "start_ms": int(row["start_ms"]),
                    "end_ms": int(row["end_ms"]),
                    "duration_ms": max(0, int(row["end_ms"]) - int(row["start_ms"])),
                    "source_text": row["source_text"],
                }
            )
    return {
        "span": {
            "span_id": span.span_id,
            "span_index": span.span_index,
            "scene_ids": list(span.scene_ids),
            "start_ms": span.start_ms,
            "end_ms": span.end_ms,
            "duration_ms": max(0, span.end_ms - span.start_ms),
            "render_unit_count": span.render_unit_count,
            "render_units": render_units,
        }
    }


def _estimated_slot_pressure(text: str, duration_ms: int) -> float:
    seconds = max(0.1, duration_ms / 1000.0)
    cps = max(0.0, len(text.strip()) / seconds)
    return cps / TARGET_CPS


def _parse_simple_chinese_integer(token: str) -> int | None:
    token = str(token or "").strip()
    if not token:
        return None
    digit_map = {
        "零": 0,
        "〇": 0,
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    unit_map = {"十": 10, "百": 100, "千": 1000}
    total = 0
    current = 0
    for char in token:
        if char in digit_map:
            current = digit_map[char]
            continue
        if char in unit_map:
            total += max(1, current) * unit_map[char]
            current = 0
            continue
        return None
    return total + current


def _extract_exponent_hints(*texts: str) -> set[str]:
    hints: set[str] = set()
    for raw_text in texts:
        text = str(raw_text or "").strip()
        if not text:
            continue
        for match in _ARABIC_EXPONENT_RE.findall(text):
            value = int(match)
            if 0 < value <= 200:
                hints.add(str(value))
        if len(text) > 24:
            continue
        for token in _CHINESE_NUMERAL_RE.findall(text):
            value = _parse_simple_chinese_integer(token)
            if value is not None and 0 < value <= 200:
                hints.add(str(value))
    return hints


def _has_incomplete_scientific_notation(text: str) -> bool:
    return bool(_INCOMPLETE_SCIENTIFIC_NOTATION_RE.search(str(text or "")))


def _repair_incomplete_scientific_notation(text: str, *, exponent: str) -> str:
    repaired = _INCOMPLETE_SCIENTIFIC_NOTATION_RE.sub(f"10 mũ {exponent}", str(text or ""))
    return _normalize_canonical_text(repaired)


def _apply_scientific_notation_autofix(
    *,
    render_rows: list[dict[str, object]],
    canonical_by_segment: dict[str, dict[str, object]],
) -> list[str]:
    ordered_rows = list(sorted(render_rows, key=lambda row: int(row["segment_index"])))
    position_by_segment_id = {str(row["segment_id"]): index for index, row in enumerate(ordered_rows)}
    changed_segment_ids: list[str] = []
    for row in ordered_rows:
        segment_id = str(row["segment_id"])
        payload = canonical_by_segment.get(segment_id)
        if payload is None:
            continue
        canonical_text = str(payload.get("canonical_text") or "")
        source_text = str(payload.get("source_text") or row["source_text"] or "")
        if not _has_incomplete_scientific_notation(canonical_text):
            continue
        if not _SCIENTIFIC_NOTATION_CUE_RE.search(source_text) and not _SCIENTIFIC_NOTATION_CUE_RE.search(canonical_text):
            continue
        row_position = position_by_segment_id[segment_id]
        hint_values: set[str] = set()
        for neighbor_position in range(max(0, row_position - 2), min(len(ordered_rows), row_position + 3)):
            if neighbor_position == row_position:
                continue
            neighbor_row = ordered_rows[neighbor_position]
            neighbor_payload = canonical_by_segment.get(str(neighbor_row["segment_id"]), {})
            hint_values.update(
                _extract_exponent_hints(
                    str(neighbor_row["source_text"] or ""),
                    str(neighbor_payload.get("canonical_text") or ""),
                )
            )
        if len(hint_values) != 1:
            continue
        exponent = next(iter(hint_values))
        repaired_text = _repair_incomplete_scientific_notation(canonical_text, exponent=exponent)
        if not repaired_text or _has_incomplete_scientific_notation(repaired_text):
            continue
        payload["canonical_text"] = repaired_text
        payload["unsafe_to_guess"] = False
        payload["risk_flags"] = [
            flag
            for flag in list(payload.get("risk_flags", []))
            if str(flag) not in {"unsafe_to_guess", "ambiguous_term"}
        ]
        payload["slot_pressure"] = _estimated_slot_pressure(
            repaired_text,
            max(0, int(row["end_ms"]) - int(row["start_ms"])),
        )
        changed_segment_ids.append(segment_id)
    return changed_segment_ids


def _budget_allows_soft_escalation(metrics: ContextualRunMetrics, policy: NarrationBudgetPolicy) -> bool:
    if metrics.estimated_cost_usd >= policy.max_llm_cost_usd * policy.soft_stop_ratio:
        metrics.budget_soft_stop_hit = True
        return False
    return True


def _refresh_cost(metrics: ContextualRunMetrics, *, engine: OpenAITranslationEngine, model: str) -> None:
    metrics.estimated_cost_usd = engine.estimate_total_cost_usd(metrics.call_metrics, model=model)


def _term_glossary_payload(lookup: dict[str, ApprovedTermMemoryEntry]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    seen: set[str] = set()
    for entry in lookup.values():
        key = _normalize_term_key(entry.source_surface or entry.normalized_form)
        if not key or key in seen or not entry.approved_target:
            continue
        seen.add(key)
        items.append(
            {
                "source_term": entry.source_surface or entry.normalized_form,
                "preferred_vi": entry.approved_target,
                "status": "approved" if entry.approved_by_human else "prefer",
            }
        )
    return items


def _promote_run_local_entry(
    run_local_memory: dict[str, ApprovedTermMemoryEntry],
    *,
    source_surface: str,
    approved_target: str,
    confidence: float,
    now: str,
) -> None:
    if not source_surface or not approved_target:
        return
    entry = ApprovedTermMemoryEntry(
        source_surface=source_surface,
        normalized_form=_normalize_term_key(source_surface),
        approved_target=approved_target,
        context_fingerprint="run-local",
        approved_by_human=False,
        last_seen=now,
        confidence=confidence,
    )
    run_local_memory[_normalize_term_key(source_surface)] = entry


def _project_scene_record(project_id: str, scene: SceneChunk, *, summary: str, now: str) -> SceneMemoryRecord:
    return SceneMemoryRecord(
        scene_id=scene.scene_id,
        project_id=project_id,
        scene_index=scene.scene_index,
        start_segment_index=scene.start_segment_index,
        end_segment_index=scene.end_segment_index,
        start_ms=scene.start_ms,
        end_ms=scene.end_ms,
        short_scene_summary=summary,
        participants_json=[],
        unresolved_references_json=[],
        open_ambiguities_json=[],
        status="planned",
        created_at=now,
        updated_at=now,
    )


def _append_dialogue_scene_analyses(
    *,
    context: JobContext,
    workspace,
    database: ProjectDatabase,
    engine: OpenAITranslationEngine,
    scene: SceneChunk,
    now: str,
    active_prompt_family: dict[str, TranslationPromptTemplate],
    source_language: str,
    target_language: str,
    model: str,
    project_profile_id: str | None,
    metrics: ContextualRunMetrics,
    character_profiles: dict[str, CharacterProfileRecord],
    relationship_profiles: dict[str, RelationshipProfileRecord],
    analyses: list[SegmentAnalysisRecord],
    scene_records: list[SceneMemoryRecord],
) -> None:
    def _record_call(metric) -> None:
        metrics.llm_call_count += 1
        metrics.call_metrics.append(metric)

    def _count_retry() -> None:
        metrics.llm_retry_count += 1

    planner_output = engine.plan_scene(
        context,
        template=active_prompt_family["scene_planner"],
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
        prompt_cache_key=OpenAITranslationEngine.build_prompt_cache_key(
            template=active_prompt_family["scene_planner"],
            model=model,
            source_language=source_language,
            target_language=target_language,
            route_mode=DIALOGUE_LEGACY_ROUTE,
            project_profile_id=project_profile_id,
        ),
        record_call=_record_call,
        route_mode=DIALOGUE_LEGACY_ROUTE,
    )
    _refresh_cost(metrics, engine=engine, model=model)
    scene_records.append(_scene_record_from_output(workspace.project_id, scene, planner_output, now=now))
    for seed in planner_output.character_updates:
        character_profiles[seed.character_id] = _character_record_from_seed(workspace.project_id, seed, now=now)
    for seed in planner_output.relationship_updates:
        relationship_profiles[seed.relationship_id] = _relationship_record_from_seed(
            workspace.project_id,
            seed,
            now=now,
            scene_id=scene.scene_id,
        )

    scene_batches = _chunk_scene_segments(scene.segments, batch_size=CONTEXTUAL_STAGE_BATCH_SIZE)
    semantic_items: dict[str, object] = {}
    semantic_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
        template=active_prompt_family["semantic_pass"],
        model=model,
        source_language=source_language,
        target_language=target_language,
        route_mode=DIALOGUE_LEGACY_ROUTE,
        project_profile_id=project_profile_id,
    )
    for batch_index, batch_rows in enumerate(scene_batches, start=1):
        metrics.batch_count += 1

        def semantic_runner(current_rows, current_batch_index, current_batch_count):
            return engine.analyze_semantics(
                context,
                template=active_prompt_family["semantic_pass"],
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
                output_model=SemanticBatchOutput,
                prompt_cache_key=semantic_prompt_cache_key,
                record_call=_record_call,
                route_mode=DIALOGUE_LEGACY_ROUTE,
            )

        semantic_items.update(
            _run_stage_batch_with_retry(
                context=context,
                stage_label="Semantic pass",
                scene_id=scene.scene_id,
                batch_rows=batch_rows,
                batch_index=batch_index,
                batch_count=len(scene_batches),
                stage_runner=semantic_runner,
                on_retry=_count_retry,
            )
        )
    _refresh_cost(metrics, engine=engine, model=model)
    _validate_stage_item_ids(
        "Semantic pass",
        expected_ids=list(scene.segment_ids),
        actual_ids=list(semantic_items),
        scene_id=scene.scene_id,
        batch_index=len(scene_batches),
        batch_count=len(scene_batches),
    )

    adaptation_items: dict[str, object] = {}
    adaptation_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
        template=active_prompt_family["dialogue_adaptation"],
        model=model,
        source_language=source_language,
        target_language=target_language,
        route_mode=DIALOGUE_LEGACY_ROUTE,
        project_profile_id=project_profile_id,
    )
    for batch_index, batch_rows in enumerate(scene_batches, start=1):
        metrics.batch_count += 1

        def adaptation_runner(current_rows, current_batch_index, current_batch_count):
            return engine.adapt_dialogue(
                context,
                template=active_prompt_family["dialogue_adaptation"],
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
                output_model=DialogueAdaptationBatchOutput,
                prompt_cache_key=adaptation_prompt_cache_key,
                record_call=_record_call,
                route_mode=DIALOGUE_LEGACY_ROUTE,
            )

        adaptation_items.update(
            _run_stage_batch_with_retry(
                context=context,
                stage_label="Dialogue adaptation",
                scene_id=scene.scene_id,
                batch_rows=batch_rows,
                batch_index=batch_index,
                batch_count=len(scene_batches),
                stage_runner=adaptation_runner,
                on_retry=_count_retry,
            )
        )
    _refresh_cost(metrics, engine=engine, model=model)
    _validate_stage_item_ids(
        "Dialogue adaptation",
        expected_ids=list(scene.segment_ids),
        actual_ids=list(adaptation_items),
        scene_id=scene.scene_id,
        batch_index=len(scene_batches),
        batch_count=len(scene_batches),
    )

    critic_items: dict[str, object] = {}
    if "semantic_critic" in active_prompt_family:
        critic_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
            template=active_prompt_family["semantic_critic"],
            model=model,
            source_language=source_language,
            target_language=target_language,
            route_mode=DIALOGUE_LEGACY_ROUTE,
            project_profile_id=project_profile_id,
        )
        for batch_index, batch_rows in enumerate(scene_batches, start=1):
            metrics.batch_count += 1

            def critic_runner(current_rows, current_batch_index, current_batch_count):
                return engine.critique_dialogue(
                    context,
                    template=active_prompt_family["semantic_critic"],
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
                    prompt_cache_key=critic_prompt_cache_key,
                    record_call=_record_call,
                    route_mode=DIALOGUE_LEGACY_ROUTE,
                )

            critic_items.update(
                _run_stage_batch_with_retry(
                    context=context,
                    stage_label="Semantic critic",
                    scene_id=scene.scene_id,
                    batch_rows=batch_rows,
                    batch_index=batch_index,
                    batch_count=len(scene_batches),
                    stage_runner=critic_runner,
                    on_retry=_count_retry,
                )
            )
        _refresh_cost(metrics, engine=engine, model=model)

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
        review_reason_codes = [_normalize_review_reason_code(code) for code in review_reason_codes]
        review_reason_codes = list(dict.fromkeys(review_reason_codes))
        needs_review = (
            semantic_item.needs_human_review
            or adaptation_item.needs_human_review
            or bool(getattr(critic_item, "review_needed", False))
        )
        merged_risk_flags = list(dict.fromkeys(list(semantic_item.risk_flags) + list(adaptation_item.risk_flags)))
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
                risk_flags_json=merged_risk_flags,
                confidence_json=semantic_item.confidence.model_dump(mode="json", by_alias=True),
                needs_human_review=needs_review,
                review_status="needs_review" if needs_review else "approved",
                review_reason_codes_json=review_reason_codes,
                review_question=semantic_item.review_question,
                approved_subtitle_text=adaptation_item.subtitle_text.strip(),
                approved_tts_text=adaptation_item.tts_text.strip(),
                semantic_qc_passed=not critic_issues,
                semantic_qc_issues_json=critic_issues,
                source_template_family_id=active_prompt_family["semantic_pass"].family_id
                or active_prompt_family["semantic_pass"].template_id,
                adaptation_template_family_id=active_prompt_family["dialogue_adaptation"].family_id
                or active_prompt_family["dialogue_adaptation"].template_id,
                created_at=now,
                updated_at=now,
            )
        )


def run_contextual_translation_v2(
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
    checkpoint_state: ContextualTranslationCheckpointState | None = None,
    checkpoint_writer: Callable[[list[SceneMemoryRecord], list[CharacterProfileRecord], list[RelationshipProfileRecord], list[SegmentAnalysisRecord], list[SceneRouteDecision], list[Any], ContextualRunMetrics, list[str], int], None] | None = None,
) -> dict[str, object]:
    selected_prompt_family = resolve_prompt_family(workspace.root_dir, selected_template)
    dialogue_prompt_family = resolve_prompt_family(
        workspace.root_dir,
        load_prompt_template(workspace.root_dir, "contextual_default_adaptation"),
    )
    for family_name, family, required_roles in (
        ("selected", selected_prompt_family, {"semantic_pass", "dialogue_adaptation"}),
        ("dialogue", dialogue_prompt_family, {"scene_planner", "semantic_pass", "dialogue_adaptation"}),
    ):
        missing_roles = sorted(required_roles - set(family))
        if missing_roles:
            raise RuntimeError(f"Thiếu contextual prompt roles trong {family_name} family: {', '.join(missing_roles)}")

    scenes = chunk_segments_into_scenes(segments)
    now = _utc_now_iso()
    profile_state = load_project_profile_state(workspace.root_dir)
    project_profile_id = profile_state.project_profile_id if profile_state is not None else None
    prior_analysis_rows = {
        str(row["segment_id"]): dict(row)
        for row in database.list_segment_analyses(workspace.project_id)
    }
    if checkpoint_state is not None:
        for item in checkpoint_state.analyses:
            prior_analysis_rows.setdefault(
                item.segment_id,
                {
                    "segment_id": item.segment_id,
                    "speaker_json": item.speaker_json,
                    "review_reason_codes_json": item.review_reason_codes_json,
                },
            )

    if checkpoint_state is not None and checkpoint_state.route_decisions:
        route_decisions = list(checkpoint_state.route_decisions)
    else:
        route_decisions = [
            _route_scene_v2(
                scene,
                prior_analysis_rows=prior_analysis_rows,
                narration_family_id=selected_prompt_family["dialogue_adaptation"].family_id
                or selected_prompt_family["dialogue_adaptation"].template_id,
                dialogue_family_id=dialogue_prompt_family["dialogue_adaptation"].family_id
                or dialogue_prompt_family["dialogue_adaptation"].template_id,
                prefer_narration=True,
            )
            for scene in scenes
        ]

    spans, span_by_scene_id = _build_narration_spans(scenes, route_decisions)
    metrics = checkpoint_state.metrics if checkpoint_state is not None else ContextualRunMetrics()
    metrics.router_version = NARRATION_FAST_V2_ROUTER_VERSION
    metrics.semantic_schema_version = selected_prompt_family["semantic_pass"].output_schema_version
    metrics.postprocess_version = NARRATION_FAST_V2_POSTPROCESS_VERSION
    metrics.span_count = len(spans)

    scene_records: list[SceneMemoryRecord] = list(checkpoint_state.scenes) if checkpoint_state is not None else []
    character_profiles = (
        {item.character_id: item for item in checkpoint_state.character_profiles}
        if checkpoint_state is not None
        else {
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
    )
    relationship_profiles = (
        {item.relationship_id: item for item in checkpoint_state.relationship_profiles}
        if checkpoint_state is not None
        else {
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
    )
    analyses: list[SegmentAnalysisRecord] = list(checkpoint_state.analyses) if checkpoint_state is not None else []
    completed_scene_ids = set(checkpoint_state.completed_scene_ids) if checkpoint_state is not None else set()

    global_term_memory = _load_term_memory_entries(_global_term_memory_path())
    project_term_memory_entries = _load_term_memory_entries(_project_term_memory_path(workspace.root_dir))
    run_local_term_memory: dict[str, ApprovedTermMemoryEntry] = {}
    policy = NarrationBudgetPolicy(
        entity_micro_cap=min(12, max(1, math.ceil(0.15 * max(1, len(spans))))),
        ambiguity_micro_cap=min(10, max(1, math.ceil(0.12 * max(1, len(spans))))),
        slot_rewrite_cap=min(8, max(1, math.ceil(0.10 * max(1, len(spans))))),
    )

    route_decision_by_scene = {item.scene_id: item for item in route_decisions}
    scene_by_id = {scene.scene_id: scene for scene in scenes}
    scene_cursor = 0
    total_scenes = max(1, len(scenes))
    while scene_cursor < len(scenes):
        scene = scenes[scene_cursor]
        route_decision = route_decision_by_scene[scene.scene_id]
        if scene.scene_id in completed_scene_ids:
            if route_decision.route_mode == NARRATION_FAST_V2_ROUTE and scene.scene_id in span_by_scene_id:
                scene_cursor += len(span_by_scene_id[scene.scene_id].scene_ids)
            else:
                scene_cursor += 1
            continue

        if route_decision.route_mode == DIALOGUE_LEGACY_ROUTE:
            context.report_progress(
                min(20, int((scene_cursor + 1) * 20 / total_scenes)),
                f"Contextual V2 dialogue legacy: scene {scene_cursor + 1}/{total_scenes}",
            )
            _append_dialogue_scene_analyses(
                context=context,
                workspace=workspace,
                database=database,
                engine=engine,
                scene=scene,
                now=now,
                active_prompt_family=dialogue_prompt_family,
                source_language=source_language,
                target_language=target_language,
                model=model,
                project_profile_id=project_profile_id,
                metrics=metrics,
                character_profiles=character_profiles,
                relationship_profiles=relationship_profiles,
                analyses=analyses,
                scene_records=scene_records,
            )
            completed_scene_ids.add(scene.scene_id)
            scene_cursor += 1
        else:
            span = span_by_scene_id[scene.scene_id]
            span_scenes = [scene_by_id[scene_id] for scene_id in span.scene_ids]
            context.report_progress(
                min(30, int(((scene_cursor + len(span.scene_ids)) / total_scenes) * 30)),
                f"Narration Fast V2: span {span.span_index + 1}/{max(1, len(spans))}",
            )

            def _record_call(metric) -> None:
                metrics.llm_call_count += 1
                metrics.call_metrics.append(metric)

            def _count_retry() -> None:
                metrics.llm_retry_count += 1

            span_context = _span_context_payload(span=span, scenes=span_scenes)
            memory_lookup = _merge_term_memory(
                run_local=list(run_local_term_memory.values()),
                project_local=project_term_memory_entries,
                global_defaults=global_term_memory,
            )
            base_glossary = _build_glossary_payload(
                database,
                workspace.project_id,
                relationship_rows=[],
                narration_term_sheet=_term_glossary_payload(memory_lookup),
            )
            scene_summary = span_context["scene_summary"]
            for span_scene in span_scenes:
                scene_records.append(_project_scene_record(workspace.project_id, span_scene, summary=scene_summary, now=now))

            render_rows = [row for scene_item in span_scenes for row in scene_item.segments]
            span_payload = _span_payload(span, span_scenes)
            metrics.base_semantic_call_count += 1
            metrics.batch_count += 1
            base_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                template=selected_prompt_family["semantic_pass"],
                model=model,
                source_language=source_language,
                target_language=target_language,
                route_mode=NARRATION_FAST_V2_ROUTE,
                project_profile_id=project_profile_id,
            )
            canonical_items = _run_stage_batch_with_positional_retry(
                context=context,
                stage_label="Narration canonical semantic",
                scene_id=span.span_id,
                batch_rows=render_rows,
                batch_index=span.span_index + 1,
                batch_count=max(1, len(spans)),
                stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.analyze_narration_canonical_span(
                    context,
                    template=selected_prompt_family["semantic_pass"],
                    span_payload={
                        "span": {
                            **span_payload["span"],
                            "render_units": [
                                item
                                for item in span_payload["span"]["render_units"]
                                if item["segment_id"] in {str(row["segment_id"]) for row in current_rows}
                            ],
                            "batch_index": current_batch_index,
                            "batch_count": current_batch_count,
                        }
                    },
                    source_language=source_language,
                    target_language=target_language,
                    context_payload=span_context,
                    glossary_payload=base_glossary,
                    model=model,
                    prompt_cache_key=base_prompt_cache_key,
                    record_call=_record_call,
                    route_mode=NARRATION_FAST_V2_ROUTE,
                ),
                on_retry=_count_retry,
            )
            _refresh_cost(metrics, engine=engine, model=model)

            canonical_by_segment: dict[str, dict[str, object]] = {}
            for row in render_rows:
                segment_id = str(row["segment_id"])
                item = canonical_items[segment_id]
                canonical_text = _normalize_canonical_text(item.canonical_text)
                risk_flags = list(dict.fromkeys(item.risk_flags))
                if item.unsafe_to_guess and "unsafe_to_guess" not in risk_flags:
                    risk_flags.append("unsafe_to_guess")
                slot_pressure = _estimated_slot_pressure(canonical_text, int(row["end_ms"]) - int(row["start_ms"]))
                canonical_by_segment[segment_id] = {
                    "segment_id": segment_id,
                    "scene_id": next(scene_item.scene_id for scene_item in span_scenes if segment_id in scene_item.segment_ids),
                    "segment_index": int(row["segment_index"]),
                    "source_text": str(row["source_text"] or ""),
                    "canonical_text": canonical_text,
                    "risk_flags": risk_flags,
                    "needs_shortening": bool(item.needs_shortening),
                    "unsafe_to_guess": bool(item.unsafe_to_guess),
                    "entities": [entity.model_dump(mode="json") for entity in item.entities],
                    "slot_pressure": slot_pressure,
                    "review_reason_codes": [],
                    "review_question": "",
                }
            _apply_scientific_notation_autofix(
                render_rows=render_rows,
                canonical_by_segment=canonical_by_segment,
            )
            entity_candidates: list[dict[str, object]] = []
            ambiguity_candidates: list[dict[str, object]] = []
            slot_candidates: list[dict[str, object]] = []
            for row in render_rows:
                segment_id = str(row["segment_id"])
                item = canonical_items[segment_id]
                segment_payload = canonical_by_segment[segment_id]
                segment_risk_flags = list(segment_payload["risk_flags"])
                for entity in item.entities:
                    entity_key = _normalize_term_key(entity.src)
                    if entity_key and entity_key in memory_lookup and memory_lookup[entity_key].approved_target:
                        metrics.approved_term_memory_hits += 1
                        continue
                    if entity.status != "approved" or any(flag in segment_risk_flags for flag in _HARD_ENTITY_FLAGS):
                        entity_candidates.append(
                            {
                                "segment_id": segment_id,
                                "source_term": entity.src,
                                "preferred_target": entity.dst,
                                "status": entity.status,
                                "confidence": entity.confidence,
                                "notes": entity.notes,
                            }
                        )
                if bool(segment_payload["unsafe_to_guess"]) or any(flag in _AMBIGUITY_FLAGS for flag in segment_risk_flags):
                    ambiguity_candidates.append(
                        {
                            "segment_id": segment_id,
                            "canonical_text": str(segment_payload["canonical_text"]),
                            "risk_flags": segment_risk_flags,
                            "unsafe_to_guess": bool(segment_payload["unsafe_to_guess"]),
                        }
                    )
                if bool(segment_payload["needs_shortening"]) or float(segment_payload["slot_pressure"]) > SLOT_PRESSURE_THRESHOLD:
                    slot_candidates.append(
                        {
                            "segment_id": segment_id,
                            "canonical_text": str(segment_payload["canonical_text"]),
                            "slot_pressure": float(segment_payload["slot_pressure"]),
                        }
                    )
            if (
                entity_candidates
                and "entity_micro_pass" in selected_prompt_family
                and metrics.entity_micro_pass_count < policy.entity_micro_cap
                and _budget_allows_soft_escalation(metrics, policy)
            ):
                metrics.entity_micro_pass_count += 1
                entity_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                    template=selected_prompt_family["entity_micro_pass"],
                    model=model,
                    source_language=source_language,
                    target_language=target_language,
                    route_mode=NARRATION_FAST_V2_ROUTE,
                    project_profile_id=project_profile_id,
                )
                entity_output = engine.resolve_narration_entities(
                    context,
                    template=selected_prompt_family["entity_micro_pass"],
                    span_payload={"span_id": span.span_id, "items": entity_candidates[: policy.entity_micro_cap]},
                    source_language=source_language,
                    target_language=target_language,
                    context_payload=span_context,
                    glossary_payload=base_glossary,
                    model=model,
                    prompt_cache_key=entity_prompt_cache_key,
                    record_call=_record_call,
                    route_mode=NARRATION_FAST_V2_ROUTE,
                )
                _refresh_cost(metrics, engine=engine, model=model)
                for item in entity_output.items:
                    anchor_segment_ids: list[str] = []
                    for raw_position in item.segment_positions:
                        try:
                            position = int(raw_position)
                        except (TypeError, ValueError):
                            continue
                        if 0 <= position < len(render_rows):
                            anchor_segment_ids.append(str(render_rows[position]["segment_id"]))
                    if not anchor_segment_ids:
                        anchor_segment_ids = _fallback_term_anchor_segment_ids(
                            type("SpanScene", (), {"segments": render_rows}),
                            item.source_term,
                        )
                    for segment_id in anchor_segment_ids:
                        segment_payload = canonical_by_segment.get(segment_id)
                        if segment_payload is None:
                            continue
                        if item.status in {"approved", "prefer"} and item.approved_target:
                            _promote_run_local_entry(
                                run_local_term_memory,
                                source_surface=item.source_term,
                                approved_target=item.approved_target,
                                confidence=float(item.confidence or 0.0),
                                now=now,
                            )
                        elif "technical_term_uncertainty" not in segment_payload["review_reason_codes"]:
                            segment_payload["review_reason_codes"].append("technical_term_uncertainty")
                            segment_payload["review_question"] = (
                                segment_payload["review_question"]
                                or f"Thuật ngữ/thực thể chưa đủ chắc để chốt: {item.source_term}."
                            )

            if (
                ambiguity_candidates
                and "ambiguity_micro_pass" in selected_prompt_family
                and metrics.ambiguity_micro_pass_count < policy.ambiguity_micro_cap
                and (any(item["unsafe_to_guess"] for item in ambiguity_candidates) or _budget_allows_soft_escalation(metrics, policy))
            ):
                metrics.ambiguity_micro_pass_count += 1
                ambiguity_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                    template=selected_prompt_family["ambiguity_micro_pass"],
                    model=model,
                    source_language=source_language,
                    target_language=target_language,
                    route_mode=NARRATION_FAST_V2_ROUTE,
                    project_profile_id=project_profile_id,
                )
                ambiguity_output = _run_stage_batch_with_positional_retry(
                    context=context,
                    stage_label="Narration ambiguity micro-pass",
                    scene_id=span.span_id,
                    batch_rows=ambiguity_candidates[: policy.ambiguity_micro_cap],
                    batch_index=span.span_index + 1,
                    batch_count=max(1, len(spans)),
                    stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.resolve_narration_ambiguity(
                        context,
                        template=selected_prompt_family["ambiguity_micro_pass"],
                        span_payload={
                            "span_id": span.span_id,
                            "items": current_rows,
                            "batch_index": current_batch_index,
                            "batch_count": current_batch_count,
                        },
                        source_language=source_language,
                        target_language=target_language,
                        context_payload=span_context,
                        glossary_payload=base_glossary,
                        model=model,
                        prompt_cache_key=ambiguity_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=NARRATION_FAST_V2_ROUTE,
                    ),
                    on_retry=_count_retry,
                )
                _refresh_cost(metrics, engine=engine, model=model)
                for segment_id, item in ambiguity_output.items():
                    segment_payload = canonical_by_segment.get(segment_id)
                    if segment_payload is None:
                        continue
                    segment_payload["canonical_text"] = _normalize_canonical_text(item.canonical_text)
                    segment_payload["risk_flags"] = list(dict.fromkeys(segment_payload["risk_flags"] + list(item.risk_flags)))
                    if item.unsafe_to_guess and "ambiguous_term" not in segment_payload["review_reason_codes"]:
                        segment_payload["review_reason_codes"].append("ambiguous_term")
                        segment_payload["review_question"] = (
                            segment_payload["review_question"] or "Ngữ cảnh vẫn chưa đủ chắc để chốt câu narration này."
                        )

            if (
                slot_candidates
                and metrics.slot_rewrite_count < policy.slot_rewrite_cap
                and "dialogue_adaptation" in selected_prompt_family
                and _budget_allows_soft_escalation(metrics, policy)
            ):
                metrics.slot_rewrite_count += 1
                slot_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                    template=selected_prompt_family["dialogue_adaptation"],
                    model=model,
                    source_language=source_language,
                    target_language=target_language,
                    route_mode=NARRATION_FAST_V2_ROUTE,
                    project_profile_id=project_profile_id,
                )
                slot_output = _run_stage_batch_with_positional_retry(
                    context=context,
                    stage_label="Narration slot rewrite",
                    scene_id=span.span_id,
                    batch_rows=slot_candidates[: policy.slot_rewrite_cap],
                    batch_index=span.span_index + 1,
                    batch_count=max(1, len(spans)),
                    stage_runner=lambda current_rows, current_batch_index, current_batch_count: engine.rewrite_narration_slot(
                        context,
                        template=selected_prompt_family["dialogue_adaptation"],
                        span_payload={
                            "span": {
                                "span_id": span.span_id,
                                "batch_index": current_batch_index,
                                "batch_count": current_batch_count,
                                "items": current_rows,
                            }
                        },
                        source_language=source_language,
                        target_language=target_language,
                        context_payload=span_context,
                        glossary_payload=base_glossary,
                        model=model,
                        prompt_cache_key=slot_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=NARRATION_FAST_V2_ROUTE,
                    ),
                    on_retry=_count_retry,
                )
                _refresh_cost(metrics, engine=engine, model=model)
                for segment_id, item in slot_output.items():
                    segment_payload = canonical_by_segment.get(segment_id)
                    if segment_payload is None:
                        continue
                    segment_payload["canonical_text"] = _normalize_canonical_text(item.canonical_text)
                    segment_payload["risk_flags"] = list(dict.fromkeys(segment_payload["risk_flags"] + list(item.risk_flags)))
                    for code in item.review_reason_codes:
                        normalized_code = _normalize_review_reason_code(str(code))
                        if normalized_code not in segment_payload["review_reason_codes"]:
                            segment_payload["review_reason_codes"].append(normalized_code)

            for row in render_rows:
                segment_id = str(row["segment_id"])
                segment_payload = canonical_by_segment[segment_id]
                risk_flags = list(dict.fromkeys(segment_payload["risk_flags"]))
                review_reason_codes = [
                    _normalize_review_reason_code(code)
                    for code in segment_payload["review_reason_codes"]
                ]
                review_reason_codes = list(dict.fromkeys(review_reason_codes))
                needs_review = bool(review_reason_codes) or bool(segment_payload["unsafe_to_guess"])
                analyses.append(
                    SegmentAnalysisRecord(
                        segment_id=segment_id,
                        project_id=workspace.project_id,
                        scene_id=segment_payload["scene_id"],
                        segment_index=int(row["segment_index"]),
                        speaker_json=_base_narration_speaker(),
                        listeners_json=_base_narration_listener(),
                        register_json=_base_narration_register(),
                        turn_function="inform",
                        resolved_ellipsis_json={},
                        honorific_policy_json={},
                        semantic_translation=segment_payload["canonical_text"],
                        glossary_hits_json=[],
                        risk_flags_json=risk_flags,
                        confidence_json=_base_narration_confidence(),
                        needs_human_review=needs_review,
                        review_status="needs_review" if needs_review else "approved",
                        review_reason_codes_json=review_reason_codes,
                        review_question=segment_payload["review_question"],
                        approved_subtitle_text=segment_payload["canonical_text"],
                        approved_tts_text=segment_payload["canonical_text"],
                        semantic_qc_passed=True,
                        semantic_qc_issues_json=[],
                        source_template_family_id=selected_prompt_family["semantic_pass"].family_id
                        or selected_prompt_family["semantic_pass"].template_id,
                        adaptation_template_family_id=selected_prompt_family["dialogue_adaptation"].family_id
                        or selected_prompt_family["dialogue_adaptation"].template_id,
                        created_at=now,
                        updated_at=now,
                    )
                )

            completed_scene_ids.update(span.scene_ids)
            scene_cursor += len(span.scene_ids)

        if checkpoint_writer is not None:
            checkpoint_writer(
                scene_records,
                list(character_profiles.values()),
                list(relationship_profiles.values()),
                analyses,
                route_decisions,
                [],
                metrics,
                sorted(completed_scene_ids),
                total_scenes,
            )

    if project_term_memory_entries:
        _save_project_term_memory(workspace.root_dir, project_term_memory_entries)

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
        "technical_term_uncertainty",
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

    route_counts = {
        NARRATION_FAST_V2_ROUTE: sum(1 for item in route_decisions if item.route_mode == NARRATION_FAST_V2_ROUTE),
        DIALOGUE_LEGACY_ROUTE: sum(1 for item in route_decisions if item.route_mode == DIALOGUE_LEGACY_ROUTE),
    }
    return {
        "scenes": scene_records,
        "character_profiles": list(character_profiles.values()),
        "relationship_profiles": list(relationship_profiles.values()),
        "segment_analyses": patched_analyses,
        "route_decisions": route_decisions,
        "term_entity_sheets": [],
        "metrics": metrics,
        "fast_path": {
            "active": True,
            "mode": "narration_fast_v2_mixed" if route_counts[DIALOGUE_LEGACY_ROUTE] else "narration_fast_v2",
            "planner_mode": "span_based",
            "critic_enabled": bool(route_counts[DIALOGUE_LEGACY_ROUTE]),
            "route_counts": route_counts,
            "span_count": metrics.span_count,
            "base_semantic_call_count": metrics.base_semantic_call_count,
            "entity_micro_pass_count": metrics.entity_micro_pass_count,
            "ambiguity_micro_pass_count": metrics.ambiguity_micro_pass_count,
            "slot_rewrite_count": metrics.slot_rewrite_count,
            "full_rescue_count": metrics.full_rescue_count,
            "estimated_cost_usd": metrics.estimated_cost_usd,
            "budget_soft_stop_hit": metrics.budget_soft_stop_hit,
            "approved_term_memory_hits": metrics.approved_term_memory_hits,
        },
        "semantic_qc": {
            "error_count": qc_report.error_count,
            "warning_count": qc_report.warning_count,
            "total_segments": qc_report.total_segments,
        },
    }
