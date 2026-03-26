from __future__ import annotations

import json
import math
import re
from dataclasses import asdict
from sqlite3 import Row
from typing import Any, Callable

from pydantic import ValidationError

from app.core.jobs import JobContext
from app.project.database import ProjectDatabase
from app.project.models import CharacterProfileRecord, RelationshipProfileRecord, SceneMemoryRecord, SegmentAnalysisRecord
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
from .models import (
    ContextualRunMetrics,
    DialogueAdaptationBatchOutput,
    NarrationTermEntityBatchOutput,
    NarrationAdaptationBatchOutput,
    ResolvedNarrationTermEntityItem,
    SceneTermEntitySheet,
    NarrationSemanticBatchOutput,
    ScenePlannerOutput,
    SceneRouteDecision,
    SemanticBatchOutput,
    TranslationPromptTemplate,
)
from .openai_engine import OpenAITranslationEngine
from .presets import (
    is_narration_fast_template,
    is_narration_fast_v2_template,
    load_prompt_template,
    resolve_prompt_family,
)
from .scene_chunker import chunk_segments_into_scenes
from .semantic_qc import analyze_segment_analyses

NARRATION_FAST_BATCH_SIZE = 16
NARRATION_ROUTE_HIGH_CONFIDENCE = 0.75
NARRATION_ROUTE_LOW_CONFIDENCE = 0.45
_ROUTING_REVIEW_CODES = {
    "uncertain_speaker",
    "uncertain_listener",
    "unclear_relationship",
    "unclear_context",
    "ambiguous_reference",
    "ambiguous_object_reference",
}
_VOCATIVE_PATTERN = re.compile(r"(各位|大家|朋友们|兄弟们|姐妹们|先生们|女士们|同学们|孩子们|观众朋友们)")
_BACKCHANNEL_PATTERN = re.compile(r"^(嗯|啊|哦|诶|欸|哎|呀|唉|好|行|对|是啊|对啊|嗯嗯)[！!。.\s]*$")


_LATIN_OR_DIGIT_PATTERN = re.compile(r"[A-Za-z0-9]")
_TERM_INTRO_PATTERNS = (
    "称为",
    "被称为",
    "又称",
    "叫做",
    "学名",
    "术语",
    "结构",
    "机制",
    "现象",
    "模型",
)

_TERM_ANCHOR_STRIP_CHARS = "\"'“”‘’《》〈〉「」『』()[]{}<>"


def _normalize_term_anchor_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).casefold()


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        getter = getattr(row, "get", None)
        if callable(getter):
            return getter(key)
    return None


def _fallback_term_anchor_segment_ids(scene, source_term: str) -> list[str]:
    raw_term = str(source_term or "").strip().strip(_TERM_ANCHOR_STRIP_CHARS)
    if not raw_term:
        return []
    normalized_term = _normalize_term_anchor_text(raw_term)
    if len(normalized_term) < 2 and not _LATIN_OR_DIGIT_PATTERN.search(raw_term):
        return []

    resolved_segment_ids: list[str] = []
    for row in scene.segments:
        segment_id = str(row["segment_id"])
        source_text = str(_row_value(row, "source_text") or "").strip()
        if not source_text:
            continue
        if raw_term in source_text or normalized_term in _normalize_term_anchor_text(source_text):
            if segment_id not in resolved_segment_ids:
                resolved_segment_ids.append(segment_id)
    return resolved_segment_ids


def _normalize_review_reason_code(raw_code: str) -> str:
    text = raw_code.strip()
    if not text:
        return "unspecified_review_reason"
    lowered = text.lower()
    if "speaker" in lowered and ("uncertain" in lowered or "ambiguous" in lowered):
        return "uncertain_speaker"
    if ("listener" in lowered or "addressee" in lowered) and ("uncertain" in lowered or "ambiguous" in lowered):
        return "uncertain_listener"
    if "damage" in lowered:
        return "ambiguous_damage_description"
    if "object" in lowered:
        return "ambiguous_object_reference"
    if "reference" in lowered:
        return "ambiguous_reference"
    if "tone" in lowered:
        return "tone_ambiguity"
    if "technical" in lowered and "term" in lowered:
        return "technical_term_uncertainty"
    if "term" in lowered or "meaning" in lowered or "ambiguousterm" in lowered:
        return "ambiguous_term"
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized or "unspecified_review_reason"


def _build_narration_fast_scene_plan(scene) -> ScenePlannerOutput:
    lead_text = str(scene.segments[0]["source_text"] or "").strip() if scene.segments else ""
    short_lead = lead_text[:96] + ("..." if len(lead_text) > 96 else "")
    summary = f"Narration fast path scene with {len(scene.segments)} segments. Lead: {short_lead or 'narration batch'}"
    return ScenePlannerOutput(
        scene_id=scene.scene_id,
        scene_summary=summary,
        participants=[],
        recent_turn_digest=short_lead,
        open_ambiguities=[],
        unresolved_references=[],
        character_updates=[],
        relationship_updates=[],
    )


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, numerator / denominator))


def _is_short_utterance(text: str) -> bool:
    return len(text.strip()) <= 12


def _is_long_sentence(text: str) -> bool:
    normalized = text.strip()
    return len(normalized) >= 24 or normalized.count("，") + normalized.count(",") >= 2


def _prior_review_penalty(scene, prior_analysis_rows: dict[str, dict[str, object]]) -> float:
    flagged = 0
    for row in scene.segments:
        analysis_row = prior_analysis_rows.get(str(row["segment_id"]))
        if not analysis_row:
            continue
        raw_codes = analysis_row.get("review_reason_codes_json") or []
        if isinstance(raw_codes, str):
            try:
                raw_codes = json.loads(raw_codes)
            except Exception:
                raw_codes = []
        normalized_codes = {_normalize_review_reason_code(str(item)) for item in raw_codes}
        if normalized_codes & _ROUTING_REVIEW_CODES:
            flagged += 1
    return _safe_ratio(flagged, len(scene.segments))


def _speaker_metrics_for_scene(scene, prior_analysis_rows: dict[str, dict[str, object]]) -> tuple[float, float]:
    speaker_keys: list[str] = []
    for row in scene.segments:
        analysis_row = prior_analysis_rows.get(str(row["segment_id"]))
        if not analysis_row:
            continue
        speaker_json = analysis_row.get("speaker_json") or {}
        if isinstance(speaker_json, str):
            try:
                speaker_json = json.loads(speaker_json)
            except Exception:
                speaker_json = {}
        speaker_key = str((speaker_json or {}).get("character_id") or "").strip().lower()
        if speaker_key and speaker_key != "unknown":
            speaker_keys.append(speaker_key)
    if not speaker_keys:
        return 0.6, 0.5
    dominance = max(speaker_keys.count(key) for key in set(speaker_keys)) / len(speaker_keys)
    if len(speaker_keys) <= 1:
        return max(0.0, min(1.0, dominance)), 0.0
    switches = sum(1 for left, right in zip(speaker_keys, speaker_keys[1:]) if left != right)
    return max(0.0, min(1.0, dominance)), _safe_ratio(switches, len(speaker_keys) - 1)


def _route_scene(
    scene,
    *,
    prior_analysis_rows: dict[str, dict[str, object]],
    narration_family_id: str,
    dialogue_family_id: str,
    prefer_narration: bool = False,
) -> SceneRouteDecision:
    texts = [str(row["source_text"] or "").strip() for row in scene.segments]
    nonempty_texts = [text for text in texts if text]
    total = max(1, len(nonempty_texts))
    speaker_dominance, speaker_switch_density = _speaker_metrics_for_scene(scene, prior_analysis_rows)
    question_density = _safe_ratio(sum(1 for text in nonempty_texts if "?" in text or "？" in text), total)
    vocative_density = _safe_ratio(sum(1 for text in nonempty_texts if _VOCATIVE_PATTERN.search(text)), total)
    backchannel_density = _safe_ratio(sum(1 for text in nonempty_texts if _BACKCHANNEL_PATTERN.match(text)), total)
    short_utterance_ratio = _safe_ratio(sum(1 for text in nonempty_texts if _is_short_utterance(text)), total)
    long_sentence_ratio = _safe_ratio(sum(1 for text in nonempty_texts if _is_long_sentence(text)), total)
    prior_penalty = _prior_review_penalty(scene, prior_analysis_rows)
    narration_score = (
        0.32 * speaker_dominance
        + 0.18 * (1.0 - speaker_switch_density)
        + 0.18 * long_sentence_ratio
        + 0.10 * (1.0 - short_utterance_ratio)
        + 0.08 * (1.0 - question_density)
        + 0.06 * (1.0 - vocative_density)
        + 0.05 * (1.0 - backchannel_density)
        - 0.15 * prior_penalty
    )
    if prefer_narration:
        narration_score += 0.2
        if (
            question_density == 0.0
            and vocative_density == 0.0
            and backchannel_density == 0.0
            and prior_penalty == 0.0
            and speaker_switch_density <= 0.5
        ):
            narration_score = max(narration_score, 0.76)
    narration_score = max(0.0, min(1.0, narration_score))
    route_mode = "dialogue"
    prompt_family_id = dialogue_family_id
    fallback_reason = ""
    if narration_score >= NARRATION_ROUTE_HIGH_CONFIDENCE:
        route_mode = "narration_fast"
        prompt_family_id = narration_family_id
    elif narration_score > NARRATION_ROUTE_LOW_CONFIDENCE:
        fallback_reason = "borderline_narration_score"
    return SceneRouteDecision(
        scene_id=scene.scene_id,
        scene_index=scene.scene_index,
        route_mode=route_mode,
        prompt_family_id=prompt_family_id,
        narration_score=round(narration_score, 4),
        speaker_dominance=round(speaker_dominance, 4),
        speaker_switch_density=round(speaker_switch_density, 4),
        question_density=round(question_density, 4),
        vocative_density=round(vocative_density, 4),
        backchannel_density=round(backchannel_density, 4),
        short_utterance_ratio=round(short_utterance_ratio, 4),
        long_sentence_ratio=round(long_sentence_ratio, 4),
        prior_review_penalty=round(prior_penalty, 4),
        fallback_reason=fallback_reason,
    )


def _should_run_narration_term_entity_pass(scene, route_decision: SceneRouteDecision) -> bool:
    if route_decision.route_mode != "narration_fast":
        return False
    texts = [str(row["source_text"] or "").strip() for row in scene.segments]
    nonempty_texts = [text for text in texts if text]
    if len(nonempty_texts) < 2:
        return False
    total_chars = sum(len(text) for text in nonempty_texts)
    intro_marker_hits = sum(1 for text in nonempty_texts if any(marker in text for marker in _TERM_INTRO_PATTERNS))
    has_latin_or_digit = any(_LATIN_OR_DIGIT_PATTERN.search(text) for text in nonempty_texts)
    return total_chars >= 72 and (
        route_decision.long_sentence_ratio >= 0.35
        or intro_marker_hits > 0
        or has_latin_or_digit
        or len(nonempty_texts) >= 6
    )


def _materialize_narration_term_sheet(
    scene,
    output: NarrationTermEntityBatchOutput,
) -> SceneTermEntitySheet:
    segment_ids = [str(row["segment_id"]) for row in scene.segments]
    resolved_items: list[ResolvedNarrationTermEntityItem] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for raw_item in output.items:
        source_term = str(raw_item.source_term or "").strip()
        preferred_vi = str(raw_item.preferred_vi or "").strip()
        if not source_term:
            continue
        normalized_status = str(raw_item.status or "").strip().lower()
        if normalized_status not in {"prefer", "needs_review"}:
            normalized_status = "prefer" if preferred_vi else "needs_review"
        resolved_segment_ids: list[str] = []
        for raw_position in raw_item.segment_positions:
            try:
                position = int(raw_position)
            except (TypeError, ValueError):
                continue
            if 0 <= position < len(segment_ids):
                segment_id = segment_ids[position]
                if segment_id not in resolved_segment_ids:
                    resolved_segment_ids.append(segment_id)
        if not resolved_segment_ids:
            resolved_segment_ids = _fallback_term_anchor_segment_ids(scene, source_term)
        dedupe_key = (source_term.casefold(), preferred_vi.casefold(), normalized_status)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        resolved_items.append(
            ResolvedNarrationTermEntityItem(
                source_term=source_term,
                preferred_vi=preferred_vi,
                category=str(raw_item.category or "term").strip() or "term",
                status=normalized_status,
                confidence=max(0.0, min(1.0, float(raw_item.confidence or 0.0))),
                segment_ids=resolved_segment_ids,
                notes=str(raw_item.notes or "").strip(),
            )
        )
    return SceneTermEntitySheet(
        scene_id=scene.scene_id,
        route_mode="narration_fast",
        active=True,
        items=resolved_items[:6],
    )


def _is_retryable_stage_exception(exc: Exception) -> bool:
    if isinstance(exc, ValidationError):
        return True
    if isinstance(exc, RuntimeError):
        message = " ".join(str(part) for part in exc.args).lower()
        return any(marker in message for marker in ("model khong tra ve du lieu co parse duoc", "invalid json", "json_invalid", "unexpected end", "eof while parsing"))
    return False


def _summarize_retryable_stage_exception(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        error_types = sorted({str(error.get("type") or "validation_error") for error in exc.errors()})
        return f"structured output validation failed ({', '.join(error_types)})"
    return str(exc).strip().splitlines()[0] or exc.__class__.__name__


def _retry_stage_batch_with_split(
    *,
    context: JobContext,
    stage_label: str,
    scene_id: str,
    batch_rows: list[Row],
    batch_index: int,
    batch_count: int,
    stage_runner: Callable[[list[Row], int, int], Any],
    reason: str,
    on_retry: Callable[[], None] | None = None,
) -> dict[str, Any]:
    midpoint = max(1, len(batch_rows) // 2)
    left_rows = batch_rows[:midpoint]
    right_rows = batch_rows[midpoint:]
    context.logger.warning(
        "%s failed for scene=%s batch=%s/%s (%s). Retrying with smaller batches (%s + %s).",
        stage_label,
        scene_id,
        batch_index,
        batch_count,
        reason,
        len(left_rows),
        len(right_rows),
    )
    if on_retry is not None:
        on_retry()
    merged_items = _run_stage_batch_with_retry(
        context=context,
        stage_label=stage_label,
        scene_id=scene_id,
        batch_rows=left_rows,
        batch_index=batch_index,
        batch_count=batch_count,
        stage_runner=stage_runner,
        on_retry=on_retry,
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
                on_retry=on_retry,
            )
        )
    return merged_items


def _run_stage_batch_with_retry(
    *,
    context: JobContext,
    stage_label: str,
    scene_id: str,
    batch_rows: list[Row],
    batch_index: int,
    batch_count: int,
    stage_runner: Callable[[list[Row], int, int], Any],
    on_retry: Callable[[], None] | None = None,
) -> dict[str, Any]:
    context.cancellation_token.raise_if_canceled()
    try:
        output = stage_runner(batch_rows, batch_index, batch_count)
    except Exception as exc:
        if len(batch_rows) <= 1 or not _is_retryable_stage_exception(exc):
            raise
        return _retry_stage_batch_with_split(
            context=context,
            stage_label=stage_label,
            scene_id=scene_id,
            batch_rows=batch_rows,
            batch_index=batch_index,
            batch_count=batch_count,
            stage_runner=stage_runner,
            reason=_summarize_retryable_stage_exception(exc),
            on_retry=on_retry,
        )
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
        return _retry_stage_batch_with_split(
            context=context,
            stage_label=stage_label,
            scene_id=scene_id,
            batch_rows=batch_rows,
            batch_index=batch_index,
            batch_count=batch_count,
            stage_runner=stage_runner,
            reason="mismatched segment ids",
            on_retry=on_retry,
        )


def _retry_stage_batch_with_split_positional(
    *,
    context: JobContext,
    stage_label: str,
    scene_id: str,
    batch_rows: list[Row],
    batch_index: int,
    batch_count: int,
    stage_runner: Callable[[list[Row], int, int], Any],
    reason: str,
    on_retry: Callable[[], None] | None = None,
    on_backoff: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    midpoint = max(1, len(batch_rows) // 2)
    left_rows = batch_rows[:midpoint]
    right_rows = batch_rows[midpoint:]
    context.logger.warning(
        "%s failed for scene=%s batch=%s/%s (%s). Retrying with smaller positional batches (%s + %s).",
        stage_label,
        scene_id,
        batch_index,
        batch_count,
        reason,
        len(left_rows),
        len(right_rows),
    )
    if on_retry is not None:
        on_retry()
    if on_backoff is not None:
        on_backoff(midpoint)
    merged_items = _run_stage_batch_with_positional_retry(
        context=context,
        stage_label=stage_label,
        scene_id=scene_id,
        batch_rows=left_rows,
        batch_index=batch_index,
        batch_count=batch_count,
        stage_runner=stage_runner,
        on_retry=on_retry,
        on_backoff=on_backoff,
    )
    if right_rows:
        merged_items.update(
            _run_stage_batch_with_positional_retry(
                context=context,
                stage_label=stage_label,
                scene_id=scene_id,
                batch_rows=right_rows,
                batch_index=batch_index,
                batch_count=batch_count,
                stage_runner=stage_runner,
                on_retry=on_retry,
                on_backoff=on_backoff,
            )
        )
    return merged_items


def _run_stage_batch_with_positional_retry(
    *,
    context: JobContext,
    stage_label: str,
    scene_id: str,
    batch_rows: list[Row],
    batch_index: int,
    batch_count: int,
    stage_runner: Callable[[list[Row], int, int], Any],
    on_retry: Callable[[], None] | None = None,
    on_backoff: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    context.cancellation_token.raise_if_canceled()
    same_batch_retry_used = False
    while True:
        try:
            output = stage_runner(batch_rows, batch_index, batch_count)
        except Exception as exc:
            if _is_retryable_stage_exception(exc) and not same_batch_retry_used:
                same_batch_retry_used = True
                if on_retry is not None:
                    on_retry()
                context.logger.warning(
                    "%s parse/schema issue for scene=%s batch=%s/%s (%s). Retrying same positional batch once.",
                    stage_label,
                    scene_id,
                    batch_index,
                    batch_count,
                    _summarize_retryable_stage_exception(exc),
                )
                continue
            if len(batch_rows) <= 1 or not _is_retryable_stage_exception(exc):
                raise
            return _retry_stage_batch_with_split_positional(
                context=context,
                stage_label=stage_label,
                scene_id=scene_id,
                batch_rows=batch_rows,
                batch_index=batch_index,
                batch_count=batch_count,
                stage_runner=stage_runner,
                reason=_summarize_retryable_stage_exception(exc),
                on_retry=on_retry,
                on_backoff=on_backoff,
            )
        actual_len = len(getattr(output, "items", []))
        expected_len = len(batch_rows)
        if actual_len == expected_len:
            return {str(row["segment_id"]): item for row, item in zip(batch_rows, output.items, strict=True)}
        mismatch_reason = f"positional length mismatch (expected={expected_len} actual={actual_len})"
        if not same_batch_retry_used:
            same_batch_retry_used = True
            if on_retry is not None:
                on_retry()
            context.logger.warning(
                "%s length mismatch for scene=%s batch=%s/%s (%s). Retrying same positional batch once.",
                stage_label,
                scene_id,
                batch_index,
                batch_count,
                mismatch_reason,
            )
            continue
        if len(batch_rows) <= 1:
            raise RuntimeError(
                f"{stage_label} positional output mismatch (scene={scene_id}, batch={batch_index}/{batch_count}): "
                f"{mismatch_reason}"
            )
        return _retry_stage_batch_with_split_positional(
            context=context,
            stage_label=stage_label,
            scene_id=scene_id,
            batch_rows=batch_rows,
            batch_index=batch_index,
            batch_count=batch_count,
            stage_runner=stage_runner,
            reason=mismatch_reason,
            on_retry=on_retry,
            on_backoff=on_backoff,
        )


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
    checkpoint_state: ContextualTranslationCheckpointState | None = None,
    checkpoint_writer: Callable[[list[SceneMemoryRecord], list[CharacterProfileRecord], list[RelationshipProfileRecord], list[SegmentAnalysisRecord], list[SceneRouteDecision], list[SceneTermEntitySheet], ContextualRunMetrics, list[str], int], None] | None = None,
) -> dict[str, object]:
    if is_narration_fast_v2_template(selected_template):
        from .narration_fast_v2 import run_contextual_translation_v2

        return run_contextual_translation_v2(
            context,
            workspace=workspace,
            database=database,
            engine=engine,
            segments=segments,
            selected_template=selected_template,
            source_language=source_language,
            target_language=target_language,
            model=model,
            checkpoint_state=checkpoint_state,
            checkpoint_writer=checkpoint_writer,
        )

    selected_prompt_family = resolve_prompt_family(workspace.root_dir, selected_template)
    profile_state = load_project_profile_state(workspace.root_dir)
    project_profile_id = profile_state.project_profile_id if profile_state is not None else None
    narration_routing_enabled = is_narration_fast_template(selected_template) or bool(
        project_profile_id and project_profile_id.startswith("zh-vi-narration-")
    )
    narration_prompt_family = resolve_prompt_family(
        workspace.root_dir,
        load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation"),
    )
    dialogue_prompt_family = (
        resolve_prompt_family(
            workspace.root_dir,
            load_prompt_template(workspace.root_dir, "contextual_default_adaptation"),
        )
        if narration_routing_enabled
        else selected_prompt_family
    )
    for family_name, family, required_roles in (
        ("selected", selected_prompt_family, {"semantic_pass", "dialogue_adaptation"}),
        ("narration", narration_prompt_family, {"semantic_pass", "dialogue_adaptation"}),
        ("dialogue", dialogue_prompt_family, {"scene_planner", "semantic_pass", "dialogue_adaptation"}),
    ):
        missing_roles = sorted(required_roles - set(family))
        if missing_roles:
            raise RuntimeError(f"Thiếu contextual prompt roles trong {family_name} family: {', '.join(missing_roles)}")

    scenes = chunk_segments_into_scenes(segments)
    now = _utc_now_iso()
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
    route_decisions: list[SceneRouteDecision] = list(checkpoint_state.route_decisions) if checkpoint_state else []
    metrics = checkpoint_state.metrics if checkpoint_state is not None else ContextualRunMetrics()
    narration_semantic_batch_cap = (
        int(metrics.narration_batch_size_caps.get("semantic_pass", NARRATION_FAST_BATCH_SIZE))
        if checkpoint_state is not None
        else NARRATION_FAST_BATCH_SIZE
    )
    narration_adaptation_batch_cap = (
        int(metrics.narration_batch_size_caps.get("dialogue_adaptation", NARRATION_FAST_BATCH_SIZE))
        if checkpoint_state is not None
        else NARRATION_FAST_BATCH_SIZE
    )
    term_entity_sheets: list[SceneTermEntitySheet] = (
        list(checkpoint_state.term_entity_sheets)
        if checkpoint_state is not None
        else []
    )
    metrics.narration_batch_size_caps = {
        "semantic_pass": narration_semantic_batch_cap,
        "dialogue_adaptation": narration_adaptation_batch_cap,
    }

    def _record_call(metric) -> None:
        metrics.llm_call_count += 1
        metrics.call_metrics.append(metric)

    def _count_retry() -> None:
        metrics.llm_retry_count += 1

    scene_records: list[SceneMemoryRecord] = list(checkpoint_state.scenes) if checkpoint_state is not None else []
    if checkpoint_state is not None:
        character_profiles = {item.character_id: item for item in checkpoint_state.character_profiles}
    else:
        character_profiles = {
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
    if checkpoint_state is not None:
        relationship_profiles = {item.relationship_id: item for item in checkpoint_state.relationship_profiles}
    else:
        relationship_profiles = {
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
    analyses: list[SegmentAnalysisRecord] = list(checkpoint_state.analyses) if checkpoint_state is not None else []
    completed_scene_ids = set(checkpoint_state.completed_scene_ids) if checkpoint_state is not None else set()

    total_scenes = max(1, len(scenes))
    for scene_position, scene in enumerate(scenes, start=1):
        if scene.scene_id in completed_scene_ids:
            continue
        if narration_routing_enabled:
            route_decision = _route_scene(
                scene,
                prior_analysis_rows=prior_analysis_rows,
                narration_family_id=narration_prompt_family["dialogue_adaptation"].family_id
                or narration_prompt_family["dialogue_adaptation"].template_id,
                dialogue_family_id=dialogue_prompt_family["dialogue_adaptation"].family_id
                or dialogue_prompt_family["dialogue_adaptation"].template_id,
                prefer_narration=is_narration_fast_template(selected_template),
            )
        else:
            route_mode = "narration_fast" if is_narration_fast_template(selected_template) else "dialogue"
            route_decision = SceneRouteDecision(
                scene_id=scene.scene_id,
                scene_index=scene.scene_index,
                route_mode=route_mode,
                prompt_family_id=selected_template.family_id or selected_template.template_id,
                narration_score=1.0 if route_mode == "narration_fast" else 0.0,
                speaker_dominance=0.6 if route_mode == "narration_fast" else 0.0,
                speaker_switch_density=0.5 if route_mode == "narration_fast" else 1.0,
                question_density=0.0,
                vocative_density=0.0,
                backchannel_density=0.0,
                short_utterance_ratio=0.0,
                long_sentence_ratio=0.0,
                prior_review_penalty=0.0,
                fallback_reason="",
            )
        route_decisions.append(route_decision)
        scene_route_mode = route_decision.route_mode
        scene_is_narration_fast = scene_route_mode == "narration_fast"
        active_prompt_family = narration_prompt_family if scene_is_narration_fast else dialogue_prompt_family
        context.report_progress(
            min(20, int(scene_position * 20 / total_scenes)),
            (
                f"Contextual V2 narration fast: scene {scene_position}/{total_scenes}"
                if scene_is_narration_fast
                else f"Contextual V2: scene {scene_position}/{total_scenes}"
            ),
        )
        planner_character_rows = [] if scene_is_narration_fast else list(character_profiles.values())
        planner_relationship_rows = [] if scene_is_narration_fast else list(relationship_profiles.values())
        if scene_is_narration_fast:
            planner_output = _build_narration_fast_scene_plan(scene)
        else:
            metrics.batch_count += 1
            planner_output = engine.plan_scene(
                context,
                template=active_prompt_family["scene_planner"],
                scene_payload=_scene_row_payload(scene),
                source_language=source_language,
                target_language=target_language,
                context_payload=_build_context_payload(
                    scene=scene,
                    existing_character_rows=planner_character_rows,
                    existing_relationship_rows=planner_relationship_rows,
                ),
                glossary_payload=_build_glossary_payload(
                    database,
                    workspace.project_id,
                    relationship_rows=planner_relationship_rows,
                ),
                model=model,
                prompt_cache_key=OpenAITranslationEngine.build_prompt_cache_key(
                    template=active_prompt_family["scene_planner"],
                    model=model,
                    source_language=source_language,
                    target_language=target_language,
                    route_mode=scene_route_mode,
                    project_profile_id=project_profile_id,
                ),
                record_call=_record_call,
                route_mode=scene_route_mode,
            )
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
        scene_context_character_rows = [] if scene_is_narration_fast else list(character_profiles.values())
        scene_context_relationship_rows = [] if scene_is_narration_fast else list(relationship_profiles.values())
        narration_term_sheet = SceneTermEntitySheet(
            scene_id=scene.scene_id,
            route_mode=scene_route_mode,
            active=False,
            items=[],
        )
        term_review_hints_by_segment: dict[str, list[str]] = {}
        if (
            scene_is_narration_fast
            and "term_entity_pass" in active_prompt_family
            and hasattr(engine, "extract_term_entities")
            and _should_run_narration_term_entity_pass(scene, route_decision)
        ):
            metrics.batch_count += 1
            metrics.term_entity_pass_scene_count += 1
            term_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                template=active_prompt_family["term_entity_pass"],
                model=model,
                source_language=source_language,
                target_language=target_language,
                route_mode=scene_route_mode,
                project_profile_id=project_profile_id,
            )
            term_output = engine.extract_term_entities(
                context,
                template=active_prompt_family["term_entity_pass"],
                scene_payload={
                    **_scene_row_payload(scene),
                    "scene_plan": planner_output.model_dump(mode="json", by_alias=True),
                },
                source_language=source_language,
                target_language=target_language,
                context_payload=_build_context_payload(
                    scene=scene,
                    planner_summary=planner_output.scene_summary,
                    existing_character_rows=scene_context_character_rows,
                    existing_relationship_rows=scene_context_relationship_rows,
                ),
                glossary_payload=_build_glossary_payload(
                    database,
                    workspace.project_id,
                    relationship_rows=scene_context_relationship_rows,
                ),
                model=model,
                prompt_cache_key=term_prompt_cache_key,
                record_call=_record_call,
                route_mode=scene_route_mode,
            )
            narration_term_sheet = _materialize_narration_term_sheet(scene, term_output)
            narration_term_sheet.active = True
            metrics.term_entity_entry_count += len(narration_term_sheet.items)
            term_entity_sheets.append(narration_term_sheet)
            for item in narration_term_sheet.items:
                if item.status != "needs_review":
                    continue
                metrics.term_entity_review_hint_count += 1
                for segment_id in item.segment_ids:
                    term_review_hints_by_segment.setdefault(segment_id, []).append(item.source_term)
        narration_term_sheet_payload = (
            [item.model_dump(mode="json") for item in narration_term_sheet.items]
            if narration_term_sheet.active
            else None
        )
        scene_batches = None
        if not scene_is_narration_fast:
            scene_batches = _chunk_scene_segments(
                scene.segments,
                batch_size=CONTEXTUAL_STAGE_BATCH_SIZE,
            )
        semantic_items: dict[str, object] = {}
        semantic_output_model = NarrationSemanticBatchOutput if scene_is_narration_fast else SemanticBatchOutput
        semantic_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
            template=active_prompt_family["semantic_pass"],
            model=model,
            source_language=source_language,
            target_language=target_language,
            route_mode=scene_route_mode,
            project_profile_id=project_profile_id,
        )
        semantic_batch_count = 0
        if scene_is_narration_fast:
            scene_semantic_batch_cap = narration_semantic_batch_cap

            def _downshift_semantic_batch_cap(recommended_size: int) -> None:
                nonlocal narration_semantic_batch_cap, scene_semantic_batch_cap
                reduced_size = max(1, min(scene_semantic_batch_cap, recommended_size))
                if reduced_size >= scene_semantic_batch_cap:
                    return
                context.logger.info(
                    "Narration semantic batch cap reduced for scene=%s from %s to %s after positional under-return.",
                    scene.scene_id,
                    scene_semantic_batch_cap,
                    reduced_size,
                )
                scene_semantic_batch_cap = reduced_size
                narration_semantic_batch_cap = min(narration_semantic_batch_cap, reduced_size)
                metrics.narration_batch_size_caps["semantic_pass"] = narration_semantic_batch_cap

            semantic_cursor = 0
            while semantic_cursor < len(scene.segments):
                current_batch_size = max(1, scene_semantic_batch_cap)
                batch_rows = scene.segments[semantic_cursor : semantic_cursor + current_batch_size]
                semantic_batch_count += 1
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
                            existing_character_rows=scene_context_character_rows,
                            existing_relationship_rows=scene_context_relationship_rows,
                        ),
                        glossary_payload=_build_glossary_payload(
                            database,
                            workspace.project_id,
                            relationship_rows=scene_context_relationship_rows,
                            narration_term_sheet=narration_term_sheet_payload,
                        ),
                        model=model,
                        output_model=semantic_output_model,
                        prompt_cache_key=semantic_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=scene_route_mode,
                    )

                semantic_items.update(
                    _run_stage_batch_with_positional_retry(
                        context=context,
                        stage_label="Semantic pass",
                        scene_id=scene.scene_id,
                        batch_rows=batch_rows,
                        batch_index=semantic_batch_count,
                        batch_count=max(1, math.ceil((len(scene.segments) - semantic_cursor) / current_batch_size)),
                        stage_runner=semantic_runner,
                        on_retry=_count_retry,
                        on_backoff=_downshift_semantic_batch_cap,
                    )
                )
                semantic_cursor += len(batch_rows)
        else:
            assert scene_batches is not None
            semantic_batch_count = len(scene_batches)
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
                            existing_character_rows=scene_context_character_rows,
                            existing_relationship_rows=scene_context_relationship_rows,
                        ),
                        glossary_payload=_build_glossary_payload(
                            database,
                            workspace.project_id,
                            relationship_rows=scene_context_relationship_rows,
                            narration_term_sheet=narration_term_sheet_payload,
                        ),
                        model=model,
                        output_model=semantic_output_model,
                        prompt_cache_key=semantic_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=scene_route_mode,
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
        _validate_stage_item_ids(
            "Semantic pass",
            expected_ids=list(scene.segment_ids),
            actual_ids=list(semantic_items),
            scene_id=scene.scene_id,
            batch_index=semantic_batch_count,
            batch_count=semantic_batch_count,
        )

        adaptation_items: dict[str, object] = {}
        adaptation_output_model = (
            NarrationAdaptationBatchOutput if scene_is_narration_fast else DialogueAdaptationBatchOutput
        )
        adaptation_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
            template=active_prompt_family["dialogue_adaptation"],
            model=model,
            source_language=source_language,
            target_language=target_language,
            route_mode=scene_route_mode,
            project_profile_id=project_profile_id,
        )
        adaptation_batch_count = 0
        if scene_is_narration_fast:
            scene_adaptation_batch_cap = narration_adaptation_batch_cap

            def _downshift_adaptation_batch_cap(recommended_size: int) -> None:
                nonlocal narration_adaptation_batch_cap, scene_adaptation_batch_cap
                reduced_size = max(1, min(scene_adaptation_batch_cap, recommended_size))
                if reduced_size >= scene_adaptation_batch_cap:
                    return
                context.logger.info(
                    "Narration adaptation batch cap reduced for scene=%s from %s to %s after positional under-return.",
                    scene.scene_id,
                    scene_adaptation_batch_cap,
                    reduced_size,
                )
                scene_adaptation_batch_cap = reduced_size
                narration_adaptation_batch_cap = min(narration_adaptation_batch_cap, reduced_size)
                metrics.narration_batch_size_caps["dialogue_adaptation"] = narration_adaptation_batch_cap

            adaptation_cursor = 0
            while adaptation_cursor < len(scene.segments):
                current_batch_size = max(1, scene_adaptation_batch_cap)
                batch_rows = scene.segments[adaptation_cursor : adaptation_cursor + current_batch_size]
                adaptation_batch_count += 1
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
                            existing_character_rows=scene_context_character_rows,
                            existing_relationship_rows=scene_context_relationship_rows,
                        ),
                        glossary_payload=_build_glossary_payload(
                            database,
                            workspace.project_id,
                            relationship_rows=scene_context_relationship_rows,
                            narration_term_sheet=narration_term_sheet_payload,
                        ),
                        model=model,
                        output_model=adaptation_output_model,
                        prompt_cache_key=adaptation_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=scene_route_mode,
                    )

                adaptation_items.update(
                    _run_stage_batch_with_positional_retry(
                        context=context,
                        stage_label="Dialogue adaptation",
                        scene_id=scene.scene_id,
                        batch_rows=batch_rows,
                        batch_index=adaptation_batch_count,
                        batch_count=max(1, math.ceil((len(scene.segments) - adaptation_cursor) / current_batch_size)),
                        stage_runner=adaptation_runner,
                        on_retry=_count_retry,
                        on_backoff=_downshift_adaptation_batch_cap,
                    )
                )
                adaptation_cursor += len(batch_rows)
        else:
            assert scene_batches is not None
            adaptation_batch_count = len(scene_batches)
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
                            existing_character_rows=scene_context_character_rows,
                            existing_relationship_rows=scene_context_relationship_rows,
                        ),
                        glossary_payload=_build_glossary_payload(
                            database,
                            workspace.project_id,
                            relationship_rows=scene_context_relationship_rows,
                            narration_term_sheet=narration_term_sheet_payload,
                        ),
                        model=model,
                        output_model=adaptation_output_model,
                        prompt_cache_key=adaptation_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=scene_route_mode,
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
        _validate_stage_item_ids(
            "Dialogue adaptation",
            expected_ids=list(scene.segment_ids),
            actual_ids=list(adaptation_items),
            scene_id=scene.scene_id,
            batch_index=adaptation_batch_count,
            batch_count=adaptation_batch_count,
        )

        critic_items: dict[str, object] = {}
        if "semantic_critic" in active_prompt_family and not scene_is_narration_fast:
            critic_prompt_cache_key = OpenAITranslationEngine.build_prompt_cache_key(
                template=active_prompt_family["semantic_critic"],
                model=model,
                source_language=source_language,
                target_language=target_language,
                route_mode=scene_route_mode,
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
                            existing_character_rows=scene_context_character_rows,
                            existing_relationship_rows=scene_context_relationship_rows,
                        ),
                        glossary_payload=_build_glossary_payload(
                            database,
                            workspace.project_id,
                            relationship_rows=scene_context_relationship_rows,
                        ),
                        model=model,
                        prompt_cache_key=critic_prompt_cache_key,
                        record_call=_record_call,
                        route_mode=scene_route_mode,
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
            term_review_hints = list(dict.fromkeys(term_review_hints_by_segment.get(segment_id, [])))
            for code in adaptation_item.review_reason_codes:
                if code not in review_reason_codes:
                    review_reason_codes.append(code)
            if critic_item:
                for code in critic_item.error_codes:
                    if code not in review_reason_codes:
                        review_reason_codes.append(code)
                critic_issues = [item.model_dump(mode="json", by_alias=True) for item in critic_item.issues]
            if term_review_hints and "technical_term_uncertainty" not in review_reason_codes:
                review_reason_codes.append("technical_term_uncertainty")
            review_reason_codes = [_normalize_review_reason_code(code) for code in review_reason_codes]
            review_reason_codes = list(dict.fromkeys(review_reason_codes))
            needs_review = (
                semantic_item.needs_human_review
                or adaptation_item.needs_human_review
                or bool(getattr(critic_item, "review_needed", False))
                or bool(term_review_hints)
            )
            merged_risk_flags = list(dict.fromkeys(list(semantic_item.risk_flags) + list(adaptation_item.risk_flags)))
            if term_review_hints and "term_entity_review_hint" not in merged_risk_flags:
                merged_risk_flags.append("term_entity_review_hint")
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
                    review_question=(
                        semantic_item.review_question
                        or (
                            f"Thuat ngu/thuc the chua du chac de chot: {', '.join(term_review_hints)}."
                            if term_review_hints
                            else ""
                        )
                    ),
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
        completed_scene_ids.add(scene.scene_id)
        if checkpoint_writer is not None:
            checkpoint_writer(
                scene_records,
                list(character_profiles.values()),
                list(relationship_profiles.values()),
                analyses,
                route_decisions,
                term_entity_sheets,
                metrics,
                sorted(completed_scene_ids),
                total_scenes,
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

    narration_scene_count = sum(1 for item in route_decisions if item.route_mode == "narration_fast")
    any_narration_fast = narration_scene_count > 0
    all_narration_fast = narration_scene_count == len(route_decisions) and bool(route_decisions)
    return {
        "scenes": scene_records,
        "character_profiles": list(character_profiles.values()),
        "relationship_profiles": list(relationship_profiles.values()),
        "segment_analyses": patched_analyses,
        "route_decisions": route_decisions,
        "term_entity_sheets": term_entity_sheets,
        "metrics": metrics,
        "fast_path": {
            "active": any_narration_fast,
            "mode": "narration_fast" if all_narration_fast else ("mixed" if any_narration_fast else "default"),
            "semantic_batch_size": (
                NARRATION_FAST_BATCH_SIZE if all_narration_fast else (None if any_narration_fast else CONTEXTUAL_STAGE_BATCH_SIZE)
            ),
            "adaptive_batch_caps": (
                dict(metrics.narration_batch_size_caps)
                if any_narration_fast
                else {}
            ),
            "planner_mode": "deterministic" if all_narration_fast else ("mixed" if any_narration_fast else "llm"),
            "critic_enabled": any(item.route_mode == "dialogue" for item in route_decisions),
            "route_counts": {
                "narration_fast": narration_scene_count,
                "dialogue": len(route_decisions) - narration_scene_count,
            },
            "term_entity_pass": {
                "scene_count": metrics.term_entity_pass_scene_count,
                "entry_count": metrics.term_entity_entry_count,
                "review_hint_count": metrics.term_entity_review_hint_count,
            },
        },
        "semantic_qc": {
            "error_count": qc_report.error_count,
            "warning_count": qc_report.warning_count,
            "total_segments": qc_report.total_segments,
        },
    }
