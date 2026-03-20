from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
_UNKNOWN_SPEAKER_RE = re.compile(r"^unknown(?:[\s_-]*speaker)?(?:[\s_-]*\d+)?$", re.IGNORECASE)


def _row_value(row: object, key: str, default: object = None) -> object:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return getattr(row, key, default)


def _json_field(row: object, key: str, default: object) -> object:
    raw_value = _row_value(row, key, default)
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _normalize_speaker_key(value: object | None) -> str | None:
    text = str(value or "").strip()
    if not text or _UNKNOWN_SPEAKER_RE.match(text):
        return None
    return text


def _canonical_segment_id_for_row(row: object) -> str:
    source_segment_id = _normalize_speaker_key(_row_value(row, "source_segment_id"))
    if source_segment_id:
        return source_segment_id
    return str(_row_value(row, "segment_id", "") or "").strip()


@dataclass(slots=True)
class SpeakerCandidate:
    speaker_type: str
    speaker_key: str
    label: str
    segment_count: int


@dataclass(slots=True)
class SpeakerBindingPlan:
    active_bindings: bool
    segment_voice_preset_ids: dict[str, str] = field(default_factory=dict)
    segment_speaker_keys: dict[str, str] = field(default_factory=dict)
    unresolved_speakers: list[str] = field(default_factory=list)
    missing_preset_ids: list[str] = field(default_factory=list)


def discover_speaker_candidates(
    analysis_rows: list[object],
    *,
    character_name_map: dict[str, str] | None = None,
) -> list[SpeakerCandidate]:
    counts: Counter[str] = Counter()
    for row in analysis_rows:
        speaker_json = _json_field(row, "speaker_json", {})
        if not isinstance(speaker_json, dict):
            continue
        speaker_key = _normalize_speaker_key(speaker_json.get("character_id"))
        if speaker_key:
            counts[speaker_key] += 1

    candidates: list[SpeakerCandidate] = []
    for speaker_key, segment_count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        display_name = character_name_map.get(speaker_key, "") if character_name_map else ""
        label = f"{display_name} ({speaker_key})" if display_name and display_name != speaker_key else speaker_key
        candidates.append(
            SpeakerCandidate(
                speaker_type="character",
                speaker_key=speaker_key,
                label=label,
                segment_count=segment_count,
            )
        )
    return candidates


def build_speaker_binding_plan(
    *,
    subtitle_rows: list[object],
    analysis_rows: list[object],
    binding_rows: list[object],
    available_preset_ids: set[str],
) -> SpeakerBindingPlan:
    analysis_by_segment_id: dict[str, object] = {
        str(_row_value(row, "segment_id", "") or "").strip(): row
        for row in analysis_rows
    }
    binding_map: dict[tuple[str, str], str] = {}
    for row in binding_rows:
        speaker_type = str(_row_value(row, "speaker_type", "character") or "character").strip() or "character"
        speaker_key = _normalize_speaker_key(_row_value(row, "speaker_key"))
        voice_preset_id = str(_row_value(row, "voice_preset_id", "") or "").strip()
        if not speaker_key or not voice_preset_id:
            continue
        binding_map[(speaker_type, speaker_key)] = voice_preset_id

    plan = SpeakerBindingPlan(active_bindings=bool(binding_map))
    unresolved_speakers: set[str] = set()
    missing_preset_ids: set[str] = set()

    for row in subtitle_rows:
        segment_id = str(_row_value(row, "segment_id", "") or "").strip()
        if not segment_id:
            continue
        canonical_segment_id = _canonical_segment_id_for_row(row)
        analysis_row = analysis_by_segment_id.get(canonical_segment_id)
        if analysis_row is None:
            continue
        speaker_json = _json_field(analysis_row, "speaker_json", {})
        if not isinstance(speaker_json, dict):
            continue
        speaker_key = _normalize_speaker_key(speaker_json.get("character_id"))
        if not speaker_key:
            continue
        plan.segment_speaker_keys[segment_id] = speaker_key
        bound_preset_id = binding_map.get(("character", speaker_key))
        if not plan.active_bindings:
            continue
        if not bound_preset_id:
            unresolved_speakers.add(speaker_key)
            continue
        if bound_preset_id not in available_preset_ids:
            missing_preset_ids.add(bound_preset_id)
            continue
        plan.segment_voice_preset_ids[segment_id] = bound_preset_id

    plan.unresolved_speakers = sorted(unresolved_speakers)
    plan.missing_preset_ids = sorted(missing_preset_ids)
    return plan
