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


def _primary_listener_key(row: object) -> str | None:
    listeners_json = _json_field(row, "listeners_json", [])
    if not isinstance(listeners_json, list) or not listeners_json:
        return None
    primary_listener = listeners_json[0]
    if not isinstance(primary_listener, dict):
        return None
    return _normalize_speaker_key(primary_listener.get("character_id"))


def _format_character_label(character_key: str, character_name_map: dict[str, str] | None = None) -> str:
    display_name = character_name_map.get(character_key, "") if character_name_map else ""
    if display_name and display_name != character_key:
        return f"{display_name} ({character_key})"
    return character_key


@dataclass(slots=True)
class SpeakerCandidate:
    speaker_type: str
    speaker_key: str
    label: str
    segment_count: int


@dataclass(slots=True)
class RelationshipVoicePolicyCandidate:
    speaker_key: str
    listener_key: str
    label: str
    segment_count: int


@dataclass(slots=True)
class SpeakerBindingPlan:
    active_bindings: bool
    active_voice_policies: bool = False
    segment_voice_preset_ids: dict[str, str] = field(default_factory=dict)
    segment_speaker_keys: dict[str, str] = field(default_factory=dict)
    unresolved_speakers: list[str] = field(default_factory=list)
    missing_preset_ids: list[str] = field(default_factory=list)
    character_policy_hits: int = 0
    relationship_policy_hits: int = 0
    segment_voice_sources: dict[str, str] = field(default_factory=dict)


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
        candidates.append(
            SpeakerCandidate(
                speaker_type="character",
                speaker_key=speaker_key,
                label=_format_character_label(speaker_key, character_name_map),
                segment_count=segment_count,
            )
        )
    return candidates


def discover_relationship_voice_policy_candidates(
    analysis_rows: list[object],
    *,
    relationship_rows: list[object] | None = None,
    character_name_map: dict[str, str] | None = None,
) -> list[RelationshipVoicePolicyCandidate]:
    counts: Counter[tuple[str, str]] = Counter()
    for row in analysis_rows:
        speaker_json = _json_field(row, "speaker_json", {})
        if not isinstance(speaker_json, dict):
            continue
        speaker_key = _normalize_speaker_key(speaker_json.get("character_id"))
        listener_key = _primary_listener_key(row)
        if speaker_key and listener_key:
            counts[(speaker_key, listener_key)] += 1

    for row in relationship_rows or []:
        speaker_key = _normalize_speaker_key(_row_value(row, "from_character_id"))
        listener_key = _normalize_speaker_key(_row_value(row, "to_character_id"))
        if speaker_key and listener_key:
            counts.setdefault((speaker_key, listener_key), 0)

    candidates: list[RelationshipVoicePolicyCandidate] = []
    for (speaker_key, listener_key), segment_count in sorted(
        counts.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1]),
    ):
        candidates.append(
            RelationshipVoicePolicyCandidate(
                speaker_key=speaker_key,
                listener_key=listener_key,
                label=(
                    f"{_format_character_label(speaker_key, character_name_map)} -> "
                    f"{_format_character_label(listener_key, character_name_map)}"
                ),
                segment_count=segment_count,
            )
        )
    return candidates


def _voice_policy_maps(
    voice_policy_rows: list[object] | None,
) -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    character_policy_map: dict[str, str] = {}
    relationship_policy_map: dict[tuple[str, str], str] = {}
    for row in voice_policy_rows or []:
        policy_scope = str(_row_value(row, "policy_scope", "character") or "character").strip() or "character"
        speaker_key = _normalize_speaker_key(_row_value(row, "speaker_character_id"))
        listener_key = _normalize_speaker_key(_row_value(row, "listener_character_id"))
        voice_preset_id = str(_row_value(row, "voice_preset_id", "") or "").strip()
        if not speaker_key or not voice_preset_id:
            continue
        if policy_scope == "relationship" and listener_key:
            relationship_policy_map[(speaker_key, listener_key)] = voice_preset_id
            continue
        character_policy_map[speaker_key] = voice_preset_id
    return character_policy_map, relationship_policy_map


def build_speaker_binding_plan(
    *,
    subtitle_rows: list[object],
    analysis_rows: list[object],
    binding_rows: list[object],
    available_preset_ids: set[str],
    voice_policy_rows: list[object] | None = None,
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

    character_policy_map, relationship_policy_map = _voice_policy_maps(voice_policy_rows)
    plan = SpeakerBindingPlan(
        active_bindings=bool(binding_map),
        active_voice_policies=bool(character_policy_map or relationship_policy_map),
    )
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
        if bound_preset_id:
            if bound_preset_id not in available_preset_ids:
                missing_preset_ids.add(bound_preset_id)
                continue
            plan.segment_voice_preset_ids[segment_id] = bound_preset_id
            plan.segment_voice_sources[segment_id] = "speaker_binding"
            continue

        listener_key = _primary_listener_key(analysis_row)
        relationship_preset_id = relationship_policy_map.get((speaker_key, listener_key)) if listener_key else None
        if relationship_preset_id:
            if relationship_preset_id not in available_preset_ids:
                missing_preset_ids.add(relationship_preset_id)
                continue
            plan.segment_voice_preset_ids[segment_id] = relationship_preset_id
            plan.segment_voice_sources[segment_id] = "relationship_policy"
            plan.relationship_policy_hits += 1
            continue

        character_preset_id = character_policy_map.get(speaker_key)
        if character_preset_id:
            if character_preset_id not in available_preset_ids:
                missing_preset_ids.add(character_preset_id)
                continue
            plan.segment_voice_preset_ids[segment_id] = character_preset_id
            plan.segment_voice_sources[segment_id] = "character_policy"
            plan.character_policy_hits += 1
            continue

        if plan.active_bindings:
            unresolved_speakers.add(speaker_key)

    plan.unresolved_speakers = sorted(unresolved_speakers)
    plan.missing_preset_ids = sorted(missing_preset_ids)
    return plan
