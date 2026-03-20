from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field

from .models import VoicePreset

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
class RegisterVoiceStyleCandidate:
    label: str
    segment_count: int
    politeness: str = ""
    power_direction: str = ""
    emotional_tone: str = ""
    turn_function: str = ""
    relation_type: str = ""


@dataclass(slots=True)
class VoicePolicyValue:
    voice_preset_id: str = ""
    speed_override: float | None = None
    volume_override: float | None = None
    pitch_override: float | None = None

    def has_style_override(self) -> bool:
        return (
            self.speed_override is not None
            or self.volume_override is not None
            or self.pitch_override is not None
        )


@dataclass(slots=True)
class SpeakerBindingPlan:
    active_bindings: bool
    active_voice_policies: bool = False
    active_register_voice_styles: bool = False
    segment_voice_preset_ids: dict[str, str] = field(default_factory=dict)
    segment_voice_style_overrides: dict[str, dict[str, float]] = field(default_factory=dict)
    segment_speaker_keys: dict[str, str] = field(default_factory=dict)
    unresolved_speakers: list[str] = field(default_factory=list)
    missing_preset_ids: list[str] = field(default_factory=list)
    character_policy_hits: int = 0
    relationship_policy_hits: int = 0
    character_style_hits: int = 0
    relationship_style_hits: int = 0
    register_style_hits: int = 0
    segment_voice_sources: dict[str, str] = field(default_factory=dict)
    segment_voice_style_sources: dict[str, str] = field(default_factory=dict)
    segment_voice_style_source_details: dict[str, dict[str, str]] = field(default_factory=dict)


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


def _normalize_policy_token(value: object | None) -> str:
    return str(value or "").strip().lower()


def _relationship_type_map(relationship_rows: list[object] | None) -> dict[tuple[str, str], str]:
    mapped: dict[tuple[str, str], str] = {}
    for row in relationship_rows or []:
        speaker_key = _normalize_speaker_key(_row_value(row, "from_character_id"))
        listener_key = _normalize_speaker_key(_row_value(row, "to_character_id"))
        if not speaker_key or not listener_key:
            continue
        relation_type = _normalize_policy_token(_row_value(row, "relation_type"))
        if relation_type:
            mapped[(speaker_key, listener_key)] = relation_type
    return mapped


def discover_register_voice_style_candidates(
    analysis_rows: list[object],
    *,
    relationship_rows: list[object] | None = None,
) -> list[RegisterVoiceStyleCandidate]:
    relationship_types = _relationship_type_map(relationship_rows)
    counts: Counter[tuple[str, str, str, str, str]] = Counter()
    for row in analysis_rows:
        register_json = _json_field(row, "register_json", {})
        if not isinstance(register_json, dict):
            register_json = {}
        speaker_json = _json_field(row, "speaker_json", {})
        speaker_key = _normalize_speaker_key(
            speaker_json.get("character_id") if isinstance(speaker_json, dict) else None
        )
        listener_key = _primary_listener_key(row)
        signature = (
            _normalize_policy_token(register_json.get("politeness")),
            _normalize_policy_token(register_json.get("power_direction")),
            _normalize_policy_token(register_json.get("emotional_tone")),
            _normalize_policy_token(_row_value(row, "turn_function")),
            relationship_types.get((speaker_key, listener_key), "") if speaker_key and listener_key else "",
        )
        if not any(signature):
            continue
        counts[signature] += 1

    candidates: list[RegisterVoiceStyleCandidate] = []
    for signature, segment_count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        politeness, power_direction, emotional_tone, turn_function, relation_type = signature
        label_parts = [
            f"lich_su={politeness}" if politeness else "",
            f"quyen_luc={power_direction}" if power_direction else "",
            f"cam_xuc={emotional_tone}" if emotional_tone else "",
            f"chuc_nang={turn_function}" if turn_function else "",
            f"quan_he={relation_type}" if relation_type else "",
        ]
        candidates.append(
            RegisterVoiceStyleCandidate(
                label=", ".join(part for part in label_parts if part),
                segment_count=segment_count,
                politeness=politeness,
                power_direction=power_direction,
                emotional_tone=emotional_tone,
                turn_function=turn_function,
                relation_type=relation_type,
            )
        )
    return candidates


def _voice_policy_maps(
    voice_policy_rows: list[object] | None,
) -> tuple[dict[str, VoicePolicyValue], dict[tuple[str, str], VoicePolicyValue]]:
    character_policy_map: dict[str, VoicePolicyValue] = {}
    relationship_policy_map: dict[tuple[str, str], VoicePolicyValue] = {}
    for row in voice_policy_rows or []:
        policy_scope = str(_row_value(row, "policy_scope", "character") or "character").strip() or "character"
        speaker_key = _normalize_speaker_key(_row_value(row, "speaker_character_id"))
        listener_key = _normalize_speaker_key(_row_value(row, "listener_character_id"))
        voice_preset_id = str(_row_value(row, "voice_preset_id", "") or "").strip()
        if not speaker_key:
            continue
        policy_value = VoicePolicyValue(
            voice_preset_id=voice_preset_id,
            speed_override=_normalize_optional_float(_row_value(row, "speed_override")),
            volume_override=_normalize_optional_float(_row_value(row, "volume_override")),
            pitch_override=_normalize_optional_float(_row_value(row, "pitch_override")),
        )
        if not policy_value.voice_preset_id and not policy_value.has_style_override():
            continue
        if policy_scope == "relationship" and listener_key:
            relationship_policy_map[(speaker_key, listener_key)] = policy_value
            continue
        character_policy_map[speaker_key] = policy_value
    return character_policy_map, relationship_policy_map


def _register_style_policy_rows(
    register_style_policy_rows: list[object] | None,
) -> list[tuple[dict[str, str], VoicePolicyValue, str]]:
    policies: list[tuple[dict[str, str], VoicePolicyValue, str]] = []
    for row in register_style_policy_rows or []:
        value = VoicePolicyValue(
            speed_override=_normalize_optional_float(_row_value(row, "speed_override")),
            volume_override=_normalize_optional_float(_row_value(row, "volume_override")),
            pitch_override=_normalize_optional_float(_row_value(row, "pitch_override")),
        )
        if not value.has_style_override():
            continue
        match_payload = {
            "politeness": _normalize_policy_token(_row_value(row, "politeness")),
            "power_direction": _normalize_policy_token(_row_value(row, "power_direction")),
            "emotional_tone": _normalize_policy_token(_row_value(row, "emotional_tone")),
            "turn_function": _normalize_policy_token(_row_value(row, "turn_function")),
            "relation_type": _normalize_policy_token(_row_value(row, "relation_type")),
        }
        if not any(match_payload.values()):
            continue
        policies.append((match_payload, value, str(_row_value(row, "policy_id", "") or "").strip()))
    return policies


def _normalize_optional_float(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _register_style_context(
    analysis_row: object,
    *,
    relation_type: str,
) -> dict[str, str]:
    register_json = _json_field(analysis_row, "register_json", {})
    if not isinstance(register_json, dict):
        register_json = {}
    return {
        "politeness": _normalize_policy_token(register_json.get("politeness")),
        "power_direction": _normalize_policy_token(register_json.get("power_direction")),
        "emotional_tone": _normalize_policy_token(register_json.get("emotional_tone")),
        "turn_function": _normalize_policy_token(_row_value(analysis_row, "turn_function")),
        "relation_type": _normalize_policy_token(relation_type),
    }


def _register_style_safe_to_apply(
    analysis_row: object,
    *,
    requires_relation_signal: bool,
) -> bool:
    if bool(_row_value(analysis_row, "needs_human_review", False)):
        return False
    risk_flags = {
        _normalize_policy_token(item)
        for item in _json_field(analysis_row, "risk_flags_json", [])
        if str(item or "").strip()
    }
    review_reason_codes = {
        _normalize_policy_token(item)
        for item in _json_field(analysis_row, "review_reason_codes_json", [])
        if str(item or "").strip()
    }
    if {"uncertain_speaker", "unclear_relationship"} & (risk_flags | review_reason_codes):
        return False
    confidence_json = _json_field(analysis_row, "confidence_json", {})
    if isinstance(confidence_json, dict):
        speaker_confidence = _normalize_optional_float(confidence_json.get("speaker"))
        relation_confidence = _normalize_optional_float(confidence_json.get("relation"))
        if speaker_confidence is not None and speaker_confidence < 0.65:
            return False
        if requires_relation_signal and relation_confidence is not None and relation_confidence < 0.65:
            return False
    return True


def _select_register_style_policy(
    analysis_row: object,
    *,
    relation_type: str,
    register_style_policies: list[tuple[dict[str, str], VoicePolicyValue, str]],
) -> VoicePolicyValue | None:
    if not register_style_policies:
        return None
    context = _register_style_context(analysis_row, relation_type=relation_type)
    matched_policy: VoicePolicyValue | None = None
    matched_specificity = -1
    matched_policy_id = ""
    for match_payload, value, policy_id in register_style_policies:
        if not all(not expected or context.get(field_name, "") == expected for field_name, expected in match_payload.items()):
            continue
        requires_relation_signal = bool(match_payload.get("relation_type") or match_payload.get("power_direction"))
        if not _register_style_safe_to_apply(
            analysis_row,
            requires_relation_signal=requires_relation_signal,
        ):
            continue
        specificity = sum(1 for expected in match_payload.values() if expected)
        if specificity > matched_specificity or (
            specificity == matched_specificity and policy_id < matched_policy_id
        ):
            matched_policy = value
            matched_specificity = specificity
            matched_policy_id = policy_id
    return matched_policy


def _merge_style_overrides(
    *,
    register_policy: VoicePolicyValue | None,
    character_policy: VoicePolicyValue | None,
    relationship_policy: VoicePolicyValue | None,
) -> tuple[dict[str, float], str | None, dict[str, str]]:
    override: dict[str, float] = {}
    source_details: dict[str, str] = {}
    source: str | None = None
    if register_policy is not None:
        if register_policy.speed_override is not None:
            override["speed"] = register_policy.speed_override
            source_details["speed"] = "register_policy"
        if register_policy.volume_override is not None:
            override["volume"] = register_policy.volume_override
            source_details["volume"] = "register_policy"
        if register_policy.pitch_override is not None:
            override["pitch"] = register_policy.pitch_override
            source_details["pitch"] = "register_policy"
    if character_policy is not None:
        if character_policy.speed_override is not None:
            override["speed"] = character_policy.speed_override
            source_details["speed"] = "character_policy"
        if character_policy.volume_override is not None:
            override["volume"] = character_policy.volume_override
            source_details["volume"] = "character_policy"
        if character_policy.pitch_override is not None:
            override["pitch"] = character_policy.pitch_override
            source_details["pitch"] = "character_policy"
    if relationship_policy is not None:
        if relationship_policy.speed_override is not None:
            override["speed"] = relationship_policy.speed_override
            source_details["speed"] = "relationship_policy"
        if relationship_policy.volume_override is not None:
            override["volume"] = relationship_policy.volume_override
            source_details["volume"] = "relationship_policy"
        if relationship_policy.pitch_override is not None:
            override["pitch"] = relationship_policy.pitch_override
            source_details["pitch"] = "relationship_policy"
    distinct_sources = [name for name in ("relationship_policy", "character_policy", "register_policy") if name in source_details.values()]
    if len(distinct_sources) == 1:
        source = distinct_sources[0]
    elif distinct_sources:
        source = "+".join(distinct_sources)
    return override, source, source_details


def build_speaker_binding_plan(
    *,
    subtitle_rows: list[object],
    analysis_rows: list[object],
    binding_rows: list[object],
    available_preset_ids: set[str],
    voice_policy_rows: list[object] | None = None,
    relationship_rows: list[object] | None = None,
    register_style_policy_rows: list[object] | None = None,
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
    register_style_policies = _register_style_policy_rows(register_style_policy_rows)
    relationship_types = _relationship_type_map(relationship_rows)
    plan = SpeakerBindingPlan(
        active_bindings=bool(binding_map),
        active_voice_policies=bool(character_policy_map or relationship_policy_map),
        active_register_voice_styles=bool(register_style_policies),
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
        character_policy = character_policy_map.get(speaker_key)
        listener_key = _primary_listener_key(analysis_row)
        relationship_policy = relationship_policy_map.get((speaker_key, listener_key)) if listener_key else None
        relation_type = relationship_types.get((speaker_key, listener_key), "") if listener_key else ""
        register_policy = _select_register_style_policy(
            analysis_row,
            relation_type=relation_type,
            register_style_policies=register_style_policies,
        )

        bound_preset_id = binding_map.get(("character", speaker_key))
        if bound_preset_id:
            if bound_preset_id not in available_preset_ids:
                missing_preset_ids.add(bound_preset_id)
                continue
            plan.segment_voice_preset_ids[segment_id] = bound_preset_id
            plan.segment_voice_sources[segment_id] = "speaker_binding"
        else:
            relationship_preset_id = relationship_policy.voice_preset_id if relationship_policy else ""
            if relationship_preset_id:
                if relationship_preset_id not in available_preset_ids:
                    missing_preset_ids.add(relationship_preset_id)
                    continue
                plan.segment_voice_preset_ids[segment_id] = relationship_preset_id
                plan.segment_voice_sources[segment_id] = "relationship_policy"
                plan.relationship_policy_hits += 1
            else:
                character_preset_id = character_policy.voice_preset_id if character_policy else ""
                if character_preset_id:
                    if character_preset_id not in available_preset_ids:
                        missing_preset_ids.add(character_preset_id)
                        continue
                    plan.segment_voice_preset_ids[segment_id] = character_preset_id
                    plan.segment_voice_sources[segment_id] = "character_policy"
                    plan.character_policy_hits += 1
                elif plan.active_bindings:
                    unresolved_speakers.add(speaker_key)

        style_overrides, style_source, style_source_details = _merge_style_overrides(
            register_policy=register_policy,
            character_policy=character_policy,
            relationship_policy=relationship_policy,
        )
        if style_overrides:
            plan.segment_voice_style_overrides[segment_id] = style_overrides
            if style_source:
                plan.segment_voice_style_sources[segment_id] = style_source
            if style_source_details:
                plan.segment_voice_style_source_details[segment_id] = style_source_details
            if "relationship_policy" in style_source_details.values():
                plan.relationship_style_hits += 1
            if "character_policy" in style_source_details.values():
                plan.character_style_hits += 1
            if "register_policy" in style_source_details.values():
                plan.register_style_hits += 1

    plan.unresolved_speakers = sorted(unresolved_speakers)
    plan.missing_preset_ids = sorted(missing_preset_ids)
    return plan


def resolve_segment_voice_presets(
    *,
    plan: SpeakerBindingPlan,
    default_preset: VoicePreset,
    available_presets: dict[str, VoicePreset],
) -> dict[str, VoicePreset]:
    segment_voice_presets: dict[str, VoicePreset] = {}
    affected_segment_ids = set(plan.segment_voice_preset_ids) | set(plan.segment_voice_style_overrides)
    for segment_id in affected_segment_ids:
        base_preset_id = plan.segment_voice_preset_ids.get(segment_id, default_preset.voice_preset_id)
        base_preset = available_presets.get(base_preset_id, default_preset)
        style_override = plan.segment_voice_style_overrides.get(segment_id, {})
        if not style_override:
            segment_voice_presets[segment_id] = base_preset
            continue
        update_payload = {
            key: style_override[key]
            for key in ("speed", "volume", "pitch")
            if key in style_override
        }
        segment_voice_presets[segment_id] = base_preset.model_copy(update=update_payload)
    return segment_voice_presets
