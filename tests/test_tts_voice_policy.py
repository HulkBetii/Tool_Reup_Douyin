from __future__ import annotations

import json
from pathlib import Path

from app.tts.speaker_binding import (
    build_speaker_binding_plan,
    discover_relationship_voice_policy_candidates,
)


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_build_speaker_binding_plan_applies_character_voice_policy_fallback() -> None:
    fixture = _load_regression_fixture("zh-vi-character-voice-policy-fallback.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        voice_policy_rows=list(fixture["voice_policy_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is False
    assert plan.active_voice_policies is True
    assert plan.segment_voice_preset_ids == {"evt-1": "voice-a"}
    assert plan.segment_voice_sources == {"evt-1": "character_policy"}
    assert plan.segment_speaker_keys == {"evt-1": "char_a", "evt-2": "char_c"}
    assert plan.character_policy_hits == 1
    assert plan.relationship_policy_hits == 0
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []


def test_build_speaker_binding_plan_relationship_policy_overrides_character_policy() -> None:
    fixture = _load_regression_fixture("zh-vi-relationship-voice-policy-overrides-character.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        voice_policy_rows=list(fixture["voice_policy_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.segment_voice_preset_ids == {"evt-1": "voice-rel"}
    assert plan.segment_voice_sources == {"evt-1": "relationship_policy"}
    assert plan.character_policy_hits == 0
    assert plan.relationship_policy_hits == 1
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []


def test_build_speaker_binding_plan_explicit_binding_wins_over_voice_policies() -> None:
    fixture = _load_regression_fixture("zh-vi-explicit-speaker-binding-wins-over-policy.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        voice_policy_rows=list(fixture["voice_policy_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is True
    assert plan.active_voice_policies is True
    assert plan.segment_voice_preset_ids == {"evt-1": "voice-bind"}
    assert plan.segment_voice_sources == {"evt-1": "speaker_binding"}
    assert plan.character_policy_hits == 0
    assert plan.relationship_policy_hits == 0
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []


def test_build_speaker_binding_plan_blocks_when_selected_voice_policy_preset_is_missing() -> None:
    fixture = _load_regression_fixture("zh-vi-voice-policy-missing-preset-blocks.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        voice_policy_rows=list(fixture["voice_policy_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is False
    assert plan.active_voice_policies is True
    assert plan.segment_voice_preset_ids == {}
    assert plan.segment_voice_sources == {}
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == ["voice-missing"]


def test_discover_relationship_voice_policy_candidates_uses_analysis_and_known_relations() -> None:
    candidates = discover_relationship_voice_policy_candidates(
        [
            {
                "segment_id": "seg-1",
                "speaker_json": {"character_id": "char_a"},
                "listeners_json": [{"character_id": "char_b"}],
            }
        ],
        relationship_rows=[
            {
                "relationship_id": "rel:char_c->char_d",
                "from_character_id": "char_c",
                "to_character_id": "char_d",
            }
        ],
        character_name_map={"char_a": "A", "char_b": "B"},
    )

    assert [(item.speaker_key, item.listener_key, item.segment_count) for item in candidates] == [
        ("char_a", "char_b", 1),
        ("char_c", "char_d", 0),
    ]
    assert candidates[0].label == "A (char_a) -> B (char_b)"
