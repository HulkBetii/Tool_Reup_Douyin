from __future__ import annotations

import json
from pathlib import Path

from app.tts.speaker_binding import build_speaker_binding_plan, discover_speaker_candidates


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_build_speaker_binding_plan_blocks_partial_config_when_bindings_exist() -> None:
    fixture = _load_regression_fixture("zh-vi-speaker-binding-partial-config-blocks-tts.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is True
    assert plan.segment_voice_preset_ids == {"evt-1": "voice-a"}
    assert plan.segment_speaker_keys == {"evt-1": "char_a", "evt-2": "char_b"}
    assert plan.unresolved_speakers == ["char_b"]
    assert plan.missing_preset_ids == []


def test_build_speaker_binding_plan_leaves_unknown_speaker_on_global_fallback() -> None:
    plan = build_speaker_binding_plan(
        subtitle_rows=[
            {
                "segment_id": "evt-1",
                "source_segment_id": "seg-1",
                "tts_text": "Di thoi.",
            }
        ],
        analysis_rows=[
            {
                "segment_id": "seg-1",
                "speaker_json": {"character_id": "unknown", "confidence": 0.3},
            }
        ],
        binding_rows=[
            {
                "binding_id": "bind:character:char_a",
                "project_id": "project-1",
                "speaker_type": "character",
                "speaker_key": "char_a",
                "voice_preset_id": "voice-a",
            }
        ],
        available_preset_ids={"voice-a", "default-sapi"},
    )

    assert plan.active_bindings is True
    assert plan.segment_voice_preset_ids == {}
    assert plan.segment_speaker_keys == {}
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []


def test_build_speaker_binding_plan_treats_unknown_placeholder_ids_as_global_fallback() -> None:
    fixture = _load_regression_fixture("zh-vi-speaker-binding-unknown-placeholder-falls-back.json")

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=list(fixture["analysis_rows"]),
        binding_rows=list(fixture["binding_rows"]),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is True
    assert plan.segment_voice_preset_ids == {}
    assert plan.segment_speaker_keys == {}
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []


def test_discover_speaker_candidates_prefers_character_names_when_available() -> None:
    candidates = discover_speaker_candidates(
        [
            {"segment_id": "seg-1", "speaker_json": {"character_id": "char_a"}},
            {"segment_id": "seg-2", "speaker_json": {"character_id": "char_a"}},
            {"segment_id": "seg-3", "speaker_json": {"character_id": "char_b"}},
        ],
        character_name_map={"char_a": "Nhan vat A"},
    )

    assert [(item.speaker_key, item.label, item.segment_count) for item in candidates] == [
        ("char_a", "Nhan vat A (char_a)", 2),
        ("char_b", "char_b", 1),
    ]


def test_discover_speaker_candidates_skips_unknown_placeholder_variants() -> None:
    candidates = discover_speaker_candidates(
        [
            {"segment_id": "seg-1", "speaker_json": {"character_id": "unknown_speaker"}},
            {"segment_id": "seg-2", "speaker_json": {"character_id": "Unknown Speaker 2"}},
            {"segment_id": "seg-3", "speaker_json": {"character_id": "char_a"}},
        ]
    )

    assert [(item.speaker_key, item.segment_count) for item in candidates] == [("char_a", 1)]
