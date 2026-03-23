from __future__ import annotations

import json
from pathlib import Path

from app.translate.semantic_qc import analyze_segment_analyses


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _load_golden_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "golden" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_semantic_qc_flags_honorific_drift_and_pronoun_divergence() -> None:
    report = analyze_segment_analyses(
        [
            {
                "segment_id": "seg-1",
                "segment_index": 0,
                "scene_id": "scene_0000",
                "speaker_json": {"character_id": "char_a", "confidence": 0.9},
                "listeners_json": [{"character_id": "char_b", "confidence": 0.9}],
                "honorific_policy_json": {"self_term": "anh", "address_term": "em", "confidence": 0.9},
                "confidence_json": {"overall": 0.9, "speaker": 0.9, "listener": 0.9, "relation": 0.9},
                "resolved_ellipsis_json": {"confidence": 0.9},
                "risk_flags_json": [],
                "approved_subtitle_text": "Anh đi trước nhé em.",
                "approved_tts_text": "Anh đi trước nhé em.",
            },
            {
                "segment_id": "seg-2",
                "segment_index": 1,
                "scene_id": "scene_0000",
                "speaker_json": {"character_id": "char_a", "confidence": 0.9},
                "listeners_json": [{"character_id": "char_b", "confidence": 0.9}],
                "honorific_policy_json": {"self_term": "tôi", "address_term": "bạn", "confidence": 0.9},
                "confidence_json": {"overall": 0.9, "speaker": 0.9, "listener": 0.9, "relation": 0.9},
                "resolved_ellipsis_json": {"confidence": 0.9},
                "risk_flags_json": [],
                "approved_subtitle_text": "Tôi nói rồi đó bạn.",
                "approved_tts_text": "Anh nói rồi đó em.",
            },
        ]
    )

    codes = {issue.code for issue in report.issues}
    assert "honorific_drift" in codes
    assert "sub_tts_pronoun_divergence" in codes


def test_semantic_qc_flags_low_confidence_gate() -> None:
    report = analyze_segment_analyses(
        [
            {
                "segment_id": "seg-1",
                "segment_index": 0,
                "scene_id": "scene_0000",
                "speaker_json": {"character_id": "char_a", "confidence": 0.4},
                "listeners_json": [{"character_id": "char_b", "confidence": 0.4}],
                "honorific_policy_json": {"self_term": "anh", "address_term": "em", "confidence": 0.4},
                "confidence_json": {"overall": 0.4, "speaker": 0.4, "listener": 0.4, "relation": 0.4},
                "resolved_ellipsis_json": {"confidence": 0.4},
                "risk_flags_json": ["listener_ambiguous"],
                "approved_subtitle_text": "Đi thôi.",
                "approved_tts_text": "Đi thôi em.",
            }
        ]
    )

    codes = {issue.code for issue in report.issues}
    assert "low_confidence_gate" in codes
    assert "addressee_mismatch" in codes


def test_semantic_qc_escalates_tts_only_pronoun_injection_under_ambiguous_listener() -> None:
    fixture = _load_regression_fixture("zh-vi-tts-pronoun-injection-ambiguous-listener.json")

    report = analyze_segment_analyses(fixture["segments"])

    by_code = {}
    for issue in report.issues:
        by_code.setdefault(issue.code, set()).add(issue.severity)

    assert "sub_tts_pronoun_divergence" in by_code
    assert "addressee_mismatch" in by_code
    assert "pronoun_without_evidence" in by_code
    assert "error" in by_code["sub_tts_pronoun_divergence"]
    assert report.error_count >= 1


def test_semantic_qc_blocks_directionality_mismatch_against_locked_relation_memory() -> None:
    fixture = _load_regression_fixture("zh-vi-locked-relation-directionality-mismatch.json")
    relationship_defaults = {
        ("char_a", "char_b"): dict(fixture["relationship_defaults"]["char_a->char_b"])
    }

    report = analyze_segment_analyses(fixture["segments"], relationship_defaults=relationship_defaults)

    by_code = {}
    for issue in report.issues:
        by_code.setdefault(issue.code, set()).add(issue.severity)

    assert "directionality_mismatch" in by_code
    assert "error" in by_code["directionality_mismatch"]


def test_semantic_qc_allows_whitelisted_alternates_under_locked_relation() -> None:
    fixture = _load_golden_fixture("zh-vi-locked-relation-allowed-alternates-safe.json")
    relationship_defaults = {
        ("char_a", "char_b"): dict(fixture["relationship_defaults"]["char_a->char_b"])
    }

    report = analyze_segment_analyses(fixture["segments"], relationship_defaults=relationship_defaults)

    codes = {issue.code for issue in report.issues}
    assert "directionality_mismatch" not in codes
    assert report.error_count == 0


def test_semantic_qc_allows_self_only_alternate_without_relaxing_full_relation() -> None:
    fixture = _load_regression_fixture("zh-vi-side-specific-alternates-self-safe.json")
    relationship_defaults = {
        ("char_a", "char_b"): dict(fixture["relationship_defaults"]["char_a->char_b"])
    }

    report = analyze_segment_analyses(fixture["segments"], relationship_defaults=relationship_defaults)

    codes = {issue.code for issue in report.issues}
    assert "directionality_mismatch" not in codes
    assert report.error_count == 0


def test_semantic_qc_does_not_apply_address_only_alternate_to_self_side() -> None:
    fixture = _load_golden_fixture("zh-vi-side-specific-alternates-address-does-not-relax-self.json")
    relationship_defaults = {
        ("char_a", "char_b"): dict(fixture["relationship_defaults"]["char_a->char_b"])
    }

    report = analyze_segment_analyses(fixture["segments"], relationship_defaults=relationship_defaults)

    by_code = {}
    for issue in report.issues:
        by_code.setdefault(issue.code, set()).add(issue.severity)

    assert "directionality_mismatch" in by_code
    assert "error" in by_code["directionality_mismatch"]


def test_semantic_qc_ignores_stale_narration_policy_when_text_is_neutral() -> None:
    fixture = _load_golden_fixture("zh-vi-narration-stale-honorific-policy-safe.json")

    report = analyze_segment_analyses(fixture["segments"])

    codes = {issue.code for issue in report.issues}
    assert report.error_count == 0
    assert report.warning_count == 0
    assert "honorific_drift" not in codes
    assert "addressee_mismatch" not in codes
    assert "pronoun_without_evidence" not in codes


def test_semantic_qc_blocks_narration_tts_only_audience_address_injection() -> None:
    fixture = _load_regression_fixture("zh-vi-narration-audience-address-injection.json")

    report = analyze_segment_analyses(fixture["segments"])

    by_code = {}
    for issue in report.issues:
        by_code.setdefault(issue.code, set()).add(issue.severity)

    assert "sub_tts_pronoun_divergence" in by_code
    assert "error" in by_code["sub_tts_pronoun_divergence"]
    assert report.error_count >= 1
