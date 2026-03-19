from __future__ import annotations

from app.translate.semantic_qc import analyze_segment_analyses


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
