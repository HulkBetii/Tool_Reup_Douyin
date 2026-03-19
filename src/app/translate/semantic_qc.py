from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


DEFAULT_CONFIDENCE_THRESHOLD = 0.65

COMMON_VI_PRONOUNS = {
    "anh",
    "em",
    "tôi",
    "toi",
    "ta",
    "tao",
    "mày",
    "may",
    "cậu",
    "cau",
    "bạn",
    "ban",
    "con",
    "mẹ",
    "me",
    "cha",
    "bố",
    "bo",
    "ông",
    "ong",
    "bà",
    "ba",
    "cô",
    "co",
    "chú",
    "chu",
    "dì",
    "di",
    "thầy",
    "thay",
    "sư phụ",
    "sư huynh",
    "sư tỷ",
}


@dataclass(slots=True, frozen=True)
class SemanticQcIssue:
    segment_id: str
    segment_index: int
    code: str
    severity: str
    message: str


@dataclass(slots=True, frozen=True)
class SemanticQcReport:
    total_segments: int
    issues: list[SemanticQcIssue]

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")


def _row_value(row: object, field: str, default: object = None) -> object:
    if isinstance(row, dict):
        return row.get(field, default)
    try:
        return row[field]  # type: ignore[index]
    except Exception:
        return getattr(row, field, default)


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().replace("\n", " ").split())


def _contains_term(text: str, term: str) -> bool:
    normalized_text = f" {_normalize_text(text)} "
    normalized_term = f" {_normalize_text(term)} "
    return bool(term and normalized_term in normalized_text)


def _extract_pronoun_terms(text: str) -> set[str]:
    normalized = f" {_normalize_text(text)} "
    return {term for term in COMMON_VI_PRONOUNS if f" {term} " in normalized}


def _pair_key(row: object) -> tuple[str, str]:
    speaker = _row_value(row, "speaker_json", {}) or {}
    listeners = _row_value(row, "listeners_json", []) or []
    speaker_id = str((speaker or {}).get("character_id", "unknown"))
    primary_listener = "unknown"
    if isinstance(listeners, list) and listeners:
        primary_listener = str((listeners[0] or {}).get("character_id", "unknown"))
    return speaker_id, primary_listener


def analyze_segment_analyses(
    rows: Iterable[object],
    *,
    relationship_defaults: dict[tuple[str, str], dict[str, object]] | None = None,
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
) -> SemanticQcReport:
    normalized_rows = list(rows)
    issues: list[SemanticQcIssue] = []
    previous_policy_by_pair: dict[tuple[str, str, str], tuple[str, str]] = {}
    relationship_map = relationship_defaults or {}

    for row in normalized_rows:
        segment_id = str(_row_value(row, "segment_id", ""))
        segment_index = int(_row_value(row, "segment_index", 0))
        scene_id = str(_row_value(row, "scene_id", ""))
        confidence = _row_value(row, "confidence_json", {}) or {}
        speaker = _row_value(row, "speaker_json", {}) or {}
        honorific_policy = _row_value(row, "honorific_policy_json", {}) or {}
        risk_flags = set(_row_value(row, "risk_flags_json", []) or [])
        resolved_ellipsis = _row_value(row, "resolved_ellipsis_json", {}) or {}
        subtitle_text = str(_row_value(row, "approved_subtitle_text", "") or "")
        tts_text = str(_row_value(row, "approved_tts_text", "") or "")

        speaker_confidence = float(confidence.get("speaker", speaker.get("confidence", 0.0)) or 0.0)
        listener_confidence = float(confidence.get("listener", 0.0) or 0.0)
        relation_confidence = float(confidence.get("relation", honorific_policy.get("confidence", 0.0)) or 0.0)
        overall_confidence = float(confidence.get("overall", 0.0) or 0.0)
        ellipsis_confidence = float(resolved_ellipsis.get("confidence", 0.0) or 0.0)

        self_term = str(honorific_policy.get("self_term", "") or "").strip()
        address_term = str(honorific_policy.get("address_term", "") or "").strip()

        if overall_confidence < confidence_threshold:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="low_confidence_gate",
                    severity="error",
                    message="Confidence tổng thể thấp, cần review trước khi TTS/export.",
                )
            )

        if (self_term or address_term) and (listener_confidence < 0.5 or "listener_ambiguous" in risk_flags):
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="addressee_mismatch",
                    severity="warning",
                    message="Đã chèn xưng hô nhưng người nghe còn mơ hồ.",
                )
            )

        if (self_term or address_term) and min(speaker_confidence, max(listener_confidence, ellipsis_confidence)) < 0.55:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="pronoun_without_evidence",
                    severity="warning",
                    message="Đã chèn ngôi xưng hô khi bằng chứng discourse còn yếu.",
                )
            )

        subtitle_pronouns = _extract_pronoun_terms(subtitle_text)
        tts_pronouns = _extract_pronoun_terms(tts_text)
        if subtitle_pronouns and tts_pronouns and subtitle_pronouns != tts_pronouns:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="sub_tts_pronoun_divergence",
                    severity="error",
                    message="Phụ đề và lời TTS đang dùng ngôi xưng hô khác nhau.",
                )
            )
        elif self_term and address_term:
            sub_has_policy = _contains_term(subtitle_text, self_term) or _contains_term(subtitle_text, address_term)
            tts_has_policy = _contains_term(tts_text, self_term) or _contains_term(tts_text, address_term)
            if sub_has_policy != tts_has_policy:
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="sub_tts_pronoun_divergence",
                        severity="warning",
                        message="Phụ đề và lời TTS chưa bám cùng policy xưng hô.",
                    )
                )

        pair = _pair_key(row)
        if pair[0] != "unknown" or pair[1] != "unknown":
            pair_key = (scene_id, pair[0], pair[1])
            policy_signature = (self_term, address_term)
            previous_signature = previous_policy_by_pair.get(pair_key)
            if previous_signature and previous_signature != policy_signature and self_term and address_term:
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="honorific_drift",
                        severity="error",
                        message="Cặp speaker/listener này đang trôi xưng hô trong cùng scene.",
                    )
                )
            previous_policy_by_pair[pair_key] = policy_signature

        relation_defaults = relationship_map.get(pair, {})
        expected_self_term = str(relation_defaults.get("default_self_term", "") or "")
        expected_address_term = str(relation_defaults.get("default_address_term", "") or "")
        allowed_alternates = set(relation_defaults.get("allowed_alternates_json", []) or [])
        if relation_confidence >= 0.7 and (expected_self_term or expected_address_term):
            if expected_self_term and self_term and self_term != expected_self_term and self_term not in allowed_alternates:
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="directionality_mismatch",
                        severity="warning",
                        message="Xưng hô tự xưng không khớp relation memory đã khóa/xác nhận.",
                    )
                )
            if (
                expected_address_term
                and address_term
                and address_term != expected_address_term
                and address_term not in allowed_alternates
            ):
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="directionality_mismatch",
                        severity="warning",
                        message="Xưng hô gọi người nghe không khớp relation memory đã khóa/xác nhận.",
                    )
                )

    return SemanticQcReport(total_segments=len(normalized_rows), issues=issues)
