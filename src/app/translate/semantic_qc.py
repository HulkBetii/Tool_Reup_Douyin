from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from .relationship_memory import split_allowed_alternates_by_side


DEFAULT_CONFIDENCE_THRESHOLD = 0.65
LOCKED_RELATION_STATUSES = {"locked_by_human", "confirmed", "locked"}

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
    "su phu",
    "sư huynh",
    "su huynh",
    "sư tỷ",
    "su ty",
    "chúng ta",
    "chung ta",
    "chúng tôi",
    "chung toi",
    "quý vị",
    "quy vi",
    "quý khách",
    "quy khach",
    "các bạn",
    "cac ban",
    "mọi người",
    "moi nguoi",
    "khán giả",
    "khan gia",
}
GENERIC_AUDIENCE_IDS = {
    "audience",
    "general humanity",
    "general audience",
    "viewer",
    "viewers",
    "public",
    "nguoi xem",
    "khan gia",
}
GENERIC_AUDIENCE_ROLES = {"audience"}


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
    # Normalize punctuation boundaries so "em," still counts as a vocative/pronoun.
    normalized = re.sub(r"[^\w\s]", " ", (text or "").strip().lower().replace("\n", " "), flags=re.UNICODE)
    return " ".join(normalized.split())


def _contains_term(text: str, term: str) -> bool:
    normalized_text = f" {_normalize_text(text)} "
    normalized_term = f" {_normalize_text(term)} "
    return bool(term and normalized_term in normalized_text)


def _extract_pronoun_terms(text: str) -> set[str]:
    normalized = f" {_normalize_text(text)} "
    return {term for term in COMMON_VI_PRONOUNS if f" {term} " in normalized}


def _primary_listener_payload(row: object) -> dict[str, object]:
    listeners = _row_value(row, "listeners_json", []) or []
    if isinstance(listeners, list) and listeners:
        primary_listener = listeners[0] or {}
        if isinstance(primary_listener, dict):
            return primary_listener
    return {}


def _pair_key(row: object) -> tuple[str, str]:
    speaker = _row_value(row, "speaker_json", {}) or {}
    primary_listener = _primary_listener_payload(row)
    speaker_id = str((speaker or {}).get("character_id", "unknown"))
    listener_id = str(primary_listener.get("character_id", "unknown") or "unknown")
    return speaker_id, listener_id


def _is_generic_audience_listener(listener: dict[str, object]) -> bool:
    if not listener:
        return False
    listener_role = _normalize_text(str(listener.get("role", "") or ""))
    listener_id = _normalize_text(str(listener.get("character_id", "") or ""))
    return listener_role in GENERIC_AUDIENCE_ROLES or listener_id in GENERIC_AUDIENCE_IDS


def _is_narration_like(row: object) -> bool:
    speaker = _row_value(row, "speaker_json", {}) or {}
    speaker_source = _normalize_text(str((speaker or {}).get("source", "") or ""))
    if speaker_source == "narration":
        return True
    return _is_generic_audience_listener(_primary_listener_payload(row))


def _effective_honorific_terms(
    row: object,
    *,
    honorific_policy: dict[str, object],
    subtitle_text: str,
    tts_text: str,
) -> tuple[str, str]:
    self_term = str(honorific_policy.get("self_term", "") or "").strip()
    address_term = str(honorific_policy.get("address_term", "") or "").strip()
    if not (self_term or address_term):
        return self_term, address_term
    if not _is_narration_like(row):
        return self_term, address_term
    subtitle_has_policy = (self_term and _contains_term(subtitle_text, self_term)) or (
        address_term and _contains_term(subtitle_text, address_term)
    )
    tts_has_policy = (self_term and _contains_term(tts_text, self_term)) or (
        address_term and _contains_term(tts_text, address_term)
    )
    if subtitle_has_policy or tts_has_policy:
        return self_term, address_term
    return "", ""


def _relation_status_is_locked(status: object) -> bool:
    return str(status or "").strip().lower() in LOCKED_RELATION_STATUSES


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

        narration_like = _is_narration_like(row)
        self_term, address_term = _effective_honorific_terms(
            row,
            honorific_policy=honorific_policy,
            subtitle_text=subtitle_text,
            tts_text=tts_text,
        )
        weak_listener_evidence = listener_confidence < 0.5 or "listener_ambiguous" in risk_flags
        weak_discourse_evidence = min(speaker_confidence, max(listener_confidence, ellipsis_confidence)) < 0.55

        if overall_confidence < confidence_threshold:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="low_confidence_gate",
                    severity="error",
                    message="Confidence tong the thap, can review truoc khi TTS/export.",
                )
            )

        if (self_term or address_term) and weak_listener_evidence:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="addressee_mismatch",
                    severity="warning",
                    message="Da chen xung ho nhung nguoi nghe con mo ho.",
                )
            )

        if (self_term or address_term) and weak_discourse_evidence:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="pronoun_without_evidence",
                    severity="warning",
                    message="Da chen ngoi xung ho khi bang chung discourse con yeu.",
                )
            )

        subtitle_pronouns = _extract_pronoun_terms(subtitle_text)
        tts_pronouns = _extract_pronoun_terms(tts_text)
        if narration_like and subtitle_pronouns != tts_pronouns and (subtitle_pronouns or tts_pronouns):
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="sub_tts_pronoun_divergence",
                    severity="error",
                    message="Thuyet minh dang lech cach goi khan gia/xung ho giua subtitle va TTS.",
                )
            )
        elif subtitle_pronouns and tts_pronouns and subtitle_pronouns != tts_pronouns:
            issues.append(
                SemanticQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="sub_tts_pronoun_divergence",
                    severity="error",
                    message="Phu de va loi TTS dang dung ngoi xung ho khac nhau.",
                )
            )
        elif self_term and address_term:
            sub_has_policy = _contains_term(subtitle_text, self_term) or _contains_term(subtitle_text, address_term)
            tts_has_policy = _contains_term(tts_text, self_term) or _contains_term(tts_text, address_term)
            if sub_has_policy != tts_has_policy:
                unsafe_tts_only_injection = tts_has_policy and not sub_has_policy and (
                    weak_listener_evidence or weak_discourse_evidence or narration_like
                )
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="sub_tts_pronoun_divergence",
                        severity="error" if unsafe_tts_only_injection else "warning",
                        message=(
                            "Loi TTS dang tu them xung ho khi bang chung nguoi nghe/discourse con yeu."
                            if unsafe_tts_only_injection
                            else "Phu de va loi TTS chua bam cung policy xung ho."
                        ),
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
                        message="Cap speaker/listener nay dang troi xung ho trong cung scene.",
                    )
                )
            previous_policy_by_pair[pair_key] = policy_signature

        relation_defaults = relationship_map.get(pair, {})
        expected_self_term = str(relation_defaults.get("default_self_term", "") or "")
        expected_address_term = str(relation_defaults.get("default_address_term", "") or "")
        allowed_self_alternates, allowed_address_alternates = split_allowed_alternates_by_side(
            relation_defaults.get("allowed_alternates_json", [])
        )
        relation_status_locked = _relation_status_is_locked(relation_defaults.get("status"))
        if relation_confidence >= 0.7 and (expected_self_term or expected_address_term):
            if (
                expected_self_term
                and self_term
                and self_term != expected_self_term
                and self_term not in allowed_self_alternates
            ):
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="directionality_mismatch",
                        severity="error" if relation_status_locked else "warning",
                        message=(
                            "Xung ho tu xung khong khop relation memory da khoa/xac nhan."
                            if relation_status_locked
                            else "Xung ho tu xung khong khop relation memory hien co."
                        ),
                    )
                )
            if (
                expected_address_term
                and address_term
                and address_term != expected_address_term
                and address_term not in allowed_address_alternates
            ):
                issues.append(
                    SemanticQcIssue(
                        segment_id=segment_id,
                        segment_index=segment_index,
                        code="directionality_mismatch",
                        severity="error" if relation_status_locked else "warning",
                        message=(
                            "Xung ho goi nguoi nghe khong khop relation memory da khoa/xac nhan."
                            if relation_status_locked
                            else "Xung ho goi nguoi nghe khong khop relation memory hien co."
                        ),
                    )
                )

    return SemanticQcReport(total_segments=len(normalized_rows), issues=issues)
