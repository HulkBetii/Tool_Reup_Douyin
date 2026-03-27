from __future__ import annotations

import re
from uuid import NAMESPACE_URL, uuid5

from app.project.models import SegmentRecord, SubtitleEventRecord

TIMECODE_PATTERN = re.compile(r"^(?P<hours>\d{1,2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})[.,](?P<millis>\d{3})$")
_TTS_WHITESPACE_PATTERN = re.compile(r"\s+")
_TTS_PUNCT_SPACING_PATTERN = re.compile(r"\s+([,.;:?!])")
_NARRATION_AUDIENCE_PATTERN = r"(?:các bạn|cac ban|mọi người|moi nguoi|quý vị|quy vi|bạn|ban)"
_NARRATION_LEADING_AUDIENCE_QUESTION_PATTERN = re.compile(
    rf"^(?:{_NARRATION_AUDIENCE_PATTERN})\b[^?!.]{{0,120}}\?\s+(?P<rest>.+)$",
    re.IGNORECASE,
)
_NARRATION_LEADING_VOCATIVE_PATTERN = re.compile(
    rf"^(?:{_NARRATION_AUDIENCE_PATTERN})(?:\s+(?:ơi|à|này))?\s*[,.:!…-]*\s*",
    re.IGNORECASE,
)
_NARRATION_PREFIX_CHUNGTA_PATTERN = re.compile(
    r"^(?P<prefix>giờ|gio|bây giờ|bay gio|hôm nay|hom nay|lúc này|luc nay)\s+"
    r"(?:chúng ta|chung ta)\s+",
    re.IGNORECASE,
)
_NARRATION_CHUNGTA_IMPERATIVE_PATTERN = re.compile(
    r"^(?:chúng ta|chung ta)\s+(?:hãy\s+|hay\s+|cùng\s+|cung\s+)",
    re.IGNORECASE,
)
_NARRATION_TRAILING_AUDIENCE_QUESTION_PATTERN = re.compile(
    rf"(?:,\s*)?(?:{_NARRATION_AUDIENCE_PATTERN}\s+(?:thấy\s+|thay\s+)?)?"
    r"(?:có\s+|co\s+)?(?:đúng không|dung khong)\s*[.?!…]*$",
    re.IGNORECASE,
)
_NARRATION_TRAILING_CONFIRMATION_PATTERN = re.compile(
    r"(?:,\s*)?(?:phải không|phai khong|nhỉ|nhi)\s*[.?!…]*$",
    re.IGNORECASE,
)
_NARRATION_TRAILING_AUDIENCE_PATTERN = re.compile(
    rf"(?:,\s*)?(?:{_NARRATION_AUDIENCE_PATTERN})\s*[.?!…]*$",
    re.IGNORECASE,
)
_NARRATION_TRAILING_SOFTENER_PATTERN = re.compile(
    r"(?:,\s*)?(?:nhé|nhe|nhỉ|nhi|nha|ha)\s*[.?!…]*$",
    re.IGNORECASE,
)
_NARRATION_TRAILING_PUNCT_PATTERN = re.compile(r"[\s,;:!?.…-]+$")


def format_timestamp_ms(total_ms: int) -> str:
    if total_ms < 0:
        raise ValueError("Timestamp khong duoc am")

    hours, remainder = divmod(total_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def parse_timestamp_ms(value: str) -> int:
    candidate = value.strip()
    match = TIMECODE_PATTERN.match(candidate)
    if not match:
        raise ValueError(f"Timestamp khong hop le: {value}")

    hours = int(match.group("hours"))
    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))
    millis = int(match.group("millis"))
    if minutes >= 60 or seconds >= 60:
        raise ValueError(f"Timestamp khong hop le: {value}")
    return (((hours * 60) + minutes) * 60 + seconds) * 1_000 + millis


def suggest_subtitle_text(translated_text: str, source_text: str) -> str:
    candidate = translated_text.strip()
    if candidate:
        return candidate
    return source_text.strip()


def normalize_tts_text(value: str) -> str:
    candidate = value.replace("\r\n", "\n").replace("\r", "\n")
    candidate = candidate.replace("\n", " ")
    candidate = candidate.replace("/", ", ")
    candidate = candidate.replace("|", ", ")
    candidate = _TTS_WHITESPACE_PATTERN.sub(" ", candidate).strip()
    candidate = _TTS_PUNCT_SPACING_PATTERN.sub(r"\1", candidate)
    return candidate


def suggest_tts_text(
    subtitle_text: str,
    translated_text: str,
    source_text: str,
    *,
    existing_tts_text: str = "",
) -> str:
    for candidate in (existing_tts_text, subtitle_text, translated_text, source_text):
        normalized = normalize_tts_text(candidate)
        if normalized:
            return normalized
    return ""


def _capitalize_sentence_start(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    first = candidate[0]
    if first.isalpha():
        return first.upper() + candidate[1:]
    return candidate


def neutralize_narration_review_text(value: str) -> str:
    candidate = normalize_tts_text(value)
    if not candidate:
        return ""

    original = candidate
    changed = False
    match = _NARRATION_LEADING_AUDIENCE_QUESTION_PATTERN.match(candidate)
    if match:
        candidate = normalize_tts_text(match.group("rest"))
        changed = True

    updated = _NARRATION_LEADING_VOCATIVE_PATTERN.sub("", candidate)
    if updated != candidate:
        candidate = normalize_tts_text(updated)
        changed = True

    updated = _NARRATION_PREFIX_CHUNGTA_PATTERN.sub(lambda item: f"{item.group('prefix')} ", candidate)
    if updated != candidate:
        candidate = normalize_tts_text(updated)
        changed = True

    updated = _NARRATION_CHUNGTA_IMPERATIVE_PATTERN.sub("", candidate)
    if updated != candidate:
        candidate = normalize_tts_text(updated)
        changed = True

    for pattern in (
        _NARRATION_TRAILING_AUDIENCE_QUESTION_PATTERN,
        _NARRATION_TRAILING_CONFIRMATION_PATTERN,
        _NARRATION_TRAILING_AUDIENCE_PATTERN,
        _NARRATION_TRAILING_SOFTENER_PATTERN,
    ):
        updated = pattern.sub("", candidate)
        if updated != candidate:
            candidate = normalize_tts_text(updated)
            changed = True

    candidate = _NARRATION_TRAILING_PUNCT_PATTERN.sub("", candidate).strip()
    if not candidate:
        return original

    candidate = _capitalize_sentence_start(candidate)
    if changed and candidate[-1] not in ".!?…":
        candidate = f"{candidate}."
    return candidate


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def _split_text_pair(value: str) -> tuple[str, str]:
    text = value.strip()
    if not text:
        return "", ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 1:
        midpoint = max(1, len(lines) // 2)
        first = "\n".join(lines[:midpoint]).strip()
        second = "\n".join(lines[midpoint:]).strip()
        if first and second:
            return first, second

    words = text.split()
    if len(words) >= 2:
        midpoint = max(1, len(words) // 2)
        return " ".join(words[:midpoint]).strip(), " ".join(words[midpoint:]).strip()

    midpoint = max(1, len(text) // 2)
    return text[:midpoint].strip(), text[midpoint:].strip()


def _join_text_pair(first: str, second: str, *, multiline: bool = False) -> str:
    parts = [value.strip() for value in (first, second) if value.strip()]
    if not parts:
        return ""
    separator = "\n" if multiline else " "
    return separator.join(parts)


def _derived_segment_id(seed: str, *, operation: str, start_ms: int, end_ms: int, text: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"{seed}:{operation}:{start_ms}:{end_ms}:{text}"))


def split_editor_row(row: dict[str, object], *, split_at_ms: int | None = None) -> tuple[dict[str, object], dict[str, object]]:
    start_ms = int(row["start_ms"])
    end_ms = int(row["end_ms"])
    if end_ms - start_ms < 2:
        raise ValueError("Khong the split segment co duration qua ngan")

    split_ms = split_at_ms if split_at_ms is not None else start_ms + ((end_ms - start_ms) // 2)
    if split_ms <= start_ms or split_ms >= end_ms:
        raise ValueError("Moc split phai nam giua start_ms va end_ms")

    source_first, source_second = _split_text_pair(str(row.get("source_text", "") or ""))
    translated_first, translated_second = _split_text_pair(str(row.get("translated_text", "") or ""))
    subtitle_first, subtitle_second = _split_text_pair(str(row.get("subtitle_text", "") or ""))
    tts_first, tts_second = _split_text_pair(str(row.get("tts_text", "") or ""))
    seed = str(row.get("segment_id") or row.get("segment_index") or "segment")

    base_payload = {
        "source_lang": row.get("source_lang"),
        "target_lang": row.get("target_lang"),
        "source_segment_id": row.get("source_segment_id"),
        "audio_path": None,
        "status": "edited",
        "meta_json": {},
    }
    first = {
        **base_payload,
        "segment_id": _derived_segment_id(seed, operation="split-a", start_ms=start_ms, end_ms=split_ms, text=subtitle_first or translated_first or source_first),
        "start_ms": start_ms,
        "end_ms": split_ms,
        "source_text": source_first,
        "translated_text": translated_first,
        "subtitle_text": subtitle_first or translated_first or source_first,
        "tts_text": tts_first or subtitle_first or translated_first or source_first,
    }
    second = {
        **base_payload,
        "segment_id": _derived_segment_id(seed, operation="split-b", start_ms=split_ms, end_ms=end_ms, text=subtitle_second or translated_second or source_second),
        "start_ms": split_ms,
        "end_ms": end_ms,
        "source_text": source_second,
        "translated_text": translated_second,
        "subtitle_text": subtitle_second or translated_second or source_second,
        "tts_text": tts_second or subtitle_second or translated_second or source_second,
    }
    return first, second


def merge_editor_rows(first: dict[str, object], second: dict[str, object]) -> dict[str, object]:
    start_ms = min(int(first["start_ms"]), int(second["start_ms"]))
    end_ms = max(int(first["end_ms"]), int(second["end_ms"]))
    seed = f"{first.get('segment_id', 'first')}:{second.get('segment_id', 'second')}"
    source_text = _join_text_pair(str(first.get("source_text", "") or ""), str(second.get("source_text", "") or ""))
    translated_text = _join_text_pair(
        str(first.get("translated_text", "") or ""),
        str(second.get("translated_text", "") or ""),
    )
    subtitle_text = _join_text_pair(
        str(first.get("subtitle_text", "") or ""),
        str(second.get("subtitle_text", "") or ""),
        multiline=True,
    )
    tts_text = _join_text_pair(str(first.get("tts_text", "") or ""), str(second.get("tts_text", "") or ""))
    source_segment_id = first.get("source_segment_id")
    if source_segment_id != second.get("source_segment_id"):
        source_segment_id = None
    return {
        "segment_id": _derived_segment_id(seed, operation="merge", start_ms=start_ms, end_ms=end_ms, text=subtitle_text or translated_text or source_text),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "source_segment_id": source_segment_id,
        "source_lang": first.get("source_lang") or second.get("source_lang"),
        "target_lang": first.get("target_lang") or second.get("target_lang"),
        "source_text": source_text,
        "translated_text": translated_text,
        "subtitle_text": subtitle_text or translated_text or source_text,
        "tts_text": tts_text or subtitle_text or translated_text or source_text,
        "audio_path": None,
        "status": "edited",
        "meta_json": {},
    }


def build_subtitle_event_records(
    project_id: str,
    track_id: str,
    rows: list[dict[str, object]],
) -> list[SubtitleEventRecord]:
    records: list[SubtitleEventRecord] = []
    for index, row in enumerate(rows):
        source_text = str(row.get("source_text", "") or "")
        translated_text = str(row.get("translated_text", "") or "")
        subtitle_text = str(row.get("subtitle_text", "") or "") or translated_text or source_text
        tts_text = str(row.get("tts_text", "") or "") or subtitle_text or translated_text or source_text
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        if end_ms <= start_ms:
            raise ValueError(f"Segment {index} co duration khong hop le")

        row_track_id = str(row.get("track_id") or track_id)
        existing_segment_id = row.get("segment_id")
        event_id = str(
            existing_segment_id
            if existing_segment_id is not None and row_track_id == track_id
            else _derived_segment_id(
                track_id,
                operation=f"track-{index}",
                start_ms=start_ms,
                end_ms=end_ms,
                text=subtitle_text or translated_text or source_text,
            )
        )
        records.append(
            SubtitleEventRecord(
                event_id=event_id,
                track_id=track_id,
                project_id=project_id,
                event_index=index,
                start_ms=start_ms,
                end_ms=end_ms,
                source_segment_id=(
                    str(row.get("source_segment_id"))
                    if row.get("source_segment_id") is not None
                    else None
                ),
                source_lang=str(row.get("source_lang")) if row.get("source_lang") is not None else None,
                target_lang=str(row.get("target_lang")) if row.get("target_lang") is not None else None,
                source_text=source_text,
                source_text_norm=normalize_text(source_text),
                translated_text=translated_text,
                translated_text_norm=normalize_text(translated_text),
                subtitle_text=subtitle_text,
                tts_text=tts_text,
                audio_path=str(row.get("audio_path")) if row.get("audio_path") is not None else None,
                status=str(row.get("status") or "edited"),
                meta_json=dict(row.get("meta_json") or {}),
            )
        )
    return records


def build_segment_records(project_id: str, rows: list[dict[str, object]]) -> list[SegmentRecord]:
    records: list[SegmentRecord] = []
    for index, row in enumerate(rows):
        source_text = str(row.get("source_text", "") or "")
        translated_text = str(row.get("translated_text", "") or "")
        subtitle_text = str(row.get("subtitle_text", "") or "") or translated_text or source_text
        tts_text = str(row.get("tts_text", "") or "") or subtitle_text or translated_text or source_text
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        if end_ms <= start_ms:
            raise ValueError(f"Segment {index} co duration khong hop le")

        segment_id = str(
            row.get("segment_id")
            or _derived_segment_id(
                project_id,
                operation=f"track-{index}",
                start_ms=start_ms,
                end_ms=end_ms,
                text=subtitle_text or translated_text or source_text,
            )
        )
        records.append(
            SegmentRecord(
                segment_id=segment_id,
                project_id=project_id,
                segment_index=index,
                start_ms=start_ms,
                end_ms=end_ms,
                source_lang=str(row.get("source_lang")) if row.get("source_lang") is not None else None,
                target_lang=str(row.get("target_lang")) if row.get("target_lang") is not None else None,
                source_text=source_text,
                source_text_norm=normalize_text(source_text),
                translated_text=translated_text,
                translated_text_norm=normalize_text(translated_text),
                subtitle_text=subtitle_text,
                tts_text=tts_text,
                audio_path=str(row.get("audio_path")) if row.get("audio_path") is not None else None,
                status=str(row.get("status") or "edited"),
                meta_json=dict(row.get("meta_json") or {}),
            )
        )
    return records
