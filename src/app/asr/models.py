from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class WordTimestamp:
    start_ms: int
    end_ms: int
    text: str
    probability: float | None = None


@dataclass(slots=True)
class SegmentDraft:
    segment_index: int
    start_ms: int
    end_ms: int
    source_text: str
    language: str | None = None
    words: list[WordTimestamp] = field(default_factory=list)


@dataclass(slots=True)
class TranscriptionOptions:
    model_name: str
    language: str | None = None
    vad_filter: bool = True
    word_timestamps: bool = True
    compute_type: str | None = None


@dataclass(slots=True)
class TranscriptionResult:
    source_audio_path: Path
    detected_language: str | None
    duration_ms: int | None
    segments: list[SegmentDraft]


@dataclass(slots=True)
class PersistedTranscription:
    stage_hash: str
    cache_dir: Path
    segments_json_path: Path
    segment_count: int

