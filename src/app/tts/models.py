from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pydantic import BaseModel, Field


class VoicePreset(BaseModel):
    voice_preset_id: str
    name: str
    engine: str
    voice_id: str = "default"
    speed: float = 1.0
    volume: float = 1.0
    pitch: float = 0.0
    sample_rate: int = 22050
    language: str | None = None
    engine_options: dict[str, object] = Field(default_factory=dict)
    notes: str = ""


@dataclass(slots=True)
class SynthesisResult:
    wav_path: Path
    duration_ms: int
    sample_rate: int
    voice_id: str | None = None


@dataclass(slots=True)
class SynthesizedSegmentArtifact:
    segment_id: str
    segment_index: int
    start_ms: int
    end_ms: int
    text: str
    raw_wav_path: Path
    duration_ms: int
    sample_rate: int
    voice_id: str | None = None
    voice_preset_id: str | None = None
    speaker_key: str | None = None
    voice_speed: float | None = None
    voice_volume: float | None = None
    voice_pitch: float | None = None
    fitted_wav_path: Path | None = None
    fitted_duration_ms: int | None = None


@dataclass(slots=True)
class SynthesizedSegmentsResult:
    stage_hash: str
    cache_dir: Path
    manifest_path: Path
    artifacts: list[SynthesizedSegmentArtifact] = field(default_factory=list)
