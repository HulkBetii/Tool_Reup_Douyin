from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class AudioStreamInfo:
    index: int
    codec_name: str | None
    channels: int | None
    sample_rate: int | None
    language: str | None = None


@dataclass(slots=True)
class VideoStreamInfo:
    index: int
    codec_name: str | None
    width: int | None
    height: int | None
    fps: float | None


@dataclass(slots=True)
class MediaMetadata:
    source_path: Path
    duration_ms: int | None
    size_bytes: int | None
    format_name: str | None
    bit_rate: int | None
    video_stream: VideoStreamInfo | None
    audio_streams: list[AudioStreamInfo] = field(default_factory=list)
    sha256: str | None = None

    @property
    def primary_audio_stream(self) -> AudioStreamInfo | None:
        return self.audio_streams[0] if self.audio_streams else None

    @property
    def fps(self) -> float | None:
        return self.video_stream.fps if self.video_stream else None

    @property
    def width(self) -> int | None:
        return self.video_stream.width if self.video_stream else None

    @property
    def height(self) -> int | None:
        return self.video_stream.height if self.video_stream else None


@dataclass(slots=True)
class ExtractedAudioArtifacts:
    stage_hash: str
    cache_dir: Path
    audio_16k_path: Path
    audio_48k_path: Path
    manifest_path: Path

