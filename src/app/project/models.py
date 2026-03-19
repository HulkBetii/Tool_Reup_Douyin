from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass(slots=True)
class ProjectInitRequest:
    name: str
    root_dir: Path
    source_language: str = "auto"
    target_language: str = "vi"
    source_video_path: Path | None = None


@dataclass(slots=True)
class ProjectRecord:
    project_id: str
    name: str
    root_dir: str
    source_language: str
    target_language: str
    created_at: str
    updated_at: str
    video_asset_id: str | None = None
    active_subtitle_track_id: str | None = None
    active_voice_preset_id: str | None = None
    active_export_preset_id: str | None = None
    active_watermark_profile_id: str | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class MediaAssetRecord:
    asset_id: str
    project_id: str
    asset_type: str
    path: str
    created_at: str
    sha256: str | None = None
    duration_ms: int | None = None
    fps: float | None = None
    width: int | None = None
    height: int | None = None
    audio_channels: int | None = None
    sample_rate: int | None = None


@dataclass(slots=True)
class JobRunRecord:
    job_id: str
    project_id: str | None
    stage: str
    description: str
    status: str
    started_at: str
    progress: int = 0
    input_hash: str = ""
    output_paths: list[str] = field(default_factory=list)
    log_path: str | None = None
    error_json: dict[str, object] = field(default_factory=dict)
    ended_at: str | None = None
    retry_of_job_id: str | None = None
    message: str = ""


@dataclass(slots=True)
class SegmentRecord:
    segment_id: str
    project_id: str
    segment_index: int
    start_ms: int
    end_ms: int
    source_lang: str | None = None
    target_lang: str | None = None
    source_text: str = ""
    source_text_norm: str = ""
    translated_text: str = ""
    translated_text_norm: str = ""
    subtitle_text: str = ""
    tts_text: str = ""
    audio_path: str | None = None
    status: str = "draft"
    meta_json: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class SubtitleTrackRecord:
    track_id: str
    project_id: str
    name: str
    kind: str
    created_at: str
    updated_at: str
    notes: str = ""


@dataclass(slots=True)
class SubtitleEventRecord:
    event_id: str
    track_id: str
    project_id: str
    event_index: int
    start_ms: int
    end_ms: int
    source_segment_id: str | None = None
    source_lang: str | None = None
    target_lang: str | None = None
    source_text: str = ""
    source_text_norm: str = ""
    translated_text: str = ""
    translated_text_norm: str = ""
    subtitle_text: str = ""
    tts_text: str = ""
    audio_path: str | None = None
    status: str = "draft"
    meta_json: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ProjectWorkspace:
    project_id: str
    name: str
    root_dir: Path
    database_path: Path
    project_json_path: Path
    logs_dir: Path
    cache_dir: Path
    exports_dir: Path
    video_asset_id: str | None = None
    source_video_path: Path | None = None
