from __future__ import annotations

from pydantic import BaseModel


class ExportPreset(BaseModel):
    export_preset_id: str
    name: str
    container: str = "mp4"
    video_codec: str = "h264"
    audio_codec: str = "aac"
    resolution_mode: str = "keep"
    target_aspect: str = "keep"
    target_width: int | None = None
    target_height: int | None = None
    crf: int = 18
    burn_subtitles: bool = True
    watermark_enabled: bool = False
    watermark_path: str | None = None
    watermark_position: str = "top-right"
    watermark_opacity: float = 0.85
    watermark_scale: float = 0.16
    watermark_margin: int = 24
    notes: str = ""


class WatermarkProfile(BaseModel):
    watermark_profile_id: str
    name: str
    watermark_enabled: bool = False
    watermark_path: str | None = None
    watermark_position: str = "top-right"
    watermark_opacity: float = 0.85
    watermark_scale: float = 0.16
    watermark_margin: int = 24
    notes: str = ""
