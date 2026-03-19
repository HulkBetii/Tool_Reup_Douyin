from __future__ import annotations

import json
import subprocess
from pathlib import Path
from uuid import uuid4

from app.core.ffmpeg import _resolve_executable
from app.project.bootstrap import calculate_sha256, sync_project_snapshot, utc_now_iso
from app.project.database import ProjectDatabase
from app.project.models import MediaAssetRecord, ProjectWorkspace

from .models import AudioStreamInfo, MediaMetadata, VideoStreamInfo


def _parse_fraction(raw_value: str | None) -> float | None:
    if not raw_value:
        return None
    if "/" in raw_value:
        numerator, denominator = raw_value.split("/", maxsplit=1)
        try:
            denominator_value = float(denominator)
            if denominator_value == 0:
                return None
            return float(numerator) / denominator_value
        except ValueError:
            return None
    try:
        return float(raw_value)
    except ValueError:
        return None


def parse_ffprobe_payload(payload: dict[str, object], source_path: Path, sha256: str | None = None) -> MediaMetadata:
    format_payload = payload.get("format", {}) if isinstance(payload.get("format"), dict) else {}
    streams = payload.get("streams", []) if isinstance(payload.get("streams"), list) else []

    video_stream = None
    audio_streams: list[AudioStreamInfo] = []

    for stream in streams:
        if not isinstance(stream, dict):
            continue
        codec_type = stream.get("codec_type")
        if codec_type == "video" and video_stream is None:
            video_stream = VideoStreamInfo(
                index=int(stream.get("index", 0)),
                codec_name=stream.get("codec_name"),
                width=int(stream["width"]) if stream.get("width") is not None else None,
                height=int(stream["height"]) if stream.get("height") is not None else None,
                fps=_parse_fraction(
                    stream.get("avg_frame_rate") or stream.get("r_frame_rate")
                ),
            )
        if codec_type == "audio":
            tags = stream.get("tags", {}) if isinstance(stream.get("tags"), dict) else {}
            sample_rate = stream.get("sample_rate")
            audio_streams.append(
                AudioStreamInfo(
                    index=int(stream.get("index", 0)),
                    codec_name=stream.get("codec_name"),
                    channels=int(stream["channels"]) if stream.get("channels") is not None else None,
                    sample_rate=int(sample_rate) if sample_rate is not None else None,
                    language=tags.get("language"),
                )
            )

    duration_seconds = format_payload.get("duration")
    bit_rate = format_payload.get("bit_rate")
    size_bytes = format_payload.get("size")

    duration_ms = None
    if duration_seconds is not None:
        try:
            duration_ms = int(float(duration_seconds) * 1000)
        except (TypeError, ValueError):
            duration_ms = None

    return MediaMetadata(
        source_path=source_path.resolve(),
        duration_ms=duration_ms,
        size_bytes=int(size_bytes) if size_bytes is not None else None,
        format_name=format_payload.get("format_name"),
        bit_rate=int(bit_rate) if bit_rate is not None else None,
        video_stream=video_stream,
        audio_streams=audio_streams,
        sha256=sha256,
    )


def probe_media(input_path: Path, *, ffprobe_path: str | None = None) -> MediaMetadata:
    resolved_input = input_path.expanduser().resolve()
    executable = _resolve_executable("ffprobe", ffprobe_path)
    if not executable:
        raise FileNotFoundError("Khong tim thay ffprobe trong PATH hoac settings")

    result = subprocess.run(
        [
            executable,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(resolved_input),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ffprobe tra ve loi")

    payload = json.loads(result.stdout)
    return parse_ffprobe_payload(payload, resolved_input, sha256=calculate_sha256(resolved_input))


def attach_source_video_to_project(
    workspace: ProjectWorkspace,
    metadata: MediaMetadata,
) -> MediaAssetRecord:
    database = ProjectDatabase(workspace.database_path)
    path_value = str(metadata.source_path)
    existing = database.find_media_asset_by_path(
        project_id=workspace.project_id,
        asset_type="video",
        path=path_value,
    )

    record = MediaAssetRecord(
        asset_id=existing["asset_id"] if existing else str(uuid4()),
        project_id=workspace.project_id,
        asset_type="video",
        path=path_value,
        sha256=metadata.sha256,
        duration_ms=metadata.duration_ms,
        fps=metadata.fps,
        width=metadata.width,
        height=metadata.height,
        audio_channels=metadata.primary_audio_stream.channels if metadata.primary_audio_stream else None,
        sample_rate=metadata.primary_audio_stream.sample_rate if metadata.primary_audio_stream else None,
        created_at=existing["created_at"] if existing else utc_now_iso(),
    )

    if existing:
        database.update_media_asset(record)
    else:
        database.insert_media_asset(record)

    database.update_project_video_asset(workspace.project_id, record.asset_id, utc_now_iso())
    workspace.video_asset_id = record.asset_id
    workspace.source_video_path = metadata.source_path
    sync_project_snapshot(workspace)
    return record

