from __future__ import annotations

import json
import subprocess
from pathlib import Path

from app.core.ffmpeg import _resolve_executable
from app.core.hashing import build_stage_hash, fingerprint_path
from app.core.jobs import JobCancelledError, JobContext
from app.media.models import ExtractedAudioArtifacts, MediaMetadata
from app.project.models import ProjectWorkspace


def build_extract_audio_stage_hash(
    source_path: Path,
    *,
    audio_stream_index: int | None,
    ffmpeg_path: str | None,
) -> str:
    return build_stage_hash(
        {
            "stage": "extract_audio",
            "input": fingerprint_path(source_path),
            "audio_stream_index": audio_stream_index,
            "ffmpeg_path": ffmpeg_path or "ffmpeg",
            "version": 1,
        }
    )


def get_extract_audio_artifacts(
    workspace: ProjectWorkspace,
    metadata: MediaMetadata,
    *,
    ffmpeg_path: str | None,
) -> ExtractedAudioArtifacts:
    stage_hash = build_extract_audio_stage_hash(
        metadata.source_path,
        audio_stream_index=metadata.primary_audio_stream.index if metadata.primary_audio_stream else None,
        ffmpeg_path=ffmpeg_path,
    )
    cache_dir = workspace.cache_dir / "extract_audio" / stage_hash
    return ExtractedAudioArtifacts(
        stage_hash=stage_hash,
        cache_dir=cache_dir,
        audio_16k_path=cache_dir / "audio_16k.wav",
        audio_48k_path=cache_dir / "audio_48k.wav",
        manifest_path=cache_dir / "manifest.json",
    )


def _build_extract_command(
    *,
    ffmpeg_executable: str,
    source_path: Path,
    audio_stream_index: int | None,
    output_path: Path,
    channels: int,
    sample_rate: int,
) -> list[str]:
    command = [
        ffmpeg_executable,
        "-y",
        "-hide_banner",
        "-nostats",
        "-progress",
        "pipe:1",
        "-i",
        str(source_path),
    ]
    if audio_stream_index is not None:
        command.extend(["-map", f"0:{audio_stream_index}"])
    command.extend(
        [
            "-vn",
            "-ac",
            str(channels),
            "-ar",
            str(sample_rate),
            "-c:a",
            "pcm_s16le",
            str(output_path),
        ]
    )
    return command


def _run_ffmpeg_with_progress(
    context: JobContext,
    *,
    command: list[str],
    duration_ms: int | None,
    progress_range: tuple[int, int],
) -> None:
    context.logger.info("Running ffmpeg command: %s", command)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    try:
        while True:
            context.cancellation_token.raise_if_canceled()
            line = process.stdout.readline() if process.stdout else ""
            if not line:
                if process.poll() is not None:
                    break
                continue

            stripped = line.strip()
            if stripped.startswith("out_time_ms=") and duration_ms:
                current_ms = int(stripped.split("=", maxsplit=1)[1]) // 1000
                ratio = max(0.0, min(current_ms / max(duration_ms, 1), 1.0))
                start, end = progress_range
                progress = start + int((end - start) * ratio)
                context.report_progress(progress, f"Dang extract audio... {progress}%")
            elif stripped == "progress=end":
                context.report_progress(progress_range[1], "Da extract xong buoc")
    except JobCancelledError:
        process.kill()
        raise

    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(stderr.strip() or stdout.strip() or "ffmpeg extract audio that bai")


def extract_audio_artifacts(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    metadata: MediaMetadata,
    ffmpeg_path: str | None = None,
) -> ExtractedAudioArtifacts:
    ffmpeg_executable = _resolve_executable("ffmpeg", ffmpeg_path)
    if not ffmpeg_executable:
        raise FileNotFoundError("Khong tim thay ffmpeg trong PATH hoac settings")
    if not metadata.primary_audio_stream:
        raise RuntimeError("Metadata khong co audio stream de extract")

    artifacts = get_extract_audio_artifacts(workspace, metadata, ffmpeg_path=ffmpeg_executable)
    artifacts.cache_dir.mkdir(parents=True, exist_ok=True)

    if artifacts.audio_16k_path.exists() and artifacts.audio_48k_path.exists() and artifacts.manifest_path.exists():
        context.report_progress(100, "Dung cache extract audio")
        return artifacts

    audio_stream_index = metadata.primary_audio_stream.index if metadata.primary_audio_stream else None
    commands = [
        (
            _build_extract_command(
                ffmpeg_executable=ffmpeg_executable,
                source_path=metadata.source_path,
                audio_stream_index=audio_stream_index,
                output_path=artifacts.audio_16k_path,
                channels=1,
                sample_rate=16000,
            ),
            (5, 55),
        ),
        (
            _build_extract_command(
                ffmpeg_executable=ffmpeg_executable,
                source_path=metadata.source_path,
                audio_stream_index=audio_stream_index,
                output_path=artifacts.audio_48k_path,
                channels=2,
                sample_rate=48000,
            ),
            (55, 100),
        ),
    ]

    for command, progress_range in commands:
        _run_ffmpeg_with_progress(
            context,
            command=command,
            duration_ms=metadata.duration_ms,
            progress_range=progress_range,
        )

    artifacts.manifest_path.write_text(
        json.dumps(
            {
                "stage_hash": artifacts.stage_hash,
                "source_path": str(metadata.source_path),
                "audio_16k_path": str(artifacts.audio_16k_path),
                "audio_48k_path": str(artifacts.audio_48k_path),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return artifacts


def load_cached_audio_artifacts(workspace: ProjectWorkspace) -> ExtractedAudioArtifacts | None:
    root = workspace.cache_dir / "extract_audio"
    if not root.exists():
        return None

    manifests = sorted(root.glob("*/manifest.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not manifests:
        return None

    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        source_path = payload.get("source_path")
        if workspace.source_video_path and source_path != str(workspace.source_video_path):
            continue
        cache_dir = manifest_path.parent
        return ExtractedAudioArtifacts(
            stage_hash=payload["stage_hash"],
            cache_dir=cache_dir,
            audio_16k_path=Path(payload["audio_16k_path"]),
            audio_48k_path=Path(payload["audio_48k_path"]),
            manifest_path=manifest_path,
        )
    return None
