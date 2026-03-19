from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from app.core.hashing import build_stage_hash, fingerprint_path
from app.core.jobs import JobContext
from app.project.models import ProjectWorkspace


@dataclass(slots=True)
class MixdownResult:
    stage_hash: str
    cache_dir: Path
    mixed_audio_path: Path
    manifest_path: Path


def build_mixdown_stage_hash(
    *,
    original_audio_path: Path,
    voice_track_path: Path,
    original_volume: float,
    voice_volume: float,
    bgm_path: Path | None,
    bgm_volume: float,
) -> str:
    return build_stage_hash(
        {
            "stage": "mixdown",
            "original_audio_path": fingerprint_path(original_audio_path),
            "voice_track_path": fingerprint_path(voice_track_path),
            "original_volume": original_volume,
            "voice_volume": voice_volume,
            "bgm_path": fingerprint_path(bgm_path) if bgm_path and bgm_path.exists() else None,
            "bgm_volume": bgm_volume,
            "version": 1,
        }
    )


def build_mixdown_command(
    *,
    ffmpeg_executable: str,
    original_audio_path: Path,
    voice_track_path: Path,
    output_path: Path,
    original_volume: float,
    voice_volume: float,
    bgm_path: Path | None = None,
    bgm_volume: float = 0.2,
) -> list[str]:
    command = [
        ffmpeg_executable,
        "-y",
        "-i",
        str(original_audio_path),
        "-i",
        str(voice_track_path),
    ]
    input_count = 2
    if bgm_path is not None:
        command.extend(["-stream_loop", "-1", "-i", str(bgm_path)])
        input_count += 1

    filter_parts = [
        f"[0:a]aresample=48000,volume={original_volume:.3f}[orig]",
        f"[1:a]aresample=48000,volume={voice_volume:.3f}[voice]",
    ]
    mix_inputs = ["[orig]", "[voice]"]
    if bgm_path is not None:
        filter_parts.append(f"[2:a]aresample=48000,volume={bgm_volume:.3f}[bgm]")
        mix_inputs.append("[bgm]")
    filter_parts.append(
        "".join(mix_inputs) + f"amix=inputs={input_count}:normalize=0,loudnorm=I=-16:TP=-1.5:LRA=11[out]"
    )
    command.extend(
        [
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[out]",
            "-ac",
            "2",
            "-ar",
            "48000",
            str(output_path),
        ]
    )
    return command


def mix_audio_tracks(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    original_audio_path: Path,
    voice_track_path: Path,
    ffmpeg_path: str | None,
    original_volume: float,
    voice_volume: float,
    bgm_path: Path | None = None,
    bgm_volume: float = 0.2,
) -> MixdownResult:
    ffmpeg_executable = ffmpeg_path or shutil.which("ffmpeg")
    if not ffmpeg_executable:
        raise RuntimeError("Khong tim thay ffmpeg.exe")

    stage_hash = build_mixdown_stage_hash(
        original_audio_path=original_audio_path,
        voice_track_path=voice_track_path,
        original_volume=original_volume,
        voice_volume=voice_volume,
        bgm_path=bgm_path,
        bgm_volume=bgm_volume,
    )
    cache_dir = workspace.cache_dir / "mix" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    mixed_audio_path = cache_dir / "mixed_audio.wav"
    manifest_path = cache_dir / "mix_manifest.json"
    if manifest_path.exists() and mixed_audio_path.exists():
        context.report_progress(100, "Dung cache mixed audio")
        return MixdownResult(stage_hash=stage_hash, cache_dir=cache_dir, mixed_audio_path=mixed_audio_path, manifest_path=manifest_path)

    command = build_mixdown_command(
        ffmpeg_executable=ffmpeg_executable,
        original_audio_path=original_audio_path,
        voice_track_path=voice_track_path,
        output_path=mixed_audio_path,
        original_volume=original_volume,
        voice_volume=voice_volume,
        bgm_path=bgm_path,
        bgm_volume=bgm_volume,
    )
    context.report_progress(20, "Dang mix voice + audio goc")
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=240,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "Mixdown that bai").strip())

    manifest_path.write_text(
        json.dumps(
            {
                "stage_hash": stage_hash,
                "original_audio_path": str(original_audio_path),
                "voice_track_path": str(voice_track_path),
                "bgm_path": str(bgm_path) if bgm_path else None,
                "original_volume": original_volume,
                "voice_volume": voice_volume,
                "bgm_volume": bgm_volume,
                "mixed_audio_path": str(mixed_audio_path),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    context.report_progress(100, "Da tao mixed audio")
    return MixdownResult(stage_hash=stage_hash, cache_dir=cache_dir, mixed_audio_path=mixed_audio_path, manifest_path=manifest_path)
