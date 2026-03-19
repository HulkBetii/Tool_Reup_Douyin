from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from app.core.hashing import build_stage_hash, fingerprint_path
from app.core.jobs import JobContext
from app.project.models import ProjectWorkspace
from app.tts.models import SynthesizedSegmentArtifact


@dataclass(slots=True)
class VoiceTrackResult:
    stage_hash: str
    cache_dir: Path
    voice_track_path: Path
    manifest_path: Path
    fitted_clips: list[SynthesizedSegmentArtifact] = field(default_factory=list)


def _slot_duration_ms(artifact: SynthesizedSegmentArtifact) -> int:
    return max(1, artifact.end_ms - artifact.start_ms)


def _build_atempo_filter(speed: float) -> str:
    remaining = speed
    parts: list[str] = []
    while remaining > 2.0:
        parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        parts.append("atempo=0.5")
        remaining /= 0.5
    parts.append(f"atempo={remaining:.5f}")
    return ",".join(parts)


def build_fit_filter(clip_duration_ms: int, slot_ms: int) -> str:
    slot_sec = max(0.001, slot_ms / 1000.0)
    if clip_duration_ms <= 0 or clip_duration_ms <= slot_ms:
        return f"aresample=48000,apad=pad_dur={slot_sec:.3f},atrim=end={slot_sec:.3f}"
    speed = clip_duration_ms / slot_ms
    return f"aresample=48000,{_build_atempo_filter(speed)},apad=pad_dur={slot_sec:.3f},atrim=end={slot_sec:.3f}"


def build_voice_track_stage_hash(
    artifacts: list[SynthesizedSegmentArtifact],
    *,
    total_duration_ms: int,
) -> str:
    return build_stage_hash(
        {
            "stage": "voice_track",
            "total_duration_ms": total_duration_ms,
            "artifacts": [
                {
                    "segment_id": item.segment_id,
                    "start_ms": item.start_ms,
                    "end_ms": item.end_ms,
                    "raw_wav_path": fingerprint_path(item.raw_wav_path),
                    "duration_ms": item.duration_ms,
                }
                for item in artifacts
            ],
            "version": 1,
        }
    )


def _run_ffmpeg(command: list[str]) -> None:
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
        raise RuntimeError((result.stderr or result.stdout or "FFmpeg that bai").strip())


def build_voice_track(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    artifacts: list[SynthesizedSegmentArtifact],
    ffmpeg_path: str | None,
    total_duration_ms: int,
) -> VoiceTrackResult:
    ffmpeg_executable = ffmpeg_path or shutil.which("ffmpeg")
    if not ffmpeg_executable:
        raise RuntimeError("Khong tim thay ffmpeg.exe")

    stage_hash = build_voice_track_stage_hash(artifacts, total_duration_ms=total_duration_ms)
    cache_dir = workspace.cache_dir / "mix" / stage_hash
    fitted_dir = cache_dir / "fitted"
    fitted_dir.mkdir(parents=True, exist_ok=True)
    voice_track_path = cache_dir / "voice_track.wav"
    manifest_path = cache_dir / "voice_track_manifest.json"
    if manifest_path.exists() and voice_track_path.exists():
        context.report_progress(100, "Dung cache voice track")
        return VoiceTrackResult(
            stage_hash=stage_hash,
            cache_dir=cache_dir,
            voice_track_path=voice_track_path,
            manifest_path=manifest_path,
            fitted_clips=artifacts,
        )

    fitted_artifacts: list[SynthesizedSegmentArtifact] = []
    total = max(1, len(artifacts))
    for index, artifact in enumerate(artifacts, start=1):
        slot_ms = _slot_duration_ms(artifact)
        fitted_path = fitted_dir / f"{artifact.segment_index:04d}_{artifact.segment_id}.wav"
        if not fitted_path.exists():
            filter_expr = build_fit_filter(artifact.duration_ms, slot_ms)
            _run_ffmpeg(
                [
                    ffmpeg_executable,
                    "-y",
                    "-i",
                    str(artifact.raw_wav_path),
                    "-filter:a",
                    filter_expr,
                    "-ac",
                    "2",
                    "-ar",
                    "48000",
                    str(fitted_path),
                ]
            )
        fitted_artifacts.append(
            SynthesizedSegmentArtifact(
                segment_id=artifact.segment_id,
                segment_index=artifact.segment_index,
                start_ms=artifact.start_ms,
                end_ms=artifact.end_ms,
                text=artifact.text,
                raw_wav_path=artifact.raw_wav_path,
                duration_ms=artifact.duration_ms,
                sample_rate=artifact.sample_rate,
                voice_id=artifact.voice_id,
                fitted_wav_path=fitted_path,
                fitted_duration_ms=slot_ms,
            )
        )
        context.report_progress(min(80, int(index * 80 / total)), f"Fit clip {index}/{total}")

    inputs = [
        ffmpeg_executable,
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=r=48000:cl=stereo:d={max(0.001, total_duration_ms / 1000.0):.3f}",
    ]
    filter_parts: list[str] = []
    mix_inputs = ["[0:a]"]
    for input_index, artifact in enumerate(fitted_artifacts, start=1):
        if not artifact.fitted_wav_path:
            continue
        inputs.extend(["-i", str(artifact.fitted_wav_path)])
        filter_parts.append(
            f"[{input_index}:a]adelay={artifact.start_ms}|{artifact.start_ms}[v{input_index}]"
        )
        mix_inputs.append(f"[v{input_index}]")

    if len(mix_inputs) == 1:
        filter_parts.append("[0:a]anull[out]")
    else:
        filter_parts.append("".join(mix_inputs) + f"amix=inputs={len(mix_inputs)}:normalize=0[out]")

    inputs.extend(
        [
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[out]",
            "-ac",
            "2",
            "-ar",
            "48000",
            str(voice_track_path),
        ]
    )
    _run_ffmpeg(inputs)
    manifest_path.write_text(
        json.dumps(
            {
                "stage_hash": stage_hash,
                "voice_track_path": str(voice_track_path),
                "total_duration_ms": total_duration_ms,
                "fitted_clips": [
                    {
                        "segment_id": item.segment_id,
                        "segment_index": item.segment_index,
                        "start_ms": item.start_ms,
                        "end_ms": item.end_ms,
                        "raw_wav_path": str(item.raw_wav_path),
                        "fitted_wav_path": str(item.fitted_wav_path) if item.fitted_wav_path else None,
                        "fitted_duration_ms": item.fitted_duration_ms,
                    }
                    for item in fitted_artifacts
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    context.report_progress(100, "Da tao voice track")
    return VoiceTrackResult(
        stage_hash=stage_hash,
        cache_dir=cache_dir,
        voice_track_path=voice_track_path,
        manifest_path=manifest_path,
        fitted_clips=fitted_artifacts,
    )
