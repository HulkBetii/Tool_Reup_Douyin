from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

from app.core.hashing import build_stage_hash, fingerprint_path
from app.core.jobs import JobContext
from app.project.models import ProjectWorkspace
from app.tts.models import SynthesizedSegmentArtifact

from .voiceover_track import build_voice_track


@dataclass(slots=True)
class VoiceSceneChunkResult:
    scene_id: str
    stage_hash: str
    cache_dir: Path
    voice_track_path: Path
    manifest_path: Path


@dataclass(slots=True)
class MixedSceneChunkResult:
    scene_id: str
    stage_hash: str
    cache_dir: Path
    mixed_audio_path: Path
    manifest_path: Path


@dataclass(slots=True)
class FinalAudioTrackResult:
    stage_hash: str
    cache_dir: Path
    final_audio_path: Path
    manifest_path: Path


def _concat_list_entry(path: Path) -> str:
    return "file '{}'".format(str(path).replace("'", "''"))


def _scene_duration_ms(scene_row) -> int:
    return max(1, int(scene_row["end_ms"]) - int(scene_row["start_ms"]))


def _scene_artifacts(scene_row, artifacts: list[SynthesizedSegmentArtifact]) -> list[SynthesizedSegmentArtifact]:
    start_index = int(scene_row["start_segment_index"])
    end_index = int(scene_row["end_segment_index"])
    return [item for item in artifacts if start_index <= int(item.segment_index) <= end_index]


def _rebase_scene_artifacts(scene_row, artifacts: list[SynthesizedSegmentArtifact]) -> list[SynthesizedSegmentArtifact]:
    scene_start_ms = int(scene_row["start_ms"])
    rebased: list[SynthesizedSegmentArtifact] = []
    for item in _scene_artifacts(scene_row, artifacts):
        rebased.append(
            SynthesizedSegmentArtifact(
                segment_id=item.segment_id,
                segment_index=item.segment_index,
                start_ms=max(0, item.start_ms - scene_start_ms),
                end_ms=max(0, item.end_ms - scene_start_ms),
                text=item.text,
                raw_wav_path=item.raw_wav_path,
                duration_ms=item.duration_ms,
                sample_rate=item.sample_rate,
                voice_id=item.voice_id,
                voice_preset_id=item.voice_preset_id,
                speaker_key=item.speaker_key,
                voice_speed=item.voice_speed,
                voice_volume=item.voice_volume,
                voice_pitch=item.voice_pitch,
            )
        )
    return rebased


def build_scene_voice_track(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    scene_row,
    artifacts: list[SynthesizedSegmentArtifact],
    ffmpeg_path: str | None,
) -> VoiceSceneChunkResult:
    rebased_artifacts = _rebase_scene_artifacts(scene_row, artifacts)
    scene_workspace = replace(workspace, cache_dir=workspace.cache_dir / "voice_scene_chunks")
    voice_track = build_voice_track(
        context,
        workspace=scene_workspace,
        artifacts=rebased_artifacts,
        ffmpeg_path=ffmpeg_path,
        total_duration_ms=_scene_duration_ms(scene_row),
    )
    return VoiceSceneChunkResult(
        scene_id=str(scene_row["scene_id"]),
        stage_hash=voice_track.stage_hash,
        cache_dir=voice_track.cache_dir,
        voice_track_path=voice_track.voice_track_path,
        manifest_path=voice_track.manifest_path,
    )


def build_scene_mix_stage_hash(
    *,
    scene_row,
    original_audio_path: Path,
    voice_scene_chunk_path: Path,
    original_volume: float,
    voice_volume: float,
) -> str:
    return build_stage_hash(
        {
            "stage": "narration_scene_mix",
            "scene_id": str(scene_row["scene_id"]),
            "start_ms": int(scene_row["start_ms"]),
            "end_ms": int(scene_row["end_ms"]),
            "original_audio_path": fingerprint_path(original_audio_path),
            "voice_scene_chunk_path": fingerprint_path(voice_scene_chunk_path),
            "original_volume": original_volume,
            "voice_volume": voice_volume,
            "version": 1,
        }
    )


def build_mixed_scene_chunk(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    scene_row,
    original_audio_path: Path,
    voice_scene_chunk_path: Path,
    ffmpeg_path: str,
    original_volume: float,
    voice_volume: float,
) -> MixedSceneChunkResult:
    stage_hash = build_scene_mix_stage_hash(
        scene_row=scene_row,
        original_audio_path=original_audio_path,
        voice_scene_chunk_path=voice_scene_chunk_path,
        original_volume=original_volume,
        voice_volume=voice_volume,
    )
    cache_dir = workspace.cache_dir / "mixed_scene_chunks" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    mixed_audio_path = cache_dir / f"{scene_row['scene_id']}.wav"
    manifest_path = cache_dir / "manifest.json"
    if manifest_path.exists() and mixed_audio_path.exists():
        context.report_progress(100, f"Dung cache mixed scene {scene_row['scene_id']}")
        return MixedSceneChunkResult(
            scene_id=str(scene_row["scene_id"]),
            stage_hash=stage_hash,
            cache_dir=cache_dir,
            mixed_audio_path=mixed_audio_path,
            manifest_path=manifest_path,
        )

    duration_ms = _scene_duration_ms(scene_row)
    start_seconds = max(0.0, int(scene_row["start_ms"]) / 1000.0)
    duration_seconds = max(0.001, duration_ms / 1000.0)
    command = [
        ffmpeg_path,
        "-y",
        "-ss",
        f"{start_seconds:.3f}",
        "-t",
        f"{duration_seconds:.3f}",
        "-i",
        str(original_audio_path),
        "-i",
        str(voice_scene_chunk_path),
        "-filter_complex",
        (
            f"[0:a]aresample=48000,asetpts=PTS-STARTPTS,volume={original_volume:.3f}[orig];"
            f"[1:a]aresample=48000,volume={voice_volume:.3f}[voice];"
            "[orig][voice]amix=inputs=2:normalize=0,loudnorm=I=-16:TP=-1.5:LRA=11[out]"
        ),
        "-map",
        "[out]",
        "-ac",
        "2",
        "-ar",
        "48000",
        str(mixed_audio_path),
    ]
    context.report_progress(20, f"Mix scene {scene_row['scene_id']}")
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
        raise RuntimeError((result.stderr or result.stdout or "Mix scene chunk that bai").strip())
    manifest_path.write_text(
        json.dumps(
            {
                "scene_id": str(scene_row["scene_id"]),
                "stage_hash": stage_hash,
                "mixed_audio_path": str(mixed_audio_path),
                "voice_scene_chunk_path": str(voice_scene_chunk_path),
                "original_audio_path": str(original_audio_path),
                "original_volume": original_volume,
                "voice_volume": voice_volume,
                "start_ms": int(scene_row["start_ms"]),
                "end_ms": int(scene_row["end_ms"]),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return MixedSceneChunkResult(
        scene_id=str(scene_row["scene_id"]),
        stage_hash=stage_hash,
        cache_dir=cache_dir,
        mixed_audio_path=mixed_audio_path,
        manifest_path=manifest_path,
    )


def build_final_audio_track(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    scene_chunks: list[MixedSceneChunkResult],
    ffmpeg_path: str,
) -> FinalAudioTrackResult:
    stage_hash = build_stage_hash(
        {
            "stage": "narration_final_audio_track",
            "chunks": [
                {"scene_id": item.scene_id, "mixed_audio_path": fingerprint_path(item.mixed_audio_path)}
                for item in scene_chunks
            ],
            "version": 1,
        }
    )
    cache_dir = workspace.cache_dir / "final_audio_track" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    final_audio_path = cache_dir / "final_audio.wav"
    manifest_path = cache_dir / "manifest.json"
    if manifest_path.exists() and final_audio_path.exists():
        context.report_progress(100, "Dung cache final audio track")
        return FinalAudioTrackResult(stage_hash=stage_hash, cache_dir=cache_dir, final_audio_path=final_audio_path, manifest_path=manifest_path)

    concat_list_path = cache_dir / "concat_list.txt"
    concat_list_path.write_text(
        "\n".join(_concat_list_entry(item.mixed_audio_path) for item in scene_chunks),
        encoding="utf-8",
    )
    command = [
        ffmpeg_path,
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_list_path),
        "-ac",
        "2",
        "-ar",
        "48000",
        str(final_audio_path),
    ]
    context.report_progress(20, "Dang gop final audio narration")
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
        raise RuntimeError((result.stderr or result.stdout or "Concat final audio that bai").strip())
    manifest_path.write_text(
        json.dumps(
            {
                "stage_hash": stage_hash,
                "final_audio_path": str(final_audio_path),
                "chunks": [
                    {"scene_id": item.scene_id, "mixed_audio_path": str(item.mixed_audio_path)}
                    for item in scene_chunks
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return FinalAudioTrackResult(stage_hash=stage_hash, cache_dir=cache_dir, final_audio_path=final_audio_path, manifest_path=manifest_path)
