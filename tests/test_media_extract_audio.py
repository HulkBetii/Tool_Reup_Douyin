from __future__ import annotations

import json
from pathlib import Path

from app.media.extract_audio import build_extract_audio_stage_hash, load_cached_audio_artifacts
from app.project.models import ProjectWorkspace


def test_extract_audio_stage_hash_is_stable_for_same_input(tmp_path: Path) -> None:
    source_path = tmp_path / "input.mp4"
    source_path.write_bytes(b"video")

    first = build_extract_audio_stage_hash(
        source_path,
        audio_stream_index=1,
        ffmpeg_path="ffmpeg.exe",
    )
    second = build_extract_audio_stage_hash(
        source_path,
        audio_stream_index=1,
        ffmpeg_path="ffmpeg.exe",
    )

    assert first == second


def test_load_cached_audio_artifacts_prefers_matching_source_video(tmp_path: Path) -> None:
    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=tmp_path / "project",
        database_path=tmp_path / "project" / "project.db",
        project_json_path=tmp_path / "project" / "project.json",
        logs_dir=tmp_path / "project" / "logs",
        cache_dir=tmp_path / "project" / "cache",
        exports_dir=tmp_path / "project" / "exports",
        source_video_path=tmp_path / "project" / "source_a.mp4",
    )
    (workspace.cache_dir / "extract_audio" / "hash-a").mkdir(parents=True, exist_ok=True)
    (workspace.cache_dir / "extract_audio" / "hash-b").mkdir(parents=True, exist_ok=True)

    manifest_a = workspace.cache_dir / "extract_audio" / "hash-a" / "manifest.json"
    manifest_b = workspace.cache_dir / "extract_audio" / "hash-b" / "manifest.json"

    payload_a = {
        "stage_hash": "hash-a",
        "source_path": str(workspace.source_video_path),
        "audio_16k_path": str(workspace.cache_dir / "extract_audio" / "hash-a" / "audio_16k.wav"),
        "audio_48k_path": str(workspace.cache_dir / "extract_audio" / "hash-a" / "audio_48k.wav"),
    }
    payload_b = {
        "stage_hash": "hash-b",
        "source_path": str(tmp_path / "project" / "source_b.mp4"),
        "audio_16k_path": str(workspace.cache_dir / "extract_audio" / "hash-b" / "audio_16k.wav"),
        "audio_48k_path": str(workspace.cache_dir / "extract_audio" / "hash-b" / "audio_48k.wav"),
    }
    manifest_a.write_text(json.dumps(payload_a), encoding="utf-8")
    manifest_b.write_text(json.dumps(payload_b), encoding="utf-8")

    artifacts = load_cached_audio_artifacts(workspace)

    assert artifacts is not None
    assert artifacts.stage_hash == "hash-a"

