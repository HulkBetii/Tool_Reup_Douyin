from __future__ import annotations

from pathlib import Path

from app.audio.narration_incremental import build_scene_mix_stage_hash


def test_build_scene_mix_stage_hash_changes_when_mix_volumes_change(tmp_path: Path) -> None:
    original_audio = tmp_path / "orig.wav"
    voice_scene_chunk = tmp_path / "scene.wav"
    original_audio.write_bytes(b"orig")
    voice_scene_chunk.write_bytes(b"voice")
    scene_row = {
        "scene_id": "scene-0001",
        "start_ms": 0,
        "end_ms": 2500,
    }

    base_hash = build_scene_mix_stage_hash(
        scene_row=scene_row,
        original_audio_path=original_audio,
        voice_scene_chunk_path=voice_scene_chunk,
        original_volume=0.07,
        voice_volume=1.0,
    )
    changed_hash = build_scene_mix_stage_hash(
        scene_row=scene_row,
        original_audio_path=original_audio,
        voice_scene_chunk_path=voice_scene_chunk,
        original_volume=0.05,
        voice_volume=1.0,
    )

    assert base_hash != changed_hash
