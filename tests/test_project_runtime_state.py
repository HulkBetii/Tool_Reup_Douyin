from __future__ import annotations

import json
from pathlib import Path

from app.project.runtime_state import restore_pipeline_state


def test_restore_pipeline_state_recovers_latest_outputs(tmp_path: Path) -> None:
    srt_path = tmp_path / "track.srt"
    ass_path = tmp_path / "track.ass"
    tts_manifest = tmp_path / "tts_manifest.json"
    voice_track = tmp_path / "voice_track.wav"
    mixed_audio = tmp_path / "mixed_audio.wav"
    export_video = tmp_path / "final.mp4"
    for path in (srt_path, ass_path, tts_manifest, voice_track, mixed_audio, export_video):
        path.write_text("ok", encoding="utf-8")

    rows = [
        {"stage": "export_video", "status": "success", "output_paths_json": json.dumps([str(ass_path), str(export_video)])},
        {"stage": "mixdown", "status": "success", "output_paths_json": json.dumps([str(tts_manifest), str(mixed_audio)])},
        {"stage": "voice_track", "status": "success", "output_paths_json": json.dumps([str(tts_manifest), str(voice_track)])},
        {"stage": "tts", "status": "success", "output_paths_json": json.dumps([str(tts_manifest)])},
        {"stage": "export_ass", "status": "success", "output_paths_json": json.dumps([str(ass_path)])},
        {"stage": "export_srt", "status": "success", "output_paths_json": json.dumps([str(srt_path)])},
    ]

    restored = restore_pipeline_state(rows)

    assert restored.subtitle_outputs["srt"] == srt_path
    assert restored.subtitle_outputs["ass"] == ass_path
    assert restored.tts_manifest_path == tts_manifest
    assert restored.voice_track_path == voice_track
    assert restored.mixed_audio_path == mixed_audio
    assert restored.export_output_path == export_video


def test_restore_pipeline_state_skips_missing_and_failed_outputs(tmp_path: Path) -> None:
    existing_manifest = tmp_path / "manifest.json"
    existing_manifest.write_text("ok", encoding="utf-8")

    restored = restore_pipeline_state(
        [
            {
                "stage": "mixdown",
                "status": "failed",
                "output_paths_json": json.dumps([str(tmp_path / "missing.wav")]),
            },
            {
                "stage": "tts",
                "status": "success",
                "output_paths_json": json.dumps([str(existing_manifest), str(tmp_path / "missing.wav")]),
            },
        ]
    )

    assert restored.tts_manifest_path == existing_manifest
    assert restored.mixed_audio_path is None
    assert restored.export_output_path is None
