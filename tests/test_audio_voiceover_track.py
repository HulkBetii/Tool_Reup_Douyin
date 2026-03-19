from __future__ import annotations

import wave
from pathlib import Path

from app.audio.voiceover_track import build_fit_filter
from app.audio.voiceover_track import build_voice_track
from app.core.jobs import CancellationToken, JobContext
from app.project.models import ProjectWorkspace
from app.tts.models import SynthesizedSegmentArtifact


def test_build_fit_filter_handles_padding_and_speedup() -> None:
    short_filter = build_fit_filter(clip_duration_ms=800, slot_ms=1200)
    long_filter = build_fit_filter(clip_duration_ms=2400, slot_ms=1200)

    assert "apad" in short_filter
    assert "atrim" in short_filter
    assert "atempo" in long_filter
    assert "atrim" in long_filter


def _write_wav(path: Path, *, sample_rate: int = 24000, duration_ms: int = 200) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = int(sample_rate * duration_ms / 1000)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * max(1, frames))


def test_build_voice_track_batches_large_mix(monkeypatch, tmp_path: Path) -> None:
    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Voice Track Batch Test",
        root_dir=tmp_path,
        database_path=tmp_path / "project.db",
        project_json_path=tmp_path / "project.json",
        logs_dir=tmp_path / "logs",
        cache_dir=tmp_path / "cache",
        exports_dir=tmp_path / "exports",
    )
    artifacts: list[SynthesizedSegmentArtifact] = []
    for index in range(5):
        raw_path = tmp_path / "raw" / f"{index:04d}.wav"
        _write_wav(raw_path, duration_ms=150)
        artifacts.append(
            SynthesizedSegmentArtifact(
                segment_id=f"segment-{index}",
                segment_index=index,
                start_ms=index * 300,
                end_ms=(index * 300) + 250,
                text=f"Segment {index}",
                raw_wav_path=raw_path,
                duration_ms=150,
                sample_rate=24000,
                voice_id="default",
            )
        )

    ffmpeg_commands: list[list[str]] = []

    def fake_run_ffmpeg(command: list[str]) -> None:
        ffmpeg_commands.append(command)
        output_path = Path(command[-1])
        _write_wav(output_path, sample_rate=48000, duration_ms=1500)

    monkeypatch.setattr("app.audio.voiceover_track._run_ffmpeg", fake_run_ffmpeg)

    context = JobContext(
        job_id="voice-batch-test",
        logger_name="tests.voice_batch",
        cancellation_token=CancellationToken(),
        progress_callback=lambda value, message: None,
    )
    result = build_voice_track(
        context,
        workspace=workspace,
        artifacts=artifacts,
        ffmpeg_path="ffmpeg.exe",
        total_duration_ms=3000,
        max_batch_inputs=2,
    )

    assert result.voice_track_path.exists()
    assert any("aligned_batch_001.wav" in str(command[-1]) for command in ffmpeg_commands)
    assert any("mix_level_00_001.wav" in str(command[-1]) for command in ffmpeg_commands)
