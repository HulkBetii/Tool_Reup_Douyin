from __future__ import annotations

from pathlib import Path

from app.audio.mixdown import build_mixdown_command


def test_build_mixdown_command_supports_optional_bgm(tmp_path: Path) -> None:
    original = tmp_path / "original.wav"
    voice = tmp_path / "voice.wav"
    bgm = tmp_path / "bgm.wav"
    output = tmp_path / "mixed.wav"

    command = build_mixdown_command(
        ffmpeg_executable="ffmpeg.exe",
        original_audio_path=original,
        voice_track_path=voice,
        output_path=output,
        original_volume=0.35,
        voice_volume=1.0,
        bgm_path=bgm,
        bgm_volume=0.15,
    )

    assert command[:4] == ["ffmpeg.exe", "-y", "-i", str(original)]
    assert "-stream_loop" in command
    assert "loudnorm" in command[command.index("-filter_complex") + 1]
    assert str(output) == command[-1]
