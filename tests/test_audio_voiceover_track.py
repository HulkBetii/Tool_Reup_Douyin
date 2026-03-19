from __future__ import annotations

from app.audio.voiceover_track import build_fit_filter


def test_build_fit_filter_handles_padding_and_speedup() -> None:
    short_filter = build_fit_filter(clip_duration_ms=800, slot_ms=1200)
    long_filter = build_fit_filter(clip_duration_ms=2400, slot_ms=1200)

    assert "apad" in short_filter
    assert "atrim" in short_filter
    assert "atempo" in long_filter
    assert "atrim" in long_filter
