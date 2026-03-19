from __future__ import annotations

from pathlib import Path

from app.tts.models import VoicePreset
from app.tts.sapi_engine import build_sapi_synthesize_script


def test_build_sapi_synthesize_script_contains_voice_and_output(tmp_path: Path) -> None:
    preset = VoicePreset(
        voice_preset_id="en-sapi",
        name="English",
        engine="sapi",
        voice_id="Zira",
        speed=1.25,
        volume=1.0,
        sample_rate=22050,
    )
    output_path = tmp_path / "clip.wav"

    script = build_sapi_synthesize_script(
        text="hello world",
        output_path=output_path,
        preset=preset,
    )

    assert "SAPI.SpVoice" in script
    assert "Zira" in script
    assert str(output_path.resolve()) in script
    assert "hello world" in script
