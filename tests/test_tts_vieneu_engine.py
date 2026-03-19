from __future__ import annotations

import wave
from pathlib import Path

import pytest

from app.tts.models import VoicePreset
from app.tts.vieneu_engine import VieneuTTSEngine, detect_vieneu_installation, get_vieneu_mode


def _write_wav(path: Path, *, sample_rate: int = 24000, duration_ms: int = 250) -> None:
    frames = int(sample_rate * duration_ms / 1000)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * frames)


def test_detect_vieneu_installation_reports_missing_package(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.tts.vieneu_engine.importlib.util.find_spec", lambda name: None)
    monkeypatch.setattr("app.tts.vieneu_engine._find_espeak_dependency", lambda: None)

    environment = detect_vieneu_installation()

    assert not environment.package_installed
    assert not environment.local_ready
    assert environment.package_version is None


def test_get_vieneu_mode_rejects_invalid_value() -> None:
    preset = VoicePreset(
        voice_preset_id="vieneu-invalid",
        name="VieNeu Invalid",
        engine="vieneu",
        voice_id="default",
        sample_rate=24000,
        engine_options={"mode": "batch"},
    )

    with pytest.raises(ValueError):
        get_vieneu_mode(preset)


def test_vieneu_engine_synthesizes_with_remote_voice(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_path = tmp_path / "remote.wav"
    captured: dict[str, object] = {}

    class FakeVieneu:
        def __init__(self, **kwargs) -> None:
            captured["init_kwargs"] = kwargs

        def get_preset_voice(self, voice_id: str) -> dict[str, str]:
            captured["voice_id"] = voice_id
            return {"voice_id": voice_id}

        def infer(self, **kwargs) -> bytes:
            captured["infer_kwargs"] = kwargs
            return b"audio"

        def save(self, audio: bytes, output: str) -> None:
            captured["save_audio"] = audio
            _write_wav(Path(output))

    monkeypatch.setattr("app.tts.vieneu_engine.importlib.util.find_spec", lambda name: object())
    monkeypatch.setattr("app.tts.vieneu_engine._get_vieneu_version", lambda: "1.2.3")
    monkeypatch.setattr("app.tts.vieneu_engine._find_espeak_dependency", lambda: None)
    monkeypatch.setattr(
        "app.tts.vieneu_engine.importlib.import_module",
        lambda name: type("FakeModule", (), {"Vieneu": FakeVieneu})(),
    )

    preset = VoicePreset(
        voice_preset_id="vieneu-remote",
        name="VieNeu Remote",
        engine="vieneu",
        voice_id="Tuyen",
        sample_rate=24000,
        engine_options={
            "mode": "remote",
            "api_base": "http://127.0.0.1:23333/v1",
            "model_name": "pnnbao-ump/VieNeu-TTS",
        },
    )

    engine = VieneuTTSEngine(project_root=tmp_path)
    result = engine.synthesize(text="Xin chao", output_path=output_path, preset=preset)

    assert captured["init_kwargs"] == {
        "mode": "remote",
        "api_base": "http://127.0.0.1:23333/v1",
        "model_name": "pnnbao-ump/VieNeu-TTS",
    }
    assert captured["voice_id"] == "Tuyen"
    assert captured["infer_kwargs"] == {"text": "Xin chao", "voice": {"voice_id": "Tuyen"}}
    assert result.wav_path == output_path
    assert result.sample_rate == 24000
    assert result.duration_ms > 0


def test_vieneu_engine_supports_reference_audio(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ref_audio_path = tmp_path / "assets" / "voices" / "sample.wav"
    ref_audio_path.parent.mkdir(parents=True)
    _write_wav(ref_audio_path, duration_ms=120)
    output_path = tmp_path / "clone.wav"
    captured: dict[str, object] = {}

    class FakeVieneu:
        def __init__(self, **kwargs) -> None:
            captured["init_kwargs"] = kwargs

        def infer(self, **kwargs) -> bytes:
            captured["infer_kwargs"] = kwargs
            return b"audio"

        def save(self, audio: bytes, output: str) -> None:
            _write_wav(Path(output), duration_ms=300)

    monkeypatch.setattr("app.tts.vieneu_engine.importlib.util.find_spec", lambda name: object())
    monkeypatch.setattr("app.tts.vieneu_engine._get_vieneu_version", lambda: "1.2.3")
    monkeypatch.setattr("app.tts.vieneu_engine._find_espeak_dependency", lambda: tmp_path / "espeak-ng.exe")
    monkeypatch.setattr(
        "app.tts.vieneu_engine.importlib.import_module",
        lambda name: type("FakeModule", (), {"Vieneu": FakeVieneu})(),
    )

    preset = VoicePreset(
        voice_preset_id="vieneu-clone",
        name="VieNeu Clone",
        engine="vieneu",
        voice_id="default",
        sample_rate=24000,
        engine_options={
            "mode": "local",
            "ref_audio_path": "assets/voices/sample.wav",
            "ref_text": "Xin chao ban toi la mau tham chieu",
        },
    )

    engine = VieneuTTSEngine(project_root=tmp_path)
    result = engine.synthesize(text="Day la giong clone", output_path=output_path, preset=preset)

    assert captured["init_kwargs"] == {}
    assert captured["infer_kwargs"] == {
        "text": "Day la giong clone",
        "ref_audio": str(ref_audio_path.resolve()),
        "ref_text": "Xin chao ban toi la mau tham chieu",
    }
    assert result.wav_path == output_path
    assert result.sample_rate == 24000


def test_vieneu_engine_requires_espeak_for_local_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("app.tts.vieneu_engine.importlib.util.find_spec", lambda name: object())
    monkeypatch.setattr("app.tts.vieneu_engine._get_vieneu_version", lambda: "1.2.3")
    monkeypatch.setattr("app.tts.vieneu_engine._find_espeak_dependency", lambda: None)

    preset = VoicePreset(
        voice_preset_id="vieneu-local",
        name="VieNeu Local",
        engine="vieneu",
        voice_id="default",
        sample_rate=24000,
        engine_options={"mode": "local"},
    )

    engine = VieneuTTSEngine(project_root=tmp_path)
    with pytest.raises(RuntimeError, match="eSpeak NG"):
        engine.synthesize(text="Xin chao", output_path=tmp_path / "out.wav", preset=preset)
