from __future__ import annotations

from pathlib import Path

from app.tts.base import build_tts_stage_hash
from app.project.models import ProjectWorkspace
from app.tts.models import SynthesisResult, VoicePreset
from app.tts.pipeline import synthesize_segments


class _DummyCancellationToken:
    def raise_if_canceled(self) -> None:
        return


class _DummyContext:
    def __init__(self) -> None:
        self.cancellation_token = _DummyCancellationToken()

    def report_progress(self, progress: int, message: str) -> None:
        return


class _DummyEngine:
    def synthesize(self, *, text: str, output_path: Path, preset: VoicePreset) -> SynthesisResult:
        output_path.write_bytes(b"RIFFdummy")
        return SynthesisResult(
            wav_path=output_path,
            duration_ms=320,
            sample_rate=preset.sample_rate,
            voice_id=preset.voice_id,
        )


def _workspace(tmp_path: Path) -> ProjectWorkspace:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()
    return ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )


def test_synthesize_segments_can_disable_source_fallback(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    preset = VoicePreset(
        voice_preset_id="vieneu-default",
        name="VieNeu",
        engine="vieneu",
        sample_rate=24000,
        language="vi",
    )

    result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=[
            {
                "segment_id": "seg-1",
                "segment_index": 0,
                "start_ms": 0,
                "end_ms": 1000,
                "tts_text": "",
                "subtitle_text": "",
                "translated_text": "",
                "source_text": "Ni hao",
            }
        ],
        preset=preset,
        engine=_DummyEngine(),
        allow_source_fallback=False,
    )

    assert result.artifacts == []


def test_build_tts_stage_hash_changes_when_segment_voice_assignments_change() -> None:
    preset = VoicePreset(
        voice_preset_id="default-sapi",
        name="Default",
        engine="sapi",
        sample_rate=22050,
    )
    segments = [
        {
            "segment_id": "seg-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 1000,
            "tts_text": "Xin chao",
            "subtitle_text": "Xin chao",
            "translated_text": "Xin chao",
        }
    ]

    hash_without_binding = build_tts_stage_hash(segments, preset)
    hash_with_binding = build_tts_stage_hash(
        segments,
        preset,
        segment_voice_preset_ids={"seg-1": "voice-a"},
    )

    assert hash_without_binding != hash_with_binding


def test_build_tts_stage_hash_changes_when_segment_voice_style_changes() -> None:
    preset = VoicePreset(
        voice_preset_id="default-sapi",
        name="Default",
        engine="sapi",
        sample_rate=22050,
    )
    alternate_preset = VoicePreset(
        voice_preset_id="default-sapi",
        name="Default",
        engine="sapi",
        sample_rate=22050,
        speed=0.92,
    )
    segments = [
        {
            "segment_id": "seg-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 1000,
            "tts_text": "Xin chao",
            "subtitle_text": "Xin chao",
            "translated_text": "Xin chao",
        }
    ]

    base_hash = build_tts_stage_hash(segments, preset)
    style_hash = build_tts_stage_hash(
        segments,
        preset,
        segment_voice_presets={"seg-1": alternate_preset},
    )

    assert base_hash != style_hash


def test_synthesize_segments_uses_segment_voice_presets(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace(tmp_path)
    default_preset = VoicePreset(
        voice_preset_id="default-sapi",
        name="Default",
        engine="sapi",
        sample_rate=22050,
        language="vi",
    )
    alternate_preset = VoicePreset(
        voice_preset_id="voice-a",
        name="Speaker A",
        engine="vieneu",
        sample_rate=24000,
        language="vi",
    )

    import app.tts.factory as tts_factory

    monkeypatch.setattr(tts_factory, "create_tts_engine", lambda preset, project_root=None: _DummyEngine())

    result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=[
            {
                "segment_id": "seg-1",
                "segment_index": 0,
                "start_ms": 0,
                "end_ms": 1000,
                "tts_text": "Xin chao",
                "subtitle_text": "Xin chao",
                "translated_text": "Xin chao",
                "source_text": "Ni hao",
            },
            {
                "segment_id": "seg-2",
                "segment_index": 1,
                "start_ms": 1000,
                "end_ms": 2000,
                "tts_text": "Di thoi",
                "subtitle_text": "Di thoi",
                "translated_text": "Di thoi",
                "source_text": "Zou ba",
            },
        ],
        preset=default_preset,
        engine=_DummyEngine(),
        segment_voice_presets={"seg-2": alternate_preset},
        segment_speaker_keys={"seg-1": "char_narrator", "seg-2": "char_a"},
    )

    by_segment_id = {item.segment_id: item for item in result.artifacts}
    assert by_segment_id["seg-1"].voice_preset_id == "default-sapi"
    assert by_segment_id["seg-1"].speaker_key == "char_narrator"
    assert by_segment_id["seg-2"].voice_preset_id == "voice-a"
    assert by_segment_id["seg-2"].speaker_key == "char_a"
