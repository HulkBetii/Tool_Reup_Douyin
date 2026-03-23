from __future__ import annotations

import json
from pathlib import Path
import wave

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


class _CountingEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Path]] = []

    def synthesize(self, *, text: str, output_path: Path, preset: VoicePreset) -> SynthesisResult:
        self.calls.append((text, output_path))
        _write_pcm_wav(output_path, sample_rate=preset.sample_rate, duration_ms=320)
        return SynthesisResult(
            wav_path=output_path,
            duration_ms=320,
            sample_rate=preset.sample_rate,
            voice_id=preset.voice_id,
        )


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _write_pcm_wav(path: Path, *, sample_rate: int, duration_ms: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = int(sample_rate * duration_ms / 1000)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * max(1, frames))


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


def test_build_tts_stage_hash_ignores_subtitle_only_edits_when_tts_text_is_unchanged() -> None:
    preset = VoicePreset(
        voice_preset_id="default-sapi",
        name="Default",
        engine="sapi",
        sample_rate=22050,
    )
    base_segments = [
        {
            "segment_id": "seg-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 1000,
            "tts_text": "Xin chao moi nguoi",
            "subtitle_text": "Xin chao moi nguoi",
            "translated_text": "Xin chao moi nguoi",
        }
    ]
    subtitle_only_edit = [
        {
            **base_segments[0],
            "subtitle_text": "Xin chao tat ca moi nguoi",
        }
    ]

    assert build_tts_stage_hash(base_segments, preset) == build_tts_stage_hash(subtitle_only_edit, preset)


def test_synthesize_segments_reuses_shared_tts_clip_cache_across_stage_hash_changes(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    preset = VoicePreset(
        voice_preset_id="vieneu-default",
        name="VieNeu",
        engine="vieneu",
        sample_rate=24000,
        language="vi",
    )
    base_segments = [
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
    ]
    updated_segments = [
        base_segments[0],
        {
            **base_segments[1],
            "tts_text": "Di thoi nhe",
            "subtitle_text": "Di thoi nhe",
            "translated_text": "Di thoi nhe",
        },
    ]

    first_engine = _CountingEngine()
    first_result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=base_segments,
        preset=preset,
        engine=first_engine,
    )
    second_engine = _CountingEngine()
    second_result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=updated_segments,
        preset=preset,
        engine=second_engine,
    )

    first_by_segment = {item.segment_id: item for item in first_result.artifacts}
    second_by_segment = {item.segment_id: item for item in second_result.artifacts}

    assert len(first_engine.calls) == 2
    assert len(second_engine.calls) == 1
    assert first_by_segment["seg-1"].raw_wav_path == second_by_segment["seg-1"].raw_wav_path
    assert first_by_segment["seg-2"].raw_wav_path != second_by_segment["seg-2"].raw_wav_path


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


def test_synthesize_segments_keeps_cached_duration_metadata(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("tts-cached-artifact-duration-preserved.json")
    workspace = _workspace(tmp_path)
    preset = VoicePreset(
        voice_preset_id="vieneu-default",
        name="VieNeu",
        engine="vieneu",
        sample_rate=24000,
        language="vi",
    )
    stage_hash = build_tts_stage_hash([fixture["segment"]], preset)
    cache_dir = workspace.cache_dir / "tts" / stage_hash
    raw_dir = cache_dir / "raw"
    output_path = raw_dir / "0000_seg-1.wav"
    cached_artifact = dict(fixture["cached_artifact"])
    _write_pcm_wav(output_path, sample_rate=int(cached_artifact["sample_rate"]), duration_ms=int(cached_artifact["duration_ms"]))
    manifest_path = cache_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "stage_hash": stage_hash,
                "voice_preset": preset.model_dump(mode="json"),
                "artifacts": [
                    {
                        "segment_id": "seg-1",
                        "segment_index": 0,
                        "start_ms": 0,
                        "end_ms": 1000,
                        "text": fixture["segment"]["tts_text"],
                        "raw_wav_path": str(output_path),
                        "duration_ms": cached_artifact["duration_ms"],
                        "sample_rate": cached_artifact["sample_rate"],
                        "voice_id": cached_artifact["voice_id"],
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=[fixture["segment"]],
        preset=preset,
        engine=_DummyEngine(),
    )

    assert len(result.artifacts) == 1
    artifact = result.artifacts[0]
    assert artifact.duration_ms == int(cached_artifact["duration_ms"])
    assert artifact.sample_rate == int(cached_artifact["sample_rate"])
    assert artifact.voice_id == str(cached_artifact["voice_id"])


def test_synthesize_segments_probes_cached_wav_duration_when_manifest_is_missing(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("tts-cached-artifact-duration-preserved.json")
    workspace = _workspace(tmp_path)
    preset = VoicePreset(
        voice_preset_id="vieneu-default",
        name="VieNeu",
        engine="vieneu",
        sample_rate=24000,
        language="vi",
    )
    stage_hash = build_tts_stage_hash([fixture["segment"]], preset)
    output_path = workspace.cache_dir / "tts" / stage_hash / "raw" / "0000_seg-1.wav"
    _write_pcm_wav(
        output_path,
        sample_rate=int(fixture["cached_artifact"]["sample_rate"]),
        duration_ms=int(fixture["cached_artifact"]["duration_ms"]),
    )

    result = synthesize_segments(
        _DummyContext(),
        workspace=workspace,
        segments=[fixture["segment"]],
        preset=preset,
        engine=_DummyEngine(),
    )

    assert len(result.artifacts) == 1
    assert result.artifacts[0].duration_ms == int(fixture["cached_artifact"]["duration_ms"])
