from __future__ import annotations

from pathlib import Path

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
