from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "rerun_contextual_downstream.py"
    spec = importlib.util.spec_from_file_location("rerun_contextual_downstream", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


class _FakeDatabase:
    def __init__(self, *, analyses: list[object], bindings: list[object]) -> None:
        self._analyses = analyses
        self._bindings = bindings

    def list_segment_analyses(self, _project_id: str | None = None) -> list[object]:
        return list(self._analyses)

    def list_speaker_bindings(self, _project_id: str | None = None) -> list[object]:
        return list(self._bindings)


def _preset(voice_preset_id: str) -> SimpleNamespace:
    return SimpleNamespace(voice_preset_id=voice_preset_id)


def test_rerun_contextual_downstream_blocks_partial_speaker_binding_config(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-speaker-binding-partial-config-blocks-tts.json")

    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: [_preset("voice-a"), _preset("voice-b")],
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
    )

    with pytest.raises(RuntimeError, match="Speaker binding hien chua day du"):
        module._resolve_segment_voice_plan(
            workspace=workspace,
            database=database,
            segments=list(fixture["subtitle_rows"]),
            default_preset=_preset("default-sapi"),
        )


def test_rerun_contextual_downstream_ignores_unknown_placeholder_speakers(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-speaker-binding-unknown-placeholder-falls-back.json")

    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: [_preset("voice-a")],
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
    )

    segment_voice_presets, segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_bindings is True
    assert plan.unresolved_speakers == []
    assert segment_voice_presets is None
    assert segment_speaker_keys is None


def test_rerun_contextual_downstream_returns_per_segment_voice_plan_when_bindings_are_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-speaker-binding-partial-config-blocks-tts.json")
    binding_rows = list(fixture["binding_rows"]) + [
        {
            "binding_id": "bind:character:char_b",
            "project_id": "project-1",
            "speaker_type": "character",
            "speaker_key": "char_b",
            "voice_preset_id": "voice-b",
        }
    ]

    available_presets = {
        "voice-a": _preset("voice-a"),
        "voice-b": _preset("voice-b"),
    }
    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: list(available_presets.values()),
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=binding_rows,
    )

    segment_voice_presets, segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_bindings is True
    assert plan.unresolved_speakers == []
    assert plan.missing_preset_ids == []
    assert segment_speaker_keys == {"evt-1": "char_a", "evt-2": "char_b"}
    assert segment_voice_presets is not None
    assert {key: value.voice_preset_id for key, value in segment_voice_presets.items()} == {
        "evt-1": "voice-a",
        "evt-2": "voice-b",
    }
