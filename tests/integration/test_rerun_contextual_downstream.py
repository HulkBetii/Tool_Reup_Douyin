from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from app.tts.models import VoicePreset


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
    def __init__(
        self,
        *,
        analyses: list[object],
        bindings: list[object],
        voice_policies: list[object] | None = None,
        relationship_rows: list[object] | None = None,
        register_style_policies: list[object] | None = None,
    ) -> None:
        self._analyses = analyses
        self._bindings = bindings
        self._voice_policies = voice_policies or []
        self._relationship_rows = relationship_rows or []
        self._register_style_policies = register_style_policies or []

    def list_segment_analyses(self, _project_id: str | None = None) -> list[object]:
        return list(self._analyses)

    def list_speaker_bindings(self, _project_id: str | None = None) -> list[object]:
        return list(self._bindings)

    def list_voice_policies(self, _project_id: str | None = None) -> list[object]:
        return list(self._voice_policies)

    def list_relationship_profiles(self, _project_id: str | None = None) -> list[object]:
        return list(self._relationship_rows)

    def list_register_voice_style_policies(self, _project_id: str | None = None) -> list[object]:
        return list(self._register_style_policies)


def _preset(voice_preset_id: str, **overrides) -> VoicePreset:
    payload = {
        "voice_preset_id": voice_preset_id,
        "name": voice_preset_id,
        "engine": "sapi",
        "sample_rate": 22050,
    }
    payload.update(overrides)
    return VoicePreset(**payload)


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

    with pytest.raises(RuntimeError, match="Voice plan hien chua day du"):
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


def test_rerun_contextual_downstream_uses_character_voice_policy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-character-voice-policy-fallback.json")

    available_presets = {
        "voice-a": _preset("voice-a"),
    }
    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: list(available_presets.values()),
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
    )

    segment_voice_presets, segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_voice_policies is True
    assert plan.character_policy_hits == 1
    assert segment_speaker_keys == {"evt-1": "char_a", "evt-2": "char_c"}
    assert segment_voice_presets is not None
    assert {key: value.voice_preset_id for key, value in segment_voice_presets.items()} == {"evt-1": "voice-a"}


def test_rerun_contextual_downstream_relationship_policy_overrides_character_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-relationship-voice-policy-overrides-character.json")

    available_presets = {
        "voice-a": _preset("voice-a"),
        "voice-rel": _preset("voice-rel"),
    }
    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: list(available_presets.values()),
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
    )

    segment_voice_presets, _segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_voice_policies is True
    assert plan.relationship_policy_hits == 1
    assert segment_voice_presets is not None
    assert {key: value.voice_preset_id for key, value in segment_voice_presets.items()} == {"evt-1": "voice-rel"}


def test_rerun_contextual_downstream_applies_style_only_character_voice_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-character-voice-style-fallback.json")

    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: [],
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
    )

    segment_voice_presets, segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_voice_policies is True
    assert plan.character_style_hits == 1
    assert segment_speaker_keys == {"evt-1": "char_a"}
    assert segment_voice_presets is not None
    assert segment_voice_presets["evt-1"].voice_preset_id == "default-sapi"
    assert segment_voice_presets["evt-1"].speed == 0.92
    assert segment_voice_presets["evt-1"].volume == 1.15
    assert segment_voice_presets["evt-1"].pitch == -1.5


def test_rerun_contextual_downstream_keeps_binding_preset_but_uses_policy_prosody(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-explicit-binding-keeps-preset-but-uses-policy-prosody.json")

    available_presets = {
        "voice-bind": _preset("voice-bind", engine="vieneu", sample_rate=24000),
    }
    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: list(available_presets.values()),
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
    )

    segment_voice_presets, _segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi"),
    )

    assert plan.active_bindings is True
    assert plan.relationship_style_hits == 1
    assert segment_voice_presets is not None
    assert segment_voice_presets["evt-1"].voice_preset_id == "voice-bind"
    assert segment_voice_presets["evt-1"].speed == 0.93
    assert segment_voice_presets["evt-1"].pitch == 2.0


def test_rerun_contextual_downstream_applies_register_voice_style_and_reports_hits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-register-voice-style-fallback.json")

    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: [],
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
        relationship_rows=list(fixture["relationship_rows"]),
        register_style_policies=list(fixture["register_style_policy_rows"]),
    )

    segment_voice_presets, _segment_speaker_keys, plan = module._resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=list(fixture["subtitle_rows"]),
        default_preset=_preset("default-sapi", speed=1.0, volume=1.0, pitch=0.0),
    )

    assert plan.active_register_voice_styles is True
    assert plan.register_style_hits == 1
    assert segment_voice_presets is not None
    assert segment_voice_presets["evt-1"].voice_preset_id == "default-sapi"
    assert segment_voice_presets["evt-1"].speed == 0.88
    assert segment_voice_presets["evt-1"].volume == 1.05
    assert segment_voice_presets["evt-1"].pitch == -0.6


def test_rerun_contextual_downstream_blocks_when_selected_voice_policy_preset_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    fixture = _load_regression_fixture("zh-vi-voice-policy-missing-preset-blocks.json")

    monkeypatch.setattr(
        module,
        "list_voice_presets",
        lambda _root_dir: [_preset("voice-a")],
    )

    workspace = SimpleNamespace(root_dir=Path("C:/demo"), project_id="project-1")
    database = _FakeDatabase(
        analyses=list(fixture["analysis_rows"]),
        bindings=list(fixture["binding_rows"]),
        voice_policies=list(fixture["voice_policy_rows"]),
    )

    with pytest.raises(RuntimeError, match="Preset khong con ton tai"):
        module._resolve_segment_voice_plan(
            workspace=workspace,
            database=database,
            segments=list(fixture["subtitle_rows"]),
            default_preset=_preset("default-sapi"),
        )
