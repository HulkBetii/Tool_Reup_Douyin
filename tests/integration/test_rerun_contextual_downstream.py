from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from app.ops.models import DoctorCheckResult, DoctorReport
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


def test_rerun_contextual_downstream_creates_backup_before_running(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_script_module()
    workspace = SimpleNamespace(
        root_dir=tmp_path,
        project_id="project-1",
        database_path=tmp_path / "project.db",
        project_json_path=tmp_path / "project.json",
        cache_dir=tmp_path / "cache",
        exports_dir=tmp_path / "exports",
        source_video_path=tmp_path / "source.mp4",
    )
    workspace.root_dir.mkdir(parents=True, exist_ok=True)
    workspace.cache_dir.mkdir(parents=True, exist_ok=True)
    workspace.exports_dir.mkdir(parents=True, exist_ok=True)
    workspace.project_json_path.write_text("{}", encoding="utf-8")
    workspace.database_path.write_text("", encoding="utf-8")
    workspace.source_video_path.write_bytes(b"video")

    class _RuntimeDatabase:
        def get_project(self):
            return {"project_id": "project-1"}

        def count_pending_segment_reviews(self, _project_id):
            return 0

        def list_segment_analyses(self, _project_id):
            return [{"semantic_qc_passed": 1}]

        def set_active_voice_preset_id(self, _project_id, _voice_preset_id):
            return None

        def set_active_export_preset_id(self, _project_id, _export_preset_id):
            return None

        def list_segments(self, _project_id):
            return [{"segment_id": "evt-1", "end_ms": 1000}]

        def get_primary_video_asset(self, _project_id):
            return {"path": str(workspace.source_video_path)}

    fake_database = _RuntimeDatabase()
    backup_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(module, "load_settings", lambda: SimpleNamespace(dependency_paths=SimpleNamespace(ffmpeg_path=None, ffprobe_path=None)))
    monkeypatch.setattr(module, "open_project", lambda _path: workspace)
    monkeypatch.setattr(module, "ProjectDatabase", lambda _path: fake_database)
    monkeypatch.setattr(module, "sync_project_snapshot", lambda _workspace: None)
    monkeypatch.setattr(module, "_resolve_voice_preset", lambda _root, _voice_id: _preset("vieneu-default-vi", engine="vieneu", sample_rate=24000))
    monkeypatch.setattr(
        module,
        "run_doctor",
        lambda **_kwargs: DoctorReport(generated_at="2026-03-20T00:00:00+00:00", checks=[], requested_stages=("tts",)),
    )
    monkeypatch.setattr(module, "inspect_workspace", lambda _workspace: SimpleNamespace(error_count=0, warning_count=0))
    monkeypatch.setattr(
        module,
        "create_workspace_backup",
        lambda _workspace, *, reason, stage: backup_calls.append((reason, stage))
        or SimpleNamespace(backup_dir=tmp_path / ".ops" / "backups" / "demo"),
    )
    monkeypatch.setattr(
        module,
        "_resolve_segment_voice_plan",
        lambda **_kwargs: (
            None,
            None,
            SimpleNamespace(
                active_bindings=False,
                active_voice_policies=False,
                unresolved_speakers=[],
                missing_preset_ids=[],
                segment_voice_preset_ids={},
                character_policy_hits=0,
                relationship_policy_hits=0,
                character_style_hits=0,
                relationship_style_hits=0,
                register_style_hits=0,
            ),
        ),
    )
    original_audio_path = tmp_path / "orig.wav"
    original_audio_path.write_bytes(b"orig")
    monkeypatch.setattr(module, "_resolve_original_audio", lambda _workspace, _database, _settings: SimpleNamespace(audio_48k_path=original_audio_path))
    tts_manifest_path = tmp_path / "tts_manifest.json"
    tts_manifest_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "synthesize_segments", lambda *args, **kwargs: SimpleNamespace(artifacts=[], manifest_path=tts_manifest_path))
    voice_track_path = tmp_path / "voice.wav"
    voice_track_path.write_bytes(b"voice")
    monkeypatch.setattr(module, "build_voice_track", lambda *args, **kwargs: SimpleNamespace(voice_track_path=voice_track_path))
    mixed_audio_path = tmp_path / "mixed.wav"
    mixed_audio_path.write_bytes(b"mixed")
    monkeypatch.setattr(module, "mix_audio_tracks", lambda *args, **kwargs: SimpleNamespace(mixed_audio_path=mixed_audio_path))
    monkeypatch.setattr(module, "export_subtitles", lambda _workspace, *, segments, format_name, allow_source_fallback: tmp_path / f"track.{format_name}")
    monkeypatch.setattr(module, "export_hardsub_video", lambda *args, **kwargs: workspace.exports_dir / "out.mp4")
    monkeypatch.setattr(module, "create_tts_engine", lambda _preset, project_root=None: object())
    monkeypatch.setattr(module.sys, "argv", ["rerun_contextual_downstream.py", "--project-root", str(tmp_path)])

    exit_code = module.main()

    assert exit_code == 0
    assert backup_calls == [("Safe rerun downstream before TTS -> export", "rerun_downstream")]


def test_rerun_contextual_downstream_blocks_when_doctor_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_script_module()
    workspace = SimpleNamespace(
        root_dir=tmp_path,
        project_id="project-1",
        database_path=tmp_path / "project.db",
        project_json_path=tmp_path / "project.json",
        cache_dir=tmp_path / "cache",
        exports_dir=tmp_path / "exports",
        source_video_path=tmp_path / "source.mp4",
    )
    workspace.root_dir.mkdir(parents=True, exist_ok=True)
    workspace.project_json_path.write_text("{}", encoding="utf-8")
    workspace.database_path.write_text("", encoding="utf-8")

    class _RuntimeDatabase:
        def get_project(self):
            return {"project_id": "project-1"}

        def count_pending_segment_reviews(self, _project_id):
            return 0

        def list_segment_analyses(self, _project_id):
            return [{"semantic_qc_passed": 1}]

        def set_active_voice_preset_id(self, _project_id, _voice_preset_id):
            return None

        def set_active_export_preset_id(self, _project_id, _export_preset_id):
            return None

    monkeypatch.setattr(module, "load_settings", lambda: SimpleNamespace(dependency_paths=SimpleNamespace(ffmpeg_path=None, ffprobe_path=None)))
    monkeypatch.setattr(module, "open_project", lambda _path: workspace)
    monkeypatch.setattr(module, "ProjectDatabase", lambda _path: _RuntimeDatabase())
    monkeypatch.setattr(module, "sync_project_snapshot", lambda _workspace: None)
    monkeypatch.setattr(module, "_resolve_voice_preset", lambda _root, _voice_id: _preset("vieneu-default-vi", engine="vieneu", sample_rate=24000))
    monkeypatch.setattr(
        module,
        "run_doctor",
        lambda **_kwargs: DoctorReport(
            generated_at="2026-03-20T00:00:00+00:00",
            checks=[
                DoctorCheckResult(
                    name="ffmpeg",
                    status="error",
                    message="Not found",
                    fix_hint="Configure ffmpeg",
                    blocking_stages=("mixdown", "export_video"),
                )
            ],
            requested_stages=("tts", "voice_track", "mixdown", "export_video"),
        ),
    )
    monkeypatch.setattr(module.sys, "argv", ["rerun_contextual_downstream.py", "--project-root", str(tmp_path)])

    with pytest.raises(RuntimeError, match="Blocked because rerun downstream"):
        module.main()
