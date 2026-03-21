from __future__ import annotations

import json
from pathlib import Path

from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest
from app.project.profiles import (
    ensure_project_profiles,
    list_project_profiles,
    load_project_profile_state,
    resolve_project_profile_mix_defaults,
)


def test_ensure_project_profiles_writes_default_narration_profile(tmp_path: Path) -> None:
    project_root = tmp_path / "project"

    ensure_project_profiles(project_root)
    profiles = list_project_profiles(project_root)

    assert {profile.project_profile_id for profile in profiles} == {"zh-vi-narration-clear-vieneu"}


def test_bootstrap_project_applies_requested_project_profile(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Demo",
            root_dir=tmp_path / "narration-project",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-clear-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)

    voice_preset_payload = json.loads(
        (workspace.root_dir / "presets" / "voices" / "vieneu_vi.json").read_text(encoding="utf-8")
    )
    style_preset_payload = json.loads(
        (workspace.root_dir / "presets" / "styles" / "default_ass_style.json").read_text(encoding="utf-8")
    )
    profile_state = load_project_profile_state(workspace.root_dir)

    assert database.get_translation_mode(workspace.project_id) == "contextual_v2"
    assert database.get_active_voice_preset_id(workspace.project_id) == "vieneu-default-vi"
    assert database.get_active_export_preset_id(workspace.project_id) == "youtube-16x9"
    assert database.get_active_watermark_profile_id(workspace.project_id) == "watermark-none"
    assert voice_preset_payload["speed"] == 0.93
    assert style_preset_payload["ass_style_json"]["FontSize"] == 12
    assert profile_state is not None
    assert profile_state.project_profile_id == "zh-vi-narration-clear-vieneu"
    assert profile_state.recommended_original_volume == 0.07
    assert profile_state.recommended_prompt_template_id == "contextual_default_adaptation"


def test_resolve_project_profile_mix_defaults_uses_profile_state(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Demo",
            root_dir=tmp_path / "narration-project",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-clear-vieneu",
        )
    )

    original_volume, voice_volume, state = resolve_project_profile_mix_defaults(
        workspace.root_dir,
        original_volume=None,
        voice_volume=None,
    )
    overridden_original_volume, overridden_voice_volume, _ = resolve_project_profile_mix_defaults(
        workspace.root_dir,
        original_volume=0.05,
        voice_volume=None,
    )

    assert state is not None
    assert original_volume == 0.07
    assert voice_volume == 1.0
    assert overridden_original_volume == 0.05
    assert overridden_voice_volume == 1.0
