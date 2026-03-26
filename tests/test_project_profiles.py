from __future__ import annotations

import json
from pathlib import Path

from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest
from app.project.profiles import (
    ensure_project_profile_state,
    ensure_project_profiles,
    list_project_profiles,
    load_project_profile_state,
    resolve_subtitle_subtext_mode,
    resolve_project_profile_mix_defaults,
    set_project_subtitle_subtext_mode,
)


def test_ensure_project_profiles_writes_default_narration_profile(tmp_path: Path) -> None:
    project_root = tmp_path / "project"

    ensure_project_profiles(project_root)
    profiles = list_project_profiles(project_root)

    assert {profile.project_profile_id for profile in profiles} == {
        "zh-vi-narration-clear-vieneu",
        "zh-vi-narration-fast-vieneu",
        "zh-vi-narration-fast-v2-vieneu",
    }


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
    assert profile_state.subtitle_subtext_mode == "off"


def test_bootstrap_project_applies_requested_narration_fast_profile(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Fast Demo",
            root_dir=tmp_path / "narration-fast-project",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    profile_state = load_project_profile_state(workspace.root_dir)

    assert database.get_translation_mode(workspace.project_id) == "contextual_v2"
    assert database.get_active_voice_preset_id(workspace.project_id) == "vieneu-default-vi"
    assert profile_state is not None
    assert profile_state.project_profile_id == "zh-vi-narration-fast-vieneu"
    assert profile_state.recommended_prompt_template_id == "contextual_narration_fast_adaptation"
    assert profile_state.subtitle_subtext_mode == "off"


def test_bootstrap_project_applies_requested_narration_fast_v2_profile(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Fast V2 Demo",
            root_dir=tmp_path / "narration-fast-v2-project",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-v2-vieneu",
        )
    )
    profile_state = load_project_profile_state(workspace.root_dir)

    assert profile_state is not None
    assert profile_state.project_profile_id == "zh-vi-narration-fast-v2-vieneu"
    assert profile_state.recommended_prompt_template_id == "contextual_narration_slot_rewrite"
    assert profile_state.subtitle_subtext_mode == "off"


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


def test_project_profile_state_defaults_and_updates_subtitle_subtext_mode(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    state = ensure_project_profile_state(
        project_root,
        project_profile_id="manual",
        name="Manual",
        applied_at="2026-03-26T00:00:00+00:00",
    )
    updated_state = set_project_subtitle_subtext_mode(
        project_root,
        "source_text",
        applied_at="2026-03-26T00:10:00+00:00",
    )

    assert state.subtitle_subtext_mode == "off"
    assert resolve_subtitle_subtext_mode(project_root) == "source_text"
    assert updated_state.subtitle_subtext_mode == "source_text"
