from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.project.bootstrap import bootstrap_project
from app.project.database import CANONICAL_SUBTITLE_TRACK_KIND, ProjectDatabase
from app.project.models import ProjectInitRequest


def test_bootstrap_project_creates_layout_and_database(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Demo",
            root_dir=tmp_path / "demo-project",
            source_language="auto",
            target_language="vi",
        )
    )

    assert workspace.root_dir.exists()
    assert workspace.database_path.exists()
    assert workspace.project_json_path.exists()
    assert (workspace.root_dir / "cache" / "asr").exists()
    assert (workspace.root_dir / "assets" / "voices").exists()
    assert (workspace.root_dir / "presets" / "prompts" / "default-vi-style.json").exists()
    assert not (workspace.root_dir / "presets" / "prompts" / "contextual_default_adaptation.json").exists()
    assert not (workspace.root_dir / "presets" / "prompts" / "contextual_cartoon_fun_adaptation.json").exists()
    assert (workspace.root_dir / "presets" / "exports" / "default_hardsub.json").exists()
    assert (workspace.root_dir / "presets" / "exports" / "shorts_9x16.json").exists()
    assert (workspace.root_dir / "presets" / "watermarks" / "none.json").exists()
    assert (workspace.root_dir / "presets" / "watermarks" / "logo_top_right.json").exists()
    assert (workspace.root_dir / "presets" / "voices" / "vieneu_vi.json").exists()
    assert (workspace.root_dir / "presets" / "voices" / "vieneu_clone_template.json").exists()

    payload = json.loads(workspace.project_json_path.read_text(encoding="utf-8"))
    assert payload["name"] == "Demo"
    assert payload["target_language"] == "vi"
    assert payload["active_watermark_profile_id"] == "watermark-none"
    assert payload["translation_mode"] == "legacy"

    connection = sqlite3.connect(workspace.database_path)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        connection.close()

    assert {
        "metadata",
        "projects",
        "media_assets",
        "segments",
        "subtitle_tracks",
        "subtitle_events",
        "job_runs",
        "character_profiles",
        "relationship_profiles",
        "scene_memories",
        "segment_analyses",
    } <= tables

    database = ProjectDatabase(workspace.database_path)
    active_track = database.get_active_subtitle_track(workspace.project_id)
    assert active_track is not None
    assert active_track["kind"] == CANONICAL_SUBTITLE_TRACK_KIND
    assert database.count_subtitle_events(workspace.project_id) == 0


def test_bootstrap_project_defaults_to_contextual_mode_for_zh_to_vi(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Demo",
            root_dir=tmp_path / "demo-project",
            source_language="zh",
            target_language="vi",
        )
    )

    payload = json.loads(workspace.project_json_path.read_text(encoding="utf-8"))
    assert payload["translation_mode"] == "contextual_v2"
    assert (workspace.root_dir / "presets" / "prompts" / "contextual_default_adaptation.json").exists()
    assert (workspace.root_dir / "presets" / "prompts" / "contextual_cartoon_fun_adaptation.json").exists()


def test_bootstrap_project_rejects_existing_project_root(tmp_path: Path) -> None:
    root_dir = tmp_path / "duplicate-project"
    bootstrap_project(ProjectInitRequest(name="Demo", root_dir=root_dir))

    with pytest.raises(FileExistsError):
        bootstrap_project(ProjectInitRequest(name="Demo 2", root_dir=root_dir))
