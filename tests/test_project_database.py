from __future__ import annotations

import sqlite3
from pathlib import Path

from app.project.database import CANONICAL_SUBTITLE_TRACK_KIND, ProjectDatabase
from app.project.models import ProjectRecord


def test_initialize_migrates_existing_v1_project_to_subtitle_tracks(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO metadata(key, value) VALUES ('schema_version', '1');

            CREATE TABLE projects (
                project_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                root_dir TEXT NOT NULL,
                source_language TEXT NOT NULL,
                target_language TEXT NOT NULL,
                video_asset_id TEXT,
                active_subtitle_track_id TEXT,
                active_voice_preset_id TEXT,
                active_export_preset_id TEXT,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE segments (
                segment_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                segment_index INTEGER NOT NULL,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER NOT NULL,
                source_lang TEXT,
                target_lang TEXT,
                source_text TEXT NOT NULL DEFAULT '',
                source_text_norm TEXT NOT NULL DEFAULT '',
                translated_text TEXT NOT NULL DEFAULT '',
                translated_text_norm TEXT NOT NULL DEFAULT '',
                subtitle_text TEXT NOT NULL DEFAULT '',
                tts_text TEXT NOT NULL DEFAULT '',
                audio_path TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                meta_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE media_assets (
                asset_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                type TEXT NOT NULL,
                path TEXT NOT NULL,
                sha256 TEXT,
                duration_ms INTEGER,
                fps REAL,
                width INTEGER,
                height INTEGER,
                audio_channels INTEGER,
                sample_rate INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE job_runs (
                job_id TEXT PRIMARY KEY,
                project_id TEXT,
                stage TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                input_hash TEXT NOT NULL DEFAULT '',
                output_paths_json TEXT NOT NULL DEFAULT '[]',
                log_path TEXT,
                error_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT NOT NULL,
                ended_at TEXT,
                retry_of_job_id TEXT,
                message TEXT NOT NULL DEFAULT ''
            );
            """
        )
        connection.execute(
            """
            INSERT INTO projects(
                project_id, name, root_dir, source_language, target_language,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "project-1",
                "Legacy",
                str(tmp_path),
                "en",
                "vi",
                "2026-03-18T00:00:00+00:00",
                "2026-03-18T00:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO segments(
                segment_id, project_id, segment_index, start_ms, end_ms,
                source_lang, source_text, source_text_norm, subtitle_text, tts_text, status, meta_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "seg-1",
                "project-1",
                0,
                0,
                1200,
                "en",
                "Hello",
                "Hello",
                "Xin chao",
                "Xin chao",
                "translated",
                "{}",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    database = ProjectDatabase(database_path)
    database.initialize()

    active_track = database.get_active_subtitle_track("project-1")
    subtitle_rows = database.list_subtitle_events("project-1")
    project_row = database.get_project()

    assert active_track is not None
    assert active_track["kind"] == CANONICAL_SUBTITLE_TRACK_KIND
    assert subtitle_rows[0]["segment_id"] == "seg-1"
    assert subtitle_rows[0]["source_segment_id"] == "seg-1"
    assert subtitle_rows[0]["subtitle_text"] == "Xin chao"
    assert project_row is not None
    assert project_row["active_watermark_profile_id"] is None


def test_project_database_persists_active_voice_export_and_watermark_presets(tmp_path: Path) -> None:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(tmp_path),
            source_language="en",
            target_language="vi",
            created_at="2026-03-19T00:00:00+00:00",
            updated_at="2026-03-19T00:00:00+00:00",
        )
    )

    assert database.get_active_voice_preset_id("project-1") is None
    assert database.get_active_export_preset_id("project-1") is None
    assert database.get_active_watermark_profile_id("project-1") is None

    database.set_active_voice_preset_id("project-1", "vieneu-default-vi")
    database.set_active_export_preset_id("project-1", "shorts-9x16")
    database.set_active_watermark_profile_id("project-1", "watermark-logo-top-right")

    project_row = database.get_project()

    assert database.get_active_voice_preset_id("project-1") == "vieneu-default-vi"
    assert database.get_active_export_preset_id("project-1") == "shorts-9x16"
    assert database.get_active_watermark_profile_id("project-1") == "watermark-logo-top-right"
    assert project_row is not None
    assert project_row["active_voice_preset_id"] == "vieneu-default-vi"
    assert project_row["active_export_preset_id"] == "shorts-9x16"
    assert project_row["active_watermark_profile_id"] == "watermark-logo-top-right"
