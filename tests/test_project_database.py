from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.project.database import CANONICAL_SUBTITLE_TRACK_KIND, ProjectDatabase
from app.project.models import (
    ProjectRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
    SegmentRecord,
    SpeakerBindingRecord,
)
from app.translate.relationship_memory import build_locked_relationship_record, relationship_record_from_row


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
    assert project_row["translation_mode"] == "legacy"


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


def test_project_database_preserves_allowed_alternates_when_relationship_is_relocked(tmp_path: Path) -> None:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(tmp_path),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        )
    )
    database.upsert_relationship_profiles(
        [
            RelationshipProfileRecord(
                relationship_id="rel:char_a->char_b",
                project_id="project-1",
                from_character_id="char_a",
                to_character_id="char_b",
                relation_type="siblings",
                default_self_term="anh",
                default_address_term="em",
                allowed_alternates_json={"self_terms": ["tao"], "address_terms": ["mày"]},
                scope="global",
                status="confirmed",
                evidence_segment_ids_json=["seg-10"],
                notes="existing policy",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:05:00+00:00",
            )
        ]
    )

    existing_row = database.list_relationship_profiles("project-1")[0]
    locked_record = build_locked_relationship_record(
        existing=relationship_record_from_row(existing_row, project_id="project-1"),
        project_id="project-1",
        relationship_id="rel:char_a->char_b",
        speaker_id="char_a",
        listener_id="char_b",
        self_term="anh",
        address_term="em",
        now="2026-03-20T00:10:00+00:00",
    )
    database.upsert_relationship_profiles([locked_record])

    updated_row = database.list_relationship_profiles("project-1")[0]

    assert updated_row["status"] == "locked_by_human"
    assert updated_row["relation_type"] == "siblings"
    assert updated_row["notes"] == "existing policy"
    assert json.loads(updated_row["allowed_alternates_json"]) == {"self_terms": ["tao"], "address_terms": ["mày"]}


def test_project_database_replaces_and_lists_speaker_bindings(tmp_path: Path) -> None:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(tmp_path),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        )
    )

    database.replace_speaker_bindings(
        "project-1",
        [
            SpeakerBindingRecord(
                binding_id="bind:character:char_a",
                project_id="project-1",
                speaker_type="character",
                speaker_key="char_a",
                voice_preset_id="voice-a",
                notes="main narrator",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    rows = database.list_speaker_bindings("project-1")

    assert len(rows) == 1
    assert rows[0]["speaker_type"] == "character"
    assert rows[0]["speaker_key"] == "char_a"
    assert rows[0]["voice_preset_id"] == "voice-a"
    assert rows[0]["notes"] == "main narrator"


def test_project_database_persists_contextual_translation_state(tmp_path: Path) -> None:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(tmp_path),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            created_at="2026-03-19T00:00:00+00:00",
            updated_at="2026-03-19T00:00:00+00:00",
        )
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id="project-1",
                segment_index=0,
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text="走吧",
                source_text_norm="走吧",
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id="scene_0000",
                project_id="project-1",
                scene_index=0,
                start_segment_index=0,
                end_segment_index=0,
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A thúc B đi nhanh lên",
                created_at="2026-03-19T00:00:00+00:00",
                updated_at="2026-03-19T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id="seg-1",
                project_id="project-1",
                scene_id="scene_0000",
                segment_index=0,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "confidence": 1.0}],
                honorific_policy_json={
                    "policy_id": "rel:char_a->char_b",
                    "self_term": "anh",
                    "address_term": "em",
                    "confidence": 1.0,
                },
                semantic_translation="Đi thôi",
                approved_subtitle_text="Đi thôi",
                approved_tts_text="Đi thôi em",
                semantic_qc_passed=True,
                review_status="approved",
                created_at="2026-03-19T00:00:00+00:00",
                updated_at="2026-03-19T00:00:00+00:00",
            )
        ],
    )
    database.apply_segment_analysis_outputs("project-1", target_language="vi")

    scene_rows = database.list_scene_memories("project-1")
    analysis_rows = database.list_segment_analyses("project-1")
    segment_rows = database.list_segments("project-1")

    assert len(scene_rows) == 1
    assert len(analysis_rows) == 1
    assert segment_rows[0]["subtitle_text"] == "Đi thôi"
    assert segment_rows[0]["tts_text"] == "Đi thôi em"


def test_project_database_review_queue_roundtrip(tmp_path: Path) -> None:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(tmp_path),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            created_at="2026-03-19T00:00:00+00:00",
            updated_at="2026-03-19T00:00:00+00:00",
        )
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id="project-1",
                segment_index=0,
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text="走吧",
                source_text_norm="走吧",
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id="scene_0000",
                project_id="project-1",
                scene_index=0,
                start_segment_index=0,
                end_segment_index=0,
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A thúc B đi nhanh lên",
                created_at="2026-03-19T00:00:00+00:00",
                updated_at="2026-03-19T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id="seg-1",
                project_id="project-1",
                scene_id="scene_0000",
                segment_index=0,
                speaker_json={"character_id": "char_a", "confidence": 0.9},
                listeners_json=[{"character_id": "char_b", "confidence": 0.6}],
                honorific_policy_json={
                    "policy_id": "rel:char_a->char_b",
                    "self_term": "anh",
                    "address_term": "em",
                    "confidence": 0.8,
                },
                semantic_translation="Đi thôi",
                approved_subtitle_text="Đi thôi",
                approved_tts_text="Đi thôi em",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["LISTENER_AMBIGUOUS"],
                review_question="A đang nói với B hay cả nhóm?",
                semantic_qc_passed=False,
                semantic_qc_issues_json=[
                    {
                        "code": "addressee_mismatch",
                        "severity": "warning",
                        "message": "Người nghe còn mơ hồ.",
                    }
                ],
                created_at="2026-03-19T00:00:00+00:00",
                updated_at="2026-03-19T00:00:00+00:00",
            )
        ],
    )

    assert database.count_pending_segment_reviews("project-1") == 1
    review_rows = database.list_review_queue_items("project-1")
    assert len(review_rows) == 1
    assert review_rows[0]["source_text"] == "走吧"

    database.update_segment_analysis_review(
        "project-1",
        "seg-1",
        speaker_json={"character_id": "char_a", "confidence": 1.0},
        listeners_json=[{"character_id": "char_b", "confidence": 1.0}],
        approved_subtitle_text="Đi thôi em",
        approved_tts_text="Đi thôi em",
        needs_human_review=False,
        review_status="locked",
        review_scope="line",
        review_reason_codes_json=[],
        review_question="",
        semantic_qc_passed=True,
        semantic_qc_issues_json=[],
    )
    database.apply_segment_analysis_outputs("project-1", target_language="vi")

    assert database.count_pending_segment_reviews("project-1") == 0
    assert database.list_review_queue_items("project-1") == []

    analysis_row = database.get_segment_analysis("project-1", "seg-1")
    segment_row = database.list_segments("project-1")[0]

    assert analysis_row is not None
    assert analysis_row["review_status"] == "locked"
    assert analysis_row["semantic_qc_passed"] == 1
    assert segment_row["subtitle_text"] == "Đi thôi em"
    assert segment_row["tts_text"] == "Đi thôi em"
    assert segment_row["status"] == "translated"
