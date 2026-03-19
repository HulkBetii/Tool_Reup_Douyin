from __future__ import annotations

from pathlib import Path

import pytest

from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest, SegmentRecord, SubtitleTrackRecord
from app.subtitle.editing import (
    build_segment_records,
    build_subtitle_event_records,
    format_timestamp_ms,
    merge_editor_rows,
    normalize_tts_text,
    parse_timestamp_ms,
    split_editor_row,
    suggest_subtitle_text,
    suggest_tts_text,
)


def test_timestamp_helpers_and_subtitle_fallback() -> None:
    assert format_timestamp_ms(3_723_004) == "01:02:03.004"
    assert parse_timestamp_ms("01:02:03.004") == 3_723_004
    assert parse_timestamp_ms("01:02:03,004") == 3_723_004
    assert suggest_subtitle_text(" Xin chao ", "Hello") == "Xin chao"
    assert suggest_subtitle_text("   ", " Hello ") == "Hello"
    assert normalize_tts_text(" Xin chao \n ban / nhe ") == "Xin chao ban, nhe"
    assert suggest_tts_text("Dong 1\nDong 2", "Ban dich", "Source") == "Dong 1 Dong 2"
    assert suggest_tts_text("", " Ban dich / tu nhien ", "Source") == "Ban dich, tu nhien"
    assert suggest_tts_text("", "", "Source", existing_tts_text=" Loi TTS \n rieng ") == "Loi TTS rieng"

    with pytest.raises(ValueError):
        parse_timestamp_ms("00:99:00.000")


def test_apply_segment_edits_updates_db(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Demo",
            root_dir=tmp_path / "project",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                segment_index=0,
                start_ms=0,
                end_ms=1200,
                source_text="Hello world",
                source_text_norm="Hello world",
                translated_text="Xin chao",
                translated_text_norm="Xin chao",
                subtitle_text="Xin chao",
                tts_text="Xin chao",
                status="translated",
            )
        ],
    )

    database.apply_segment_edits(
        workspace.project_id,
        [
            {
                "segment_id": "seg-1",
                "start_ms": 50,
                "end_ms": 1400,
                "translated_text": " Xin   chao ban ",
                "subtitle_text": "Xin chao ban",
                "tts_text": "Xin chao ban",
                "status": "edited",
            }
        ],
    )

    row = database.list_segments(workspace.project_id)[0]
    assert row["start_ms"] == 50
    assert row["end_ms"] == 1400
    assert row["translated_text"] == " Xin   chao ban "
    assert row["translated_text_norm"] == "Xin chao ban"
    assert row["subtitle_text"] == "Xin chao ban"
    assert row["tts_text"] == "Xin chao ban"
    assert row["status"] == "edited"


def test_split_merge_and_build_segment_records() -> None:
    row = {
        "segment_id": "seg-1",
        "segment_index": 0,
        "start_ms": 0,
        "end_ms": 4000,
        "source_lang": "en",
        "target_lang": "vi",
        "source_text": "hello world again",
        "translated_text": "xin chao the gioi nua",
        "subtitle_text": "xin chao the gioi nua",
        "tts_text": "xin chao the gioi nua",
        "audio_path": None,
        "status": "edited",
        "meta_json": {},
    }

    first, second = split_editor_row(row)

    assert first["start_ms"] == 0
    assert first["end_ms"] == 2000
    assert second["start_ms"] == 2000
    assert second["end_ms"] == 4000
    assert first["subtitle_text"]
    assert second["subtitle_text"]
    assert first["segment_id"] != second["segment_id"]

    merged = merge_editor_rows(first, second)
    assert merged["start_ms"] == 0
    assert merged["end_ms"] == 4000
    assert "xin chao" in merged["subtitle_text"]

    records = build_segment_records("project-1", [first, second])
    assert len(records) == 2
    assert records[0].segment_index == 0
    assert records[1].segment_index == 1
    assert records[0].project_id == "project-1"

    subtitle_records = build_subtitle_event_records("project-1", "track-1", [first, second])
    assert len(subtitle_records) == 2
    assert subtitle_records[0].track_id == "track-1"
    assert subtitle_records[0].event_index == 0
    assert subtitle_records[1].event_index == 1


def test_replace_segments_supports_structural_changes(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Demo",
            root_dir=tmp_path / "structural-project",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    initial_row = {
        "segment_id": "seg-1",
        "segment_index": 0,
        "start_ms": 0,
        "end_ms": 4000,
        "source_lang": "en",
        "target_lang": "vi",
        "source_text": "hello world again",
        "translated_text": "xin chao the gioi nua",
        "subtitle_text": "xin chao the gioi nua",
        "tts_text": "xin chao the gioi nua",
        "audio_path": None,
        "status": "edited",
        "meta_json": {},
    }

    first, second = split_editor_row(initial_row)
    database.replace_segments(workspace.project_id, build_segment_records(workspace.project_id, [first, second]))

    rows = database.list_segments(workspace.project_id)
    assert len(rows) == 2
    assert rows[0]["segment_index"] == 0
    assert rows[1]["segment_index"] == 1
    assert rows[0]["end_ms"] == rows[1]["start_ms"]


def test_replace_subtitle_events_keeps_canonical_segments_untouched(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Demo",
            root_dir=tmp_path / "subtitle-track-project",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                segment_index=0,
                start_ms=0,
                end_ms=2000,
                source_text="Hello world",
                source_text_norm="Hello world",
                translated_text="Xin chao",
                translated_text_norm="Xin chao",
                subtitle_text="Xin chao",
                tts_text="Xin chao",
                status="translated",
            )
        ],
    )
    database.sync_canonical_subtitle_track(workspace.project_id)

    user_track = database.create_subtitle_track(
        SubtitleTrackRecord(
            track_id=f"{workspace.project_id}:user:test",
            project_id=workspace.project_id,
            name="Edited Subtitle Track",
            kind="user",
            created_at="2026-03-18T00:00:00+00:00",
            updated_at="2026-03-18T00:00:00+00:00",
        ),
        set_active=True,
    )
    user_rows = [
        {
            "segment_id": "edited-1",
            "track_id": user_track["track_id"],
            "source_segment_id": "seg-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 2000,
            "source_lang": "en",
            "target_lang": "vi",
            "source_text": "Hello world",
            "translated_text": "Xin chao ca nha",
            "subtitle_text": "Xin chao ca nha",
            "tts_text": "Xin chao ca nha",
            "audio_path": None,
            "status": "edited",
            "meta_json": {},
        }
    ]
    database.replace_subtitle_events(
        workspace.project_id,
        user_track["track_id"],
        build_subtitle_event_records(workspace.project_id, user_track["track_id"], user_rows),
    )

    canonical_segments = database.list_segments(workspace.project_id)
    active_subtitles = database.list_subtitle_events(workspace.project_id)
    canonical_subtitles = database.list_subtitle_events(
        workspace.project_id,
        track_id=database.build_canonical_subtitle_track_id(workspace.project_id),
    )

    assert canonical_segments[0]["subtitle_text"] == "Xin chao"
    assert active_subtitles[0]["subtitle_text"] == "Xin chao ca nha"
    assert canonical_subtitles[0]["subtitle_text"] == "Xin chao"
