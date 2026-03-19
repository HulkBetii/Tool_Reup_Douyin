from __future__ import annotations

import json
from pathlib import Path

from app.asr.models import SegmentDraft, TranscriptionOptions, TranscriptionResult, WordTimestamp
from app.asr.persistence import persist_transcription_result
from app.project.database import ProjectDatabase
from app.project.models import ProjectRecord, ProjectWorkspace


def test_persist_transcription_result_writes_cache_and_replaces_segments(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()

    database_path = root_dir / "project.db"
    project_json_path = root_dir / "project.json"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id="project-1",
            name="Demo",
            root_dir=str(root_dir),
            source_language="auto",
            target_language="vi",
            created_at="2026-03-18T00:00:00+00:00",
            updated_at="2026-03-18T00:00:00+00:00",
        )
    )

    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=database_path,
        project_json_path=project_json_path,
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )

    audio_path = root_dir / "audio_16k.wav"
    audio_path.write_bytes(b"dummy-audio")
    result = TranscriptionResult(
        source_audio_path=audio_path,
        detected_language="vi",
        duration_ms=5000,
        segments=[
            SegmentDraft(
                segment_index=0,
                start_ms=0,
                end_ms=1200,
                source_text="Xin chao",
                language="vi",
                words=[WordTimestamp(start_ms=0, end_ms=400, text="Xin")],
            ),
            SegmentDraft(
                segment_index=1,
                start_ms=1300,
                end_ms=2400,
                source_text="The gioi",
                language="vi",
            ),
        ],
    )

    persisted = persist_transcription_result(
        workspace,
        result=result,
        options=TranscriptionOptions(model_name="small", language="vi"),
    )

    assert persisted.segment_count == 2
    assert persisted.segments_json_path.exists()
    assert database.count_segments("project-1") == 2
    assert database.count_subtitle_events("project-1") == 2
    subtitle_rows = database.list_subtitle_events("project-1")
    assert subtitle_rows[0]["source_segment_id"] == subtitle_rows[0]["segment_id"]
    assert subtitle_rows[0]["source_text"] == "Xin chao"

    payload = json.loads(persisted.segments_json_path.read_text(encoding="utf-8"))
    assert payload["segment_count"] == 2
    assert payload["detected_language"] == "vi"
