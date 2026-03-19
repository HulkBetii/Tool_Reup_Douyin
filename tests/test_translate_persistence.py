from __future__ import annotations

from pathlib import Path

from app.asr.models import SegmentDraft, TranscriptionOptions, TranscriptionResult
from app.asr.persistence import persist_transcription_result
from app.project.database import ProjectDatabase
from app.project.models import ProjectRecord, ProjectWorkspace
from app.translate.models import TranslationPromptTemplate
from app.translate.persistence import (
    build_translation_stage_hash,
    load_cached_translations,
    persist_translations,
)


def test_persist_translations_updates_segments_and_cache(tmp_path: Path) -> None:
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

    audio_path = root_dir / "audio.wav"
    audio_path.write_bytes(b"audio")
    persist_transcription_result(
        workspace,
        result=TranscriptionResult(
            source_audio_path=audio_path,
            detected_language="en",
            duration_ms=2000,
            segments=[
                SegmentDraft(
                    segment_index=0,
                    start_ms=0,
                    end_ms=1000,
                    source_text="Hello world",
                    language="en",
                )
            ],
        ),
        options=TranscriptionOptions(model_name="small", language="en"),
    )
    segments = database.list_segments("project-1")
    template = TranslationPromptTemplate(
        template_id="default",
        name="Default",
        system_prompt="Translate",
        user_prompt_template="{source}",
    )
    stage_hash = build_translation_stage_hash(
        segments=segments,
        template=template,
        model="gpt-4.1-mini",
        source_language="en",
        target_language="vi",
    )

    cache_path = persist_translations(
        workspace,
        translated_items=[
            {
                "segment_id": segments[0]["segment_id"],
                "translated_text": "Xin chao the gioi",
                "translated_text_norm": "Xin chao the gioi",
                "subtitle_text": "Xin chao the gioi",
                "tts_text": "Xin chao the gioi",
                "target_lang": "vi",
                "status": "translated",
            }
        ],
        stage_hash=stage_hash,
        template=template,
        model="gpt-4.1-mini",
        source_language="en",
        target_language="vi",
    )

    assert cache_path.exists()
    cached = load_cached_translations(workspace, stage_hash)
    assert cached is not None
    assert cached[0]["translated_text"] == "Xin chao the gioi"

    updated_rows = database.list_segments("project-1")
    assert updated_rows[0]["status"] == "translated"
    assert updated_rows[0]["translated_text"] == "Xin chao the gioi"
    subtitle_rows = database.list_subtitle_events("project-1")
    assert subtitle_rows[0]["translated_text"] == "Xin chao the gioi"
    assert subtitle_rows[0]["subtitle_text"] == "Xin chao the gioi"
