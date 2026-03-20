from __future__ import annotations

import json
from pathlib import Path

from app.project.database import ProjectDatabase
from app.project.models import ProjectRecord, SceneMemoryRecord, SegmentAnalysisRecord, SegmentRecord, SpeakerBindingRecord
from app.tts.speaker_binding import build_speaker_binding_plan


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_partial_speaker_binding_config_stays_fail_safe_at_stage_boundary(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-speaker-binding-partial-config-blocks-tts.json")

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
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(item["segment_id"]),
                project_id="project-1",
                segment_index=index,
                start_ms=index * 1000,
                end_ms=(index + 1) * 1000,
                source_lang="zh",
                source_text="test",
                source_text_norm="test",
            )
            for index, item in enumerate(fixture["analysis_rows"])
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
                end_segment_index=len(fixture["analysis_rows"]) - 1,
                start_ms=0,
                end_ms=len(fixture["analysis_rows"]) * 1000,
                short_scene_summary="Speaker binding gate sample",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(item["segment_id"]),
                project_id="project-1",
                scene_id="scene_0000",
                segment_index=index,
                speaker_json=dict(item["speaker_json"]),
                approved_subtitle_text="stub",
                approved_tts_text="stub",
                semantic_qc_passed=True,
                review_status="approved",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
            for index, item in enumerate(fixture["analysis_rows"])
        ],
    )
    database.replace_speaker_bindings(
        "project-1",
        [
            SpeakerBindingRecord(
                binding_id=str(item["binding_id"]),
                project_id="project-1",
                speaker_type=str(item["speaker_type"]),
                speaker_key=str(item["speaker_key"]),
                voice_preset_id=str(item["voice_preset_id"]),
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
            for item in fixture["binding_rows"]
        ],
    )

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=database.list_segment_analyses("project-1"),
        binding_rows=database.list_speaker_bindings("project-1"),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is True
    assert plan.unresolved_speakers == ["char_b"]


def test_unknown_placeholder_speakers_do_not_force_manual_binding(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-speaker-binding-unknown-placeholder-falls-back.json")

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
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(item["segment_id"]),
                project_id="project-1",
                segment_index=index,
                start_ms=index * 1000,
                end_ms=(index + 1) * 1000,
                source_lang="zh",
                source_text="test",
                source_text_norm="test",
            )
            for index, item in enumerate(fixture["analysis_rows"])
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
                end_segment_index=len(fixture["analysis_rows"]) - 1,
                start_ms=0,
                end_ms=len(fixture["analysis_rows"]) * 1000,
                short_scene_summary="Unknown placeholder speaker sample",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(item["segment_id"]),
                project_id="project-1",
                scene_id="scene_0000",
                segment_index=index,
                speaker_json=dict(item["speaker_json"]),
                approved_subtitle_text="stub",
                approved_tts_text="stub",
                semantic_qc_passed=True,
                review_status="approved",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
            for index, item in enumerate(fixture["analysis_rows"])
        ],
    )
    database.replace_speaker_bindings(
        "project-1",
        [
            SpeakerBindingRecord(
                binding_id=str(item["binding_id"]),
                project_id="project-1",
                speaker_type=str(item["speaker_type"]),
                speaker_key=str(item["speaker_key"]),
                voice_preset_id=str(item["voice_preset_id"]),
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
            for item in fixture["binding_rows"]
        ],
    )

    plan = build_speaker_binding_plan(
        subtitle_rows=list(fixture["subtitle_rows"]),
        analysis_rows=database.list_segment_analyses("project-1"),
        binding_rows=database.list_speaker_bindings("project-1"),
        available_preset_ids=set(fixture["available_preset_ids"]),
    )

    assert plan.active_bindings is True
    assert plan.segment_speaker_keys == {}
    assert plan.unresolved_speakers == []
