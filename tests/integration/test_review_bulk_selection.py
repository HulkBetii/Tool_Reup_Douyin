from __future__ import annotations

import os
from pathlib import Path

from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.settings import build_default_settings
from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest, SceneMemoryRecord, SegmentAnalysisRecord, SegmentRecord
from app.ui.main_window import MainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _seed_contextual_review_project(tmp_path: Path):
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Review Bulk Selection",
            root_dir=tmp_path / "project",
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    now = "2026-03-20T00:00:00+00:00"
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                segment_index=0,
                start_ms=0,
                end_ms=1000,
                source_lang="zh",
                source_text="走吧。",
                source_text_norm="走吧。",
            ),
            SegmentRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                segment_index=1,
                start_ms=1000,
                end_ms=2000,
                source_lang="zh",
                source_text="快点。",
                source_text_norm="快点。",
            ),
            SegmentRecord(
                segment_id="seg-3",
                project_id=workspace.project_id,
                segment_index=2,
                start_ms=2000,
                end_ms=3000,
                source_lang="zh",
                source_text="别磨蹭。",
                source_text_norm="别磨蹭。",
            ),
        ],
    )
    database.replace_contextual_translation_state(
        workspace.project_id,
        scenes=[
            SceneMemoryRecord(
                scene_id="scene-1",
                project_id=workspace.project_id,
                scene_index=0,
                start_segment_index=0,
                end_segment_index=1,
                start_ms=0,
                end_ms=2000,
                short_scene_summary="A thúc B đi nhanh",
                created_at=now,
                updated_at=now,
            ),
            SceneMemoryRecord(
                scene_id="scene-2",
                project_id=workspace.project_id,
                scene_index=1,
                start_segment_index=2,
                end_segment_index=2,
                start_ms=2000,
                end_ms=3000,
                short_scene_summary="A tiếp tục thúc B",
                created_at=now,
                updated_at=now,
            ),
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                scene_id="scene-1",
                segment_index=0,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={"policy_id": "rel:char_a->char_b", "self_term": "anh", "address_term": "em"},
                semantic_translation="Đi thôi.",
                approved_subtitle_text="Đi thôi.",
                approved_tts_text="Đi thôi em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                scene_id="scene-1",
                segment_index=1,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={"policy_id": "rel:char_a->char_b", "self_term": "anh", "address_term": "em"},
                semantic_translation="Nhanh lên.",
                approved_subtitle_text="Nhanh lên.",
                approved_tts_text="Nhanh lên em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-3",
                project_id=workspace.project_id,
                scene_id="scene-2",
                segment_index=2,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={"policy_id": "rel:char_a->char_b", "self_term": "anh", "address_term": "em"},
                semantic_translation="Đừng chậm nữa.",
                approved_subtitle_text="Đừng chậm nữa.",
                approved_tts_text="Đừng chậm nữa em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
        ],
    )
    return workspace


def test_review_bulk_selection_keeps_all_relation_rows_selected(tmp_path: Path) -> None:
    app = QApplication.instance() or QApplication([])
    workspace = _seed_contextual_review_project(tmp_path)
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    app.processEvents()

    window._select_review_rows_by_scope("relation")  # type: ignore[attr-defined]
    app.processEvents()

    assert window._selected_review_segment_ids() == ["seg-1", "seg-2", "seg-3"]  # type: ignore[attr-defined]

    window.close()
    app.processEvents()
