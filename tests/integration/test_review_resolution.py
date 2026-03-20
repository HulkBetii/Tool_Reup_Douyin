from __future__ import annotations

from pathlib import Path

from app.project.database import ProjectDatabase
from app.project.models import ProjectRecord, SceneMemoryRecord, SegmentAnalysisRecord, SegmentRecord
from app.translate.review_resolution import apply_review_resolution, resolve_review_target_segment_ids


def _seed_review_resolution_project(tmp_path: Path) -> tuple[ProjectDatabase, str]:
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    project_id = "project-1"
    now = "2026-03-20T00:00:00+00:00"
    database.insert_project(
        ProjectRecord(
            project_id=project_id,
            name="Demo",
            root_dir=str(tmp_path),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            created_at=now,
            updated_at=now,
        )
    )
    database.replace_segments(
        project_id,
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id=project_id,
                segment_index=0,
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text="走吧。",
                source_text_norm="走吧。",
            ),
            SegmentRecord(
                segment_id="seg-2",
                project_id=project_id,
                segment_index=1,
                start_ms=1200,
                end_ms=2400,
                source_lang="zh",
                source_text="快一点。",
                source_text_norm="快一点。",
            ),
            SegmentRecord(
                segment_id="seg-3",
                project_id=project_id,
                segment_index=2,
                start_ms=2400,
                end_ms=3600,
                source_lang="zh",
                source_text="别让师父等急了。",
                source_text_norm="别让师父等急了。",
            ),
            SegmentRecord(
                segment_id="seg-4",
                project_id=project_id,
                segment_index=3,
                start_ms=3600,
                end_ms=4800,
                source_lang="zh",
                source_text="我们走吧。",
                source_text_norm="我们走吧。",
            ),
        ],
    )
    database.replace_contextual_translation_state(
        project_id,
        scenes=[
            SceneMemoryRecord(
                scene_id="scene-1",
                project_id=project_id,
                scene_index=0,
                start_segment_index=0,
                end_segment_index=1,
                start_ms=0,
                end_ms=2400,
                short_scene_summary="A thúc B đi nhanh lên",
                created_at=now,
                updated_at=now,
            ),
            SceneMemoryRecord(
                scene_id="scene-2",
                project_id=project_id,
                scene_index=1,
                start_segment_index=2,
                end_segment_index=3,
                start_ms=2400,
                end_ms=4800,
                short_scene_summary="A nhắc B về sư phụ",
                created_at=now,
                updated_at=now,
            ),
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id="seg-1",
                project_id=project_id,
                scene_id="scene-1",
                segment_index=0,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={
                    "policy_id": "rel:char_a->char_b",
                    "self_term": "anh",
                    "address_term": "em",
                    "locked": False,
                    "confidence": 1.0,
                },
                resolved_ellipsis_json={"omitted_subject": "speaker", "omitted_object": "listener", "confidence": 1.0},
                risk_flags_json=[],
                confidence_json={"overall": 0.95, "speaker": 1.0, "listener": 1.0, "relation": 1.0},
                semantic_translation="Đi thôi.",
                approved_subtitle_text="Đi thôi.",
                approved_tts_text="Đi thôi em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A đang nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-2",
                project_id=project_id,
                scene_id="scene-1",
                segment_index=1,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={
                    "policy_id": "rel:char_a->char_b",
                    "self_term": "anh",
                    "address_term": "em",
                    "locked": False,
                    "confidence": 1.0,
                },
                resolved_ellipsis_json={"omitted_subject": "speaker", "omitted_object": "listener", "confidence": 1.0},
                risk_flags_json=[],
                confidence_json={"overall": 0.95, "speaker": 1.0, "listener": 1.0, "relation": 1.0},
                semantic_translation="Nhanh lên.",
                approved_subtitle_text="Nhanh lên.",
                approved_tts_text="Nhanh lên em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A đang nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-3",
                project_id=project_id,
                scene_id="scene-2",
                segment_index=2,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                listeners_json=[{"character_id": "char_b", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={
                    "policy_id": "rel:char_a->char_b",
                    "self_term": "anh",
                    "address_term": "em",
                    "locked": False,
                    "confidence": 1.0,
                },
                resolved_ellipsis_json={"omitted_subject": "speaker", "omitted_object": "listener", "confidence": 1.0},
                risk_flags_json=[],
                confidence_json={"overall": 0.95, "speaker": 1.0, "listener": 1.0, "relation": 1.0},
                semantic_translation="Đừng để sư phụ chờ lâu.",
                approved_subtitle_text="Đừng để sư phụ chờ lâu.",
                approved_tts_text="Đừng để sư phụ chờ lâu em.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="A đang nói với B hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-4",
                project_id=project_id,
                scene_id="scene-2",
                segment_index=3,
                speaker_json={"character_id": "char_c", "confidence": 1.0},
                listeners_json=[{"character_id": "char_d", "role": "primary", "confidence": 1.0}],
                honorific_policy_json={
                    "policy_id": "rel:char_c->char_d",
                    "self_term": "tôi",
                    "address_term": "bạn",
                    "locked": False,
                    "confidence": 1.0,
                },
                resolved_ellipsis_json={"omitted_subject": "speaker", "omitted_object": "listener", "confidence": 1.0},
                risk_flags_json=[],
                confidence_json={"overall": 0.95, "speaker": 1.0, "listener": 1.0, "relation": 1.0},
                semantic_translation="Chúng ta đi thôi.",
                approved_subtitle_text="Chúng ta đi thôi.",
                approved_tts_text="Chúng ta đi thôi bạn.",
                needs_human_review=True,
                review_status="needs_review",
                review_reason_codes_json=["uncertain_speaker"],
                review_question="C đang nói với D hay cả nhóm?",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
        ],
    )
    return database, project_id


def test_resolve_review_target_segment_ids_matches_scene_and_relation_scope(tmp_path: Path) -> None:
    database, project_id = _seed_review_resolution_project(tmp_path)

    scene_ids = resolve_review_target_segment_ids(
        database,
        project_id=project_id,
        segment_id="seg-1",
        scope="scene",
    )
    relationship_ids = resolve_review_target_segment_ids(
        database,
        project_id=project_id,
        segment_id="seg-1",
        scope="project-relationship",
    )

    assert scene_ids == ["seg-1", "seg-2"]
    assert relationship_ids == ["seg-1", "seg-2", "seg-3"]


def test_apply_review_resolution_updates_only_explicit_selected_rows(tmp_path: Path) -> None:
    database, project_id = _seed_review_resolution_project(tmp_path)

    updated_count = apply_review_resolution(
        database,
        project_id=project_id,
        segment_id="seg-1",
        speaker_id="char_a",
        listener_id="char_b",
        self_term="anh",
        address_term="em",
        subtitle_text="Đi thôi em.",
        tts_text="Đi thôi em.",
        scope="line",
        explicit_segment_ids=["seg-1", "seg-3"],
        target_language="vi",
        updated_at="2026-03-20T01:00:00+00:00",
    )

    seg1 = database.get_segment_analysis(project_id, "seg-1")
    seg2 = database.get_segment_analysis(project_id, "seg-2")
    seg3 = database.get_segment_analysis(project_id, "seg-3")
    relationship_rows = database.list_relationship_profiles(project_id)

    assert updated_count == 2
    assert seg1 is not None and seg2 is not None and seg3 is not None
    assert seg1["approved_subtitle_text"] == "Đi thôi em."
    assert seg1["approved_tts_text"] == "Đi thôi em."
    assert seg1["review_status"] == "approved"
    assert seg1["review_scope"] == "selected"
    assert seg1["needs_human_review"] == 0
    assert seg3["approved_subtitle_text"] == "Đừng để sư phụ chờ lâu."
    assert seg3["approved_tts_text"] == "Đừng để sư phụ chờ lâu em."
    assert seg3["review_status"] == "needs_review"
    assert seg3["review_scope"] == "selected"
    assert seg3["needs_human_review"] == 1
    assert seg2["review_status"] == "needs_review"
    assert seg2["needs_human_review"] == 1
    assert relationship_rows == []
