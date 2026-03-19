from __future__ import annotations

import json
from pathlib import Path

from app.project.database import ProjectDatabase
from app.project.models import (
    ProjectRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
    SegmentRecord,
)
from app.translate.contextual_pipeline import recompute_semantic_qc


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _load_golden_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "golden" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_recompute_semantic_qc_blocks_unsafe_tts_pronoun_injection(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-tts-pronoun-injection-ambiguous-listener.json")
    row = fixture["segments"][0]

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
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id="project-1",
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A thuc B di nhanh len",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row["speaker_json"]),
                listeners_json=list(row["listeners_json"]),
                honorific_policy_json=dict(row["honorific_policy_json"]),
                resolved_ellipsis_json=dict(row["resolved_ellipsis_json"]),
                risk_flags_json=list(row["risk_flags_json"]),
                confidence_json=dict(row["confidence_json"]),
                semantic_translation="Di thoi, dung de su phu cho lau.",
                approved_subtitle_text=str(row["approved_subtitle_text"]),
                approved_tts_text=str(row["approved_tts_text"]),
                review_status="approved",
                semantic_qc_passed=True,
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    summary = recompute_semantic_qc(database, project_id="project-1", target_language="vi")
    analysis_row = database.get_segment_analysis("project-1", str(row["segment_id"]))

    assert summary["error_count"] >= 1
    assert analysis_row is not None
    assert analysis_row["needs_human_review"] == 1
    assert analysis_row["semantic_qc_passed"] == 0
    assert analysis_row["review_status"] == "needs_review"


def test_recompute_semantic_qc_blocks_mismatch_against_locked_relation_memory(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-locked-relation-directionality-mismatch.json")
    row = fixture["segments"][0]

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
                default_self_term="anh",
                default_address_term="em",
                allowed_alternates_json=[],
                scope="global",
                status="locked_by_human",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ]
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id="project-1",
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A thuc B di ngay",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row["speaker_json"]),
                listeners_json=list(row["listeners_json"]),
                honorific_policy_json=dict(row["honorific_policy_json"]),
                resolved_ellipsis_json=dict(row["resolved_ellipsis_json"]),
                risk_flags_json=list(row["risk_flags_json"]),
                confidence_json=dict(row["confidence_json"]),
                semantic_translation="Toi noi roi, ban mau di di.",
                approved_subtitle_text=str(row["approved_subtitle_text"]),
                approved_tts_text=str(row["approved_tts_text"]),
                review_status="approved",
                semantic_qc_passed=True,
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    summary = recompute_semantic_qc(database, project_id="project-1", target_language="vi")
    analysis_row = database.get_segment_analysis("project-1", str(row["segment_id"]))

    assert summary["error_count"] >= 1
    assert analysis_row is not None
    assert analysis_row["needs_human_review"] == 1
    assert analysis_row["semantic_qc_passed"] == 0
    assert analysis_row["review_status"] == "needs_review"


def test_recompute_semantic_qc_keeps_allowed_alternates_safe_under_locked_relation(tmp_path: Path) -> None:
    fixture = _load_golden_fixture("zh-vi-locked-relation-allowed-alternates-safe.json")
    row = fixture["segments"][0]

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
                default_self_term="anh",
                default_address_term="em",
                allowed_alternates_json=["tao", "mày"],
                scope="global",
                status="locked_by_human",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ]
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id="project-1",
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A cay cu va noi chuyen gap gap",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row["speaker_json"]),
                listeners_json=list(row["listeners_json"]),
                honorific_policy_json=dict(row["honorific_policy_json"]),
                resolved_ellipsis_json=dict(row["resolved_ellipsis_json"]),
                risk_flags_json=list(row["risk_flags_json"]),
                confidence_json=dict(row["confidence_json"]),
                semantic_translation="Tao noi roi, may mau theo kip di.",
                approved_subtitle_text=str(row["approved_subtitle_text"]),
                approved_tts_text=str(row["approved_tts_text"]),
                review_status="approved",
                semantic_qc_passed=True,
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    summary = recompute_semantic_qc(database, project_id="project-1", target_language="vi")
    analysis_row = database.get_segment_analysis("project-1", str(row["segment_id"]))

    assert summary["error_count"] == 0
    assert analysis_row is not None
    assert analysis_row["semantic_qc_passed"] == 1


def test_recompute_semantic_qc_keeps_self_only_alternate_safe(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-side-specific-alternates-self-safe.json")
    row = fixture["segments"][0]

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
                default_self_term="anh",
                default_address_term="em",
                allowed_alternates_json={"self_terms": ["tao"]},
                scope="global",
                status="locked_by_human",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ]
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id="project-1",
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="A gay gat xung tao voi B",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row["speaker_json"]),
                listeners_json=list(row["listeners_json"]),
                honorific_policy_json=dict(row["honorific_policy_json"]),
                resolved_ellipsis_json=dict(row["resolved_ellipsis_json"]),
                risk_flags_json=list(row["risk_flags_json"]),
                confidence_json=dict(row["confidence_json"]),
                semantic_translation="Tao canh bao roi, em mau di di.",
                approved_subtitle_text=str(row["approved_subtitle_text"]),
                approved_tts_text=str(row["approved_tts_text"]),
                review_status="approved",
                semantic_qc_passed=True,
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    summary = recompute_semantic_qc(database, project_id="project-1", target_language="vi")
    analysis_row = database.get_segment_analysis("project-1", str(row["segment_id"]))

    assert summary["error_count"] == 0
    assert analysis_row is not None
    assert analysis_row["semantic_qc_passed"] == 1


def test_recompute_semantic_qc_does_not_apply_address_only_alternate_to_self(tmp_path: Path) -> None:
    fixture = _load_golden_fixture("zh-vi-side-specific-alternates-address-does-not-relax-self.json")
    row = fixture["segments"][0]

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
                default_self_term="anh",
                default_address_term="em",
                allowed_alternates_json={"address_terms": ["mày"]},
                scope="global",
                status="locked_by_human",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ]
    )
    database.replace_segments(
        "project-1",
        [
            SegmentRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    database.replace_contextual_translation_state(
        "project-1",
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id="project-1",
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="Address alternate co, self alternate khong co",
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id="project-1",
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row["speaker_json"]),
                listeners_json=list(row["listeners_json"]),
                honorific_policy_json=dict(row["honorific_policy_json"]),
                resolved_ellipsis_json=dict(row["resolved_ellipsis_json"]),
                risk_flags_json=list(row["risk_flags_json"]),
                confidence_json=dict(row["confidence_json"]),
                semantic_translation="May noi roi, em mau theo kip di.",
                approved_subtitle_text=str(row["approved_subtitle_text"]),
                approved_tts_text=str(row["approved_tts_text"]),
                review_status="approved",
                semantic_qc_passed=True,
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            )
        ],
    )

    summary = recompute_semantic_qc(database, project_id="project-1", target_language="vi")
    analysis_row = database.get_segment_analysis("project-1", str(row["segment_id"]))

    assert summary["error_count"] >= 1
    assert analysis_row is not None
    assert analysis_row["semantic_qc_passed"] == 0
