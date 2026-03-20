from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.project.database import ProjectDatabase
from app.project.models import (
    ProjectRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
    SegmentRecord,
)
from app.translate.contextual_pipeline import recompute_semantic_qc


def _dataset_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / "golden"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_manifest() -> dict[str, object]:
    manifest_path = _dataset_root() / "review_gate_dataset_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_fixture(relative_path: str) -> dict[str, object]:
    fixture_path = _repo_root() / Path(relative_path.replace("/", "\\"))
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _seed_review_gate_project(
    tmp_path: Path,
    *,
    fixture: dict[str, object],
    expected_outcome: str,
) -> tuple[ProjectDatabase, str, str]:
    row = dict(fixture["segments"][0])
    now = "2026-03-20T00:00:00+00:00"
    project_id = "project-1"
    database_path = tmp_path / "project.db"
    database = ProjectDatabase(database_path)
    database.initialize()
    database.insert_project(
        ProjectRecord(
            project_id=project_id,
            name="Review Gate Dataset",
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
                segment_id=str(row["segment_id"]),
                project_id=project_id,
                segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                source_lang="zh",
                source_text=str(row["source_text"]),
                source_text_norm=str(row["source_text"]),
            )
        ],
    )
    relationship_profiles: list[RelationshipProfileRecord] = []
    for pair_key, payload in dict(fixture.get("relationship_defaults", {}) or {}).items():
        speaker_id, listener_id = str(pair_key).split("->", 1)
        relationship_profiles.append(
            RelationshipProfileRecord(
                relationship_id=f"rel:{speaker_id}->{listener_id}",
                project_id=project_id,
                from_character_id=speaker_id,
                to_character_id=listener_id,
                relation_type=str(payload.get("relation_type", "") or ""),
                default_self_term=str(payload.get("default_self_term", "") or ""),
                default_address_term=str(payload.get("default_address_term", "") or ""),
                allowed_alternates_json=payload.get("allowed_alternates_json", []),
                scope=str(payload.get("scope", "global") or "global"),
                status=str(payload.get("status", "hypothesized") or "hypothesized"),
                created_at=now,
                updated_at=now,
            )
        )
    if relationship_profiles:
        database.upsert_relationship_profiles(relationship_profiles)

    initial_needs_review = expected_outcome == "needs_review"
    database.replace_contextual_translation_state(
        project_id,
        scenes=[
            SceneMemoryRecord(
                scene_id=str(row["scene_id"]),
                project_id=project_id,
                scene_index=0,
                start_segment_index=int(row["segment_index"]),
                end_segment_index=int(row["segment_index"]),
                start_ms=0,
                end_ms=1200,
                short_scene_summary="fixture scene",
                created_at=now,
                updated_at=now,
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id=str(row["segment_id"]),
                project_id=project_id,
                scene_id=str(row["scene_id"]),
                segment_index=int(row["segment_index"]),
                speaker_json=dict(row.get("speaker_json", {})),
                listeners_json=list(row.get("listeners_json", [])),
                register_json=dict(row.get("register_json", {})),
                turn_function=str(row.get("turn_function", "") or "") or None,
                honorific_policy_json=dict(row.get("honorific_policy_json", {})),
                resolved_ellipsis_json=dict(row.get("resolved_ellipsis_json", {})),
                risk_flags_json=list(row.get("risk_flags_json", [])),
                confidence_json=dict(row.get("confidence_json", {})),
                semantic_translation=str(row.get("approved_subtitle_text", "") or ""),
                approved_subtitle_text=str(row.get("approved_subtitle_text", "") or ""),
                approved_tts_text=str(row.get("approved_tts_text", "") or ""),
                needs_human_review=initial_needs_review,
                review_status="needs_review" if initial_needs_review else "approved",
                review_reason_codes_json=list(row.get("review_reason_codes_json", [])),
                review_question=str(row.get("review_question", "") or ""),
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            )
        ],
    )
    return database, project_id, str(row["segment_id"])


@pytest.mark.parametrize("case", _load_manifest()["cases"], ids=lambda case: str(case["fixture_id"]))
def test_review_gate_dataset_cases(case: dict[str, object], tmp_path: Path) -> None:
    fixture = _load_fixture(str(case["path"]))
    expected_outcome = str(case["expected_outcome"])
    assert expected_outcome in {"needs_review", "blocked"}
    assert str(case["class"]).strip()
    assert str(case["source_run"]).strip()

    database, project_id, segment_id = _seed_review_gate_project(
        tmp_path,
        fixture=fixture,
        expected_outcome=expected_outcome,
    )

    summary = recompute_semantic_qc(database, project_id=project_id, target_language="vi")
    analysis_row = database.get_segment_analysis(project_id, segment_id)

    assert analysis_row is not None
    assert database.count_pending_segment_reviews(project_id) == 1
    assert analysis_row["needs_human_review"] == 1
    assert analysis_row["review_status"] == "needs_review"
    if expected_outcome == "blocked":
        assert summary["error_count"] >= 1
        assert analysis_row["semantic_qc_passed"] == 0
    else:
        assert analysis_row["semantic_qc_passed"] == 1
        reason_codes = json.loads(analysis_row["review_reason_codes_json"] or "[]")
        assert str(case["class"]) in reason_codes


def test_review_gate_dataset_manifest_points_to_existing_files() -> None:
    manifest = _load_manifest()
    fixture_ids: set[str] = set()

    for case in manifest["cases"]:
        fixture_path = _repo_root() / Path(str(case["path"]).replace("/", "\\"))
        assert fixture_path.exists()
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        fixture_id = str(fixture["fixture_id"])
        assert fixture_id == str(case["fixture_id"])
        assert fixture_id not in fixture_ids
        fixture_ids.add(fixture_id)
        assert str(case["class"]).strip()
        assert str(case["source_run"]).strip()
        assert str(case["expected_outcome"]) in {"needs_review", "blocked"}
