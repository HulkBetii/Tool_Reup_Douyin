from __future__ import annotations

import json
from pathlib import Path

from app.translate.relationship_memory import build_locked_relationship_record, relationship_record_from_row


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_build_locked_relationship_record_preserves_allowed_alternates_and_metadata() -> None:
    fixture = _load_regression_fixture("ui-review-lock-preserves-allowed-alternates.json")
    existing = relationship_record_from_row(
        fixture["existing_relationship"],
        project_id=str(fixture["lock_input"]["project_id"]),
    )

    record = build_locked_relationship_record(
        existing=existing,
        project_id=str(fixture["lock_input"]["project_id"]),
        relationship_id=str(fixture["lock_input"]["relationship_id"]),
        speaker_id=str(fixture["lock_input"]["speaker_id"]),
        listener_id=str(fixture["lock_input"]["listener_id"]),
        self_term=str(fixture["lock_input"]["self_term"]),
        address_term=str(fixture["lock_input"]["address_term"]),
        now=str(fixture["lock_input"]["now"]),
    )

    assert record.default_self_term == "anh"
    assert record.default_address_term == "em"
    assert record.allowed_alternates_json == {"self_terms": ["tao"], "address_terms": ["mày"]}
    assert record.status == "locked_by_human"
    assert record.relation_type == "siblings"
    assert record.scope == "global"
    assert record.evidence_segment_ids_json == ["seg-10", "seg-12"]
    assert record.notes == "Reviewer da cho phep doi sang tao/may o canh cai nhau."
    assert record.created_at == "2026-03-20T00:00:00+00:00"
    assert record.updated_at == "2026-03-20T00:10:00+00:00"
