from __future__ import annotations

import json

from app.project.database import ProjectDatabase
from app.translate.contextual_pipeline import recompute_semantic_qc
from app.translate.relationship_memory import build_locked_relationship_record, relationship_record_from_row


def _analysis_json_field(row: object, field: str, default: object) -> object:
    try:
        raw_value = row[field]
    except Exception:
        raw_value = getattr(row, field, default)
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _speaker_listener_pair(row: object) -> tuple[str, str]:
    speaker = _analysis_json_field(row, "speaker_json", {})
    listeners = _analysis_json_field(row, "listeners_json", [])
    speaker_id = str(speaker.get("character_id", "unknown")) if isinstance(speaker, dict) else "unknown"
    if isinstance(listeners, list) and listeners:
        first_listener = listeners[0] or {}
        if isinstance(first_listener, dict):
            listener_id = str(first_listener.get("character_id", "unknown"))
        else:
            listener_id = "unknown"
    else:
        listener_id = "unknown"
    return speaker_id, listener_id


def _target_rows_for_scope(
    database: ProjectDatabase,
    *,
    project_id: str,
    analysis_row: object,
    scope: str,
    explicit_segment_ids: list[str] | None = None,
) -> list[object]:
    all_rows = database.list_segment_analyses(project_id)
    if explicit_segment_ids:
        explicit_ids = {str(item).strip() for item in explicit_segment_ids if str(item).strip()}
        return [row for row in all_rows if str(row["segment_id"]) in explicit_ids]

    if scope == "line":
        return [analysis_row]

    selected_pair = _speaker_listener_pair(analysis_row)
    if scope == "scene":
        return [
            row
            for row in all_rows
            if str(row["scene_id"]) == str(analysis_row["scene_id"]) and _speaker_listener_pair(row) == selected_pair
        ]
    if scope == "project-relationship":
        return [row for row in all_rows if _speaker_listener_pair(row) == selected_pair]
    raise ValueError(f"Unsupported review resolution scope: {scope}")


def resolve_review_target_segment_ids(
    database: ProjectDatabase,
    *,
    project_id: str,
    segment_id: str,
    scope: str,
    explicit_segment_ids: list[str] | None = None,
) -> list[str]:
    analysis_row = database.get_segment_analysis(project_id, segment_id)
    if analysis_row is None:
        raise ValueError(f"Unknown review segment: {segment_id}")
    return [
        str(row["segment_id"])
        for row in _target_rows_for_scope(
            database,
            project_id=project_id,
            analysis_row=analysis_row,
            scope=scope,
            explicit_segment_ids=explicit_segment_ids,
        )
    ]


def apply_review_resolution(
    database: ProjectDatabase,
    *,
    project_id: str,
    segment_id: str,
    speaker_id: str,
    listener_id: str,
    self_term: str,
    address_term: str,
    subtitle_text: str,
    tts_text: str,
    scope: str,
    target_language: str,
    updated_at: str,
    explicit_segment_ids: list[str] | None = None,
) -> int:
    analysis_row = database.get_segment_analysis(project_id, segment_id)
    if analysis_row is None:
        raise ValueError(f"Unknown review segment: {segment_id}")

    current_speaker = _analysis_json_field(analysis_row, "speaker_json", {})
    current_listeners = _analysis_json_field(analysis_row, "listeners_json", [])
    current_policy = _analysis_json_field(analysis_row, "honorific_policy_json", {})
    updated_speaker = {**current_speaker, "character_id": speaker_id, "source": "manual", "confidence": 1.0}
    updated_listeners = (
        [
            {
                **(current_listeners[0] if current_listeners else {}),
                "character_id": listener_id,
                "role": "primary",
                "confidence": 1.0,
            }
        ]
        if listener_id
        else []
    )
    relationship_id = f"rel:{speaker_id}->{listener_id}"
    updated_policy = {
        **current_policy,
        "policy_id": relationship_id,
        "self_term": self_term,
        "address_term": address_term,
        "locked": True,
        "confidence": 1.0,
    }

    target_rows = _target_rows_for_scope(
        database,
        project_id=project_id,
        analysis_row=analysis_row,
        scope=scope,
        explicit_segment_ids=explicit_segment_ids,
    )

    if scope == "project-relationship" and not explicit_segment_ids:
        existing_relationship_row = next(
            (
                row
                for row in database.list_relationship_profiles(project_id)
                if str(row["from_character_id"]) == speaker_id and str(row["to_character_id"]) == listener_id
            ),
            None,
        )
        database.upsert_relationship_profiles(
            [
                build_locked_relationship_record(
                    existing=(
                        relationship_record_from_row(existing_relationship_row, project_id=project_id)
                        if existing_relationship_row is not None
                        else None
                    ),
                    project_id=project_id,
                    relationship_id=relationship_id,
                    speaker_id=speaker_id,
                    listener_id=listener_id,
                    self_term=self_term,
                    address_term=address_term,
                    now=updated_at,
                )
            ]
        )

    review_status = "approved" if scope == "line" and not explicit_segment_ids else "locked"
    review_scope = "selected" if explicit_segment_ids else scope

    for row in target_rows:
        row_segment_id = str(row["segment_id"])
        database.update_segment_analysis_review(
            project_id,
            row_segment_id,
            speaker_json=updated_speaker,
            listeners_json=updated_listeners,
            honorific_policy_json=updated_policy,
            approved_subtitle_text=(
                subtitle_text if row_segment_id == segment_id and subtitle_text else str(row["approved_subtitle_text"] or "")
            ),
            approved_tts_text=(
                tts_text if row_segment_id == segment_id and tts_text else str(row["approved_tts_text"] or "")
            ),
            needs_human_review=False,
            review_status=review_status,
            review_scope=review_scope,
            review_reason_codes_json=[],
            review_question="",
            updated_at=updated_at,
        )

    recompute_semantic_qc(database, project_id=project_id, target_language=target_language)
    return len(target_rows)
