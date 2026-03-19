from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from app.project.models import RelationshipProfileRecord


AllowedAlternatesJson = list[str] | dict[str, list[str]]


def _row_value(row: object, field: str, default: object = None) -> object:
    if isinstance(row, Mapping):
        return row.get(field, default)
    try:
        return row[field]  # type: ignore[index]
    except Exception:
        return getattr(row, field, default)


def clone_allowed_alternates(value: object) -> AllowedAlternatesJson:
    if isinstance(value, Mapping):
        cloned: dict[str, list[str]] = {}
        for key, items in value.items():
            if isinstance(items, Sequence) and not isinstance(items, (str, bytes)):
                cloned[str(key)] = [str(item) for item in items]
        return cloned
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [str(item) for item in value]
    return []


def _load_json_like(value: object, default: object) -> object:
    if value is None:
        return default
    if isinstance(value, (Mapping, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def split_allowed_alternates_by_side(value: object) -> tuple[set[str], set[str]]:
    cloned = clone_allowed_alternates(value)
    if isinstance(cloned, dict):
        shared_terms = set(cloned.get("both_terms", [])) | set(cloned.get("all_terms", [])) | set(cloned.get("terms", []))
        self_terms = set(cloned.get("self_terms", [])) | shared_terms
        address_terms = set(cloned.get("address_terms", [])) | shared_terms
        return self_terms, address_terms
    legacy_terms = set(cloned)
    return legacy_terms, legacy_terms


def relationship_record_from_row(row: object, *, project_id: str) -> RelationshipProfileRecord:
    allowed_alternates = _load_json_like(_row_value(row, "allowed_alternates_json"), [])
    evidence_segment_ids = _load_json_like(_row_value(row, "evidence_segment_ids_json"), [])
    return RelationshipProfileRecord(
        relationship_id=str(_row_value(row, "relationship_id", "")),
        project_id=project_id,
        from_character_id=str(_row_value(row, "from_character_id", "")),
        to_character_id=str(_row_value(row, "to_character_id", "")),
        relation_type=str(_row_value(row, "relation_type", "unknown") or "unknown"),
        power_delta=str(_row_value(row, "power_delta")) if _row_value(row, "power_delta") is not None else None,
        age_delta=str(_row_value(row, "age_delta")) if _row_value(row, "age_delta") is not None else None,
        intimacy_level=str(_row_value(row, "intimacy_level")) if _row_value(row, "intimacy_level") is not None else None,
        default_self_term=str(_row_value(row, "default_self_term")) if _row_value(row, "default_self_term") is not None else None,
        default_address_term=str(_row_value(row, "default_address_term")) if _row_value(row, "default_address_term") is not None else None,
        allowed_alternates_json=clone_allowed_alternates(allowed_alternates),
        scope=str(_row_value(row, "scope", "scene") or "scene"),
        status=str(_row_value(row, "status", "hypothesized") or "hypothesized"),
        evidence_segment_ids_json=[str(item) for item in evidence_segment_ids],
        last_updated_scene_id=(
            str(_row_value(row, "last_updated_scene_id"))
            if _row_value(row, "last_updated_scene_id") is not None
            else None
        ),
        notes=str(_row_value(row, "notes", "") or ""),
        created_at=str(_row_value(row, "created_at", "") or ""),
        updated_at=str(_row_value(row, "updated_at", "") or ""),
    )


def build_locked_relationship_record(
    *,
    existing: RelationshipProfileRecord | None,
    project_id: str,
    relationship_id: str,
    speaker_id: str,
    listener_id: str,
    self_term: str,
    address_term: str,
    now: str,
) -> RelationshipProfileRecord:
    return RelationshipProfileRecord(
        relationship_id=relationship_id,
        project_id=project_id,
        from_character_id=speaker_id,
        to_character_id=listener_id,
        relation_type=existing.relation_type if existing and existing.relation_type else "manual_locked",
        power_delta=existing.power_delta if existing else None,
        age_delta=existing.age_delta if existing else None,
        intimacy_level=existing.intimacy_level if existing else None,
        default_self_term=self_term or None,
        default_address_term=address_term or None,
        allowed_alternates_json=clone_allowed_alternates(existing.allowed_alternates_json if existing else []),
        scope=existing.scope if existing and existing.scope else "global",
        status="locked_by_human",
        evidence_segment_ids_json=list(existing.evidence_segment_ids_json) if existing else [],
        last_updated_scene_id=existing.last_updated_scene_id if existing else None,
        notes=existing.notes if existing else "",
        created_at=existing.created_at if existing and existing.created_at else now,
        updated_at=now,
    )
