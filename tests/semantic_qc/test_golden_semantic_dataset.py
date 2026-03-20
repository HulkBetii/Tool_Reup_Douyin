from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.translate.semantic_qc import analyze_segment_analyses


def _dataset_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / "golden"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_manifest() -> dict[str, object]:
    manifest_path = _dataset_root() / "semantic_dataset_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_fixture(relative_path: str) -> dict[str, object]:
    fixture_path = _repo_root() / Path(relative_path.replace("/", "\\"))
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _relationship_defaults_from_fixture(fixture: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    relationship_defaults = fixture.get("relationship_defaults", {}) or {}
    mapped: dict[tuple[str, str], dict[str, object]] = {}
    for pair_key, payload in dict(relationship_defaults).items():
        speaker_id, listener_id = str(pair_key).split("->", 1)
        mapped[(speaker_id, listener_id)] = dict(payload)
    return mapped


@pytest.mark.parametrize("case", _load_manifest()["cases"], ids=lambda case: str(case["fixture_id"]))
def test_golden_semantic_dataset_cases(case: dict[str, object]) -> None:
    fixture = _load_fixture(str(case["path"]))
    expected = dict(case["expected"])
    assert str(case["expected_outcome"]) in {"safe", "blocked"}
    assert str(case["class"]).strip()
    assert str(case["source_run"]).strip()

    report = analyze_segment_analyses(
        fixture["segments"],
        relationship_defaults=_relationship_defaults_from_fixture(fixture),
    )

    codes = {issue.code for issue in report.issues}
    assert report.error_count == int(expected["error_count"])
    assert report.warning_count == int(expected["warning_count"])
    if case["expected_outcome"] == "safe":
        assert report.error_count == 0
    else:
        assert report.error_count >= 1
    for code in expected.get("required_codes", []):
        assert code in codes
    for code in expected.get("forbidden_codes", []):
        assert code not in codes


def test_golden_semantic_dataset_manifest_points_to_existing_files() -> None:
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
        assert str(case["expected_outcome"]) in {"safe", "blocked"}
