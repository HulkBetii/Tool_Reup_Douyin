from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

from pydantic import ValidationError

from app.translate.models import DialogueAdaptationBatchOutput
from app.translate.contextual_runtime import _normalize_review_reason_code, _run_stage_batch_with_retry


class _DummyToken:
    def raise_if_canceled(self) -> None:
        return None


class _DummyContext:
    def __init__(self) -> None:
        self.cancellation_token = _DummyToken()
        self.logger = logging.getLogger("test.contextual_runtime")


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_run_stage_batch_with_retry_splits_when_ids_are_missing() -> None:
    context = _DummyContext()
    rows = [
        {"segment_id": "seg-1"},
        {"segment_id": "seg-2"},
    ]
    calls: list[list[str]] = []

    def stage_runner(current_rows, _batch_index, _batch_count):
        current_ids = [str(row["segment_id"]) for row in current_rows]
        calls.append(current_ids)
        if len(current_rows) > 1:
            return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])
        return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])

    items = _run_stage_batch_with_retry(
        context=context,
        stage_label="Semantic pass",
        scene_id="scene-1",
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
    )

    assert sorted(items) == ["seg-1", "seg-2"]
    assert calls == [["seg-1", "seg-2"], ["seg-1"], ["seg-2"]]


def test_run_stage_batch_with_retry_splits_when_dialogue_adaptation_json_is_truncated() -> None:
    fixture = _load_regression_fixture("zh-vi-dialogue-adaptation-invalid-json-batch-retry.json")
    context = _DummyContext()
    rows = list(fixture["batch_rows"])
    calls: list[list[str]] = []

    def stage_runner(current_rows, _batch_index, _batch_count):
        current_ids = [str(row["segment_id"]) for row in current_rows]
        calls.append(current_ids)
        if len(current_rows) > 1:
            return DialogueAdaptationBatchOutput.model_validate_json(str(fixture["invalid_json"]))
        return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])

    items = _run_stage_batch_with_retry(
        context=context,
        stage_label=str(fixture["stage_label"]),
        scene_id=str(fixture["scene_id"]),
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
    )

    assert sorted(items) == ["seg-1", "seg-2"]
    assert calls == [["seg-1", "seg-2"], ["seg-1"], ["seg-2"]]


def test_run_stage_batch_with_retry_preserves_single_row_validation_error() -> None:
    fixture = _load_regression_fixture("zh-vi-dialogue-adaptation-invalid-json-batch-retry.json")
    context = _DummyContext()
    rows = [dict(fixture["batch_rows"][0])]

    def stage_runner(_current_rows, _batch_index, _batch_count):
        return DialogueAdaptationBatchOutput.model_validate_json(str(fixture["invalid_json"]))

    try:
        _run_stage_batch_with_retry(
            context=context,
            stage_label=str(fixture["stage_label"]),
            scene_id=str(fixture["scene_id"]),
            batch_rows=rows,
            batch_index=1,
            batch_count=1,
            stage_runner=stage_runner,
        )
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for a single-row retryable stage failure")


def test_normalize_review_reason_code_collapses_real_world_variants() -> None:
    assert _normalize_review_reason_code("ambiguous_term_scattered_items") == "ambiguous_term"
    assert _normalize_review_reason_code("Ambiguous term '散'") == "ambiguous_term"
    assert _normalize_review_reason_code("uncertain_speaker_identity") == "uncertain_speaker"
    assert _normalize_review_reason_code("ambiguous_object_reference") == "ambiguous_object_reference"
