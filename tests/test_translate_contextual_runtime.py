from __future__ import annotations

import logging
from types import SimpleNamespace

from app.translate.contextual_runtime import _normalize_review_reason_code, _run_stage_batch_with_retry


class _DummyToken:
    def raise_if_canceled(self) -> None:
        return None


class _DummyContext:
    def __init__(self) -> None:
        self.cancellation_token = _DummyToken()
        self.logger = logging.getLogger("test.contextual_runtime")


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


def test_normalize_review_reason_code_collapses_real_world_variants() -> None:
    assert _normalize_review_reason_code("ambiguous_term_scattered_items") == "ambiguous_term"
    assert _normalize_review_reason_code("Ambiguous term '散'") == "ambiguous_term"
    assert _normalize_review_reason_code("uncertain_speaker_identity") == "uncertain_speaker"
    assert _normalize_review_reason_code("ambiguous_object_reference") == "ambiguous_object_reference"
