from __future__ import annotations

from app.translate.scene_chunker import chunk_segments_into_scenes


def test_scene_chunker_splits_by_gap_and_duration() -> None:
    rows = [
        {"segment_id": "seg-1", "segment_index": 0, "start_ms": 0, "end_ms": 1000, "source_text": "A"},
        {"segment_id": "seg-2", "segment_index": 1, "start_ms": 1100, "end_ms": 2000, "source_text": "B"},
        {"segment_id": "seg-3", "segment_index": 2, "start_ms": 4000, "end_ms": 4800, "source_text": "C"},
        {"segment_id": "seg-4", "segment_index": 3, "start_ms": 5000, "end_ms": 76000, "source_text": "D"},
        {"segment_id": "seg-5", "segment_index": 4, "start_ms": 76050, "end_ms": 81000, "source_text": "E"},
    ]

    scenes = chunk_segments_into_scenes(rows, gap_ms=1500, max_segments=24, max_duration_ms=75000)

    assert len(scenes) == 3
    assert scenes[0].segment_ids == ["seg-1", "seg-2"]
    assert scenes[1].segment_ids == ["seg-3", "seg-4"]
    assert scenes[2].segment_ids == ["seg-5"]
