from __future__ import annotations

from app.subtitle.qc import SubtitleQcConfig, analyze_subtitle_rows


def test_analyze_subtitle_rows_reports_overlap_cps_cpl_and_duration() -> None:
    rows = [
        {
            "segment_id": "seg-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 500,
            "source_text": "Hello",
            "translated_text": "Xin chao cac ban toi day",
            "subtitle_text": "Dong 1 rat dai vuot nguong cho phep\nDong 2 rat dai vuot nguong cho phep\nDong 3",
        },
        {
            "segment_id": "seg-2",
            "segment_index": 1,
            "start_ms": 400,
            "end_ms": 9000,
            "source_text": "Hello again",
            "translated_text": "",
            "subtitle_text": "",
        },
    ]

    report = analyze_subtitle_rows(
        rows,
        config=SubtitleQcConfig(max_lines=2, max_cpl=20, max_cps=10.0, min_duration_ms=800, max_duration_ms=7000),
    )
    codes = {(issue.segment_id, issue.code) for issue in report.issues}

    assert report.total_segments == 2
    assert report.error_count == 1
    assert ("seg-1", "short_duration") in codes
    assert ("seg-1", "too_many_lines") in codes
    assert ("seg-1", "high_cpl") in codes
    assert ("seg-1", "high_cps") in codes
    assert ("seg-2", "overlap") in codes
    assert ("seg-2", "long_duration") in codes
    assert ("seg-2", "empty_text") in codes
