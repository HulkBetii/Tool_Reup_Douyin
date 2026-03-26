from __future__ import annotations

from pathlib import Path

from app.project.bootstrap import bootstrap_project
from app.project.models import ProjectInitRequest
from app.project.profiles import set_project_subtitle_subtext_mode
from app.subtitle.export import build_subtitle_stage_hash, export_preview_subtitles, export_subtitles


def _workspace(tmp_path: Path):
    return bootstrap_project(
        ProjectInitRequest(
            name="Subtitle Export Demo",
            root_dir=tmp_path / "subtitle-export-demo",
            source_language="zh",
            target_language="vi",
        )
    )


def _segments() -> list[dict[str, object]]:
    return [
        {
            "segment_id": "seg-001",
            "start_ms": 0,
            "end_ms": 2000,
            "source_text": "人类真的能长生不老吗",
            "translated_text": "Con người thật sự có thể trường sinh bất lão sao",
            "subtitle_text": "Con người thật sự có thể trường sinh bất lão sao?",
        }
    ]


def test_subtitle_export_defaults_to_single_line_when_subtext_is_off(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)

    output_path = export_subtitles(
        workspace,
        segments=_segments(),
        format_name="srt",
        allow_source_fallback=False,
    )
    content = output_path.read_text(encoding="utf-8")

    assert "Con người thật sự có thể trường sinh bất lão sao?" in content
    assert "人类真的能长生不老吗" not in content


def test_subtitle_export_includes_source_text_when_subtext_mode_is_enabled(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    set_project_subtitle_subtext_mode(workspace.root_dir, "source_text", applied_at="2026-03-26T00:00:00+00:00")

    output_path = export_subtitles(
        workspace,
        segments=_segments(),
        format_name="srt",
        allow_source_fallback=False,
    )
    content = output_path.read_text(encoding="utf-8")

    assert "Con người thật sự có thể trường sinh bất lão sao?" in content
    assert "人类真的能长生不老吗" in content


def test_ass_preview_export_adds_dimmed_second_line_for_source_subtext(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    preview_path = export_preview_subtitles(
        workspace,
        segments=_segments(),
        format_name="ass",
        subtitle_subtext_mode="source_text",
    )
    content = preview_path.read_text(encoding="utf-8")

    assert "\\N" in content
    assert "\\fs" in content
    assert "人类真的能长生不老吗" in content


def test_subtitle_stage_hash_changes_when_subtext_mode_changes(tmp_path: Path) -> None:
    del tmp_path
    segments = _segments()

    off_hash = build_subtitle_stage_hash(
        segments,
        format_name="ass",
        allow_source_fallback=False,
        subtitle_subtext_mode="off",
    )
    on_hash = build_subtitle_stage_hash(
        segments,
        format_name="ass",
        allow_source_fallback=False,
        subtitle_subtext_mode="source_text",
    )

    assert off_hash != on_hash
