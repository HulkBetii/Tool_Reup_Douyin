from __future__ import annotations

from pathlib import Path

from app.project.models import ProjectWorkspace
from app.subtitle.export import export_preview_subtitles, export_subtitles


def test_export_subtitles_writes_srt_and_ass(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    (root_dir / "presets" / "styles").mkdir(parents=True)
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()
    (root_dir / "presets" / "styles" / "default_ass_style.json").write_text(
        '{"ass_style_json":{"FontName":"Arial","FontSize":36,"Outline":2,"Alignment":2}}',
        encoding="utf-8",
    )

    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )
    segments = [
        {
            "segment_id": "seg-1",
            "start_ms": 0,
            "end_ms": 1200,
            "subtitle_text": "Xin chao",
            "translated_text": "Xin chao",
            "source_text": "Hello",
        }
    ]

    srt_path = export_subtitles(workspace, segments=segments, format_name="srt")
    ass_path = export_subtitles(workspace, segments=segments, format_name="ass")

    assert srt_path.exists()
    assert ass_path.exists()
    assert "Xin chao" in srt_path.read_text(encoding="utf-8")
    assert "Xin chao" in ass_path.read_text(encoding="utf-8")


def test_export_ass_uses_ass_line_break_marker(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    (root_dir / "presets" / "styles").mkdir(parents=True)
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()
    (root_dir / "presets" / "styles" / "default_ass_style.json").write_text(
        '{"ass_style_json":{"FontName":"Arial","FontSize":36,"Outline":2,"Alignment":2}}',
        encoding="utf-8",
    )

    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )

    ass_path = export_subtitles(
        workspace,
        segments=[
            {
                "segment_id": "seg-1",
                "start_ms": 0,
                "end_ms": 1200,
                "subtitle_text": "Dong 1\nDong 2",
                "translated_text": "Dong 1\nDong 2",
                "source_text": "Line 1\nLine 2",
            }
        ],
        format_name="ass",
    )

    content = ass_path.read_text(encoding="utf-8")
    assert r"Dong 1\NDong 2" in content
    assert "Dong 1\nDong 2" not in content


def test_export_preview_subtitles_uses_stable_output_path(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    (root_dir / "presets" / "styles").mkdir(parents=True)
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()
    (root_dir / "presets" / "styles" / "default_ass_style.json").write_text(
        '{"ass_style_json":{"FontName":"Arial","FontSize":36,"Outline":2,"Alignment":2}}',
        encoding="utf-8",
    )

    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )
    preview_path = export_preview_subtitles(
        workspace,
        segments=[
            {
                "segment_id": "seg-1",
                "start_ms": 0,
                "end_ms": 1200,
                "subtitle_text": "Ban dau",
                "translated_text": "Ban dau",
                "source_text": "Initial",
            }
        ],
    )
    updated_path = export_preview_subtitles(
        workspace,
        segments=[
            {
                "segment_id": "seg-1",
                "start_ms": 0,
                "end_ms": 1200,
                "subtitle_text": "Da cap nhat",
                "translated_text": "Da cap nhat",
                "source_text": "Updated",
            }
        ],
    )

    assert preview_path == workspace.cache_dir / "preview" / "live_preview.ass"
    assert updated_path == preview_path
    assert "Da cap nhat" in preview_path.read_text(encoding="utf-8")


def test_export_subtitles_can_disable_source_fallback(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    cache_dir = root_dir / "cache"
    logs_dir = root_dir / "logs"
    exports_dir = root_dir / "exports"
    (root_dir / "presets" / "styles").mkdir(parents=True)
    cache_dir.mkdir(parents=True)
    logs_dir.mkdir()
    exports_dir.mkdir()

    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=logs_dir,
        cache_dir=cache_dir,
        exports_dir=exports_dir,
    )
    segments = [
        {
            "segment_id": "seg-1",
            "start_ms": 0,
            "end_ms": 1200,
            "subtitle_text": "",
            "translated_text": "",
            "source_text": "Ni hao",
        }
    ]

    srt_path = export_subtitles(
        workspace,
        segments=segments,
        format_name="srt",
        allow_source_fallback=False,
    )

    content = srt_path.read_text(encoding="utf-8")
    assert "Ni hao" not in content
