from __future__ import annotations

from pathlib import Path

from app.exporting.presets import list_export_presets
from app.project.models import ProjectWorkspace
from app.subtitle.hardsub import (
    _progress_percent_from_ffmpeg_line,
    build_hardsub_command,
    build_hardsub_output_path,
    build_video_filter_graph,
    escape_ffmpeg_filter_path,
    load_export_preset,
)


def test_hardsub_helpers_build_windows_safe_command(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    (root_dir / "presets" / "exports").mkdir(parents=True)
    (root_dir / "assets" / "logos").mkdir(parents=True)
    (root_dir / "exports").mkdir()
    (root_dir / "logs").mkdir()
    (root_dir / "cache").mkdir()
    (root_dir / "presets" / "exports" / "default_hardsub.json").write_text(
        """
        {
          "export_preset_id": "youtube-16x9",
          "name": "YouTube 16:9",
          "container": "mp4",
          "video_codec": "h264",
          "audio_codec": "aac",
          "target_width": 1920,
          "target_height": 1080,
          "resolution_mode": "pad",
          "burn_subtitles": true,
          "watermark_enabled": true,
          "watermark_position": "top-right",
          "watermark_opacity": 0.8,
          "watermark_scale": 0.16,
          "watermark_margin": 24
        }
        """,
        encoding="utf-8",
    )
    source_video = root_dir / "input.mp4"
    subtitle_path = root_dir / "track[vi],demo.ass"
    replacement_audio = root_dir / "mixed_audio.wav"
    watermark_path = root_dir / "assets" / "logos" / "brand.png"
    source_video.write_bytes(b"video")
    subtitle_path.write_text("[Script Info]\nTitle: Demo", encoding="utf-8")
    replacement_audio.write_bytes(b"audio")
    watermark_path.write_bytes(b"png")

    preset = load_export_preset(root_dir)
    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo Project",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=root_dir / "logs",
        cache_dir=root_dir / "cache",
        exports_dir=root_dir / "exports",
    )
    output_path = build_hardsub_output_path(workspace, preset)
    command = build_hardsub_command(
        ffmpeg_executable="ffmpeg.exe",
        source_video_path=source_video,
        subtitle_path=subtitle_path,
        output_path=output_path,
        export_preset=preset,
        replacement_audio_path=replacement_audio,
        watermark_path=watermark_path,
    )
    filter_graph, output_label = build_video_filter_graph(
        subtitle_path=subtitle_path,
        export_preset=preset,
        watermark_input_index=2,
    )
    escaped_path = escape_ffmpeg_filter_path(subtitle_path)

    assert output_path.name == "Demo_Project_youtube-16x9_hardsub.mp4"
    assert command[:8] == ["ffmpeg.exe", "-y", "-i", str(source_video), "-i", str(replacement_audio), "-i", str(watermark_path)]
    assert "ass='" + escaped_path + "'" in filter_graph
    assert "overlay=" in filter_graph
    assert "scale=w=1920:h=1080" in filter_graph
    assert output_label == "[vout]"
    assert str(output_path) == command[-1]
    assert "1:a:0" in command
    assert "-filter_complex" in command
    assert r"\[" in escaped_path
    assert r"\]" in escaped_path
    assert r"\," in escaped_path


def test_list_export_presets_loads_multiple_files(tmp_path: Path) -> None:
    presets_dir = tmp_path / "presets" / "exports"
    presets_dir.mkdir(parents=True)
    (presets_dir / "default_hardsub.json").write_text(
        '{"export_preset_id":"youtube-16x9","name":"YouTube 16:9","container":"mp4","video_codec":"h264","audio_codec":"aac","target_width":1920,"target_height":1080}',
        encoding="utf-8",
    )
    (presets_dir / "shorts_9x16.json").write_text(
        '{"export_preset_id":"shorts-9x16","name":"Shorts 9:16","container":"mp4","video_codec":"h264","audio_codec":"aac","target_width":1080,"target_height":1920,"resolution_mode":"pad"}',
        encoding="utf-8",
    )

    presets = list_export_presets(tmp_path)

    assert [preset.export_preset_id for preset in presets] == ["youtube-16x9", "shorts-9x16"]


def test_hardsub_command_muxes_soft_subtitle_when_burn_disabled(tmp_path: Path) -> None:
    root_dir = tmp_path / "project"
    (root_dir / "presets" / "exports").mkdir(parents=True)
    (root_dir / "exports").mkdir()
    (root_dir / "logs").mkdir()
    (root_dir / "cache").mkdir()
    (root_dir / "presets" / "exports" / "softsub.json").write_text(
        """
        {
          "export_preset_id": "youtube-softsub",
          "name": "YouTube Softsub",
          "container": "mp4",
          "video_codec": "h264",
          "audio_codec": "aac",
          "target_width": 1920,
          "target_height": 1080,
          "resolution_mode": "keep",
          "burn_subtitles": false,
          "watermark_enabled": false
        }
        """,
        encoding="utf-8",
    )
    source_video = root_dir / "input.mp4"
    subtitle_path = root_dir / "track.vi.srt"
    source_video.write_bytes(b"video")
    subtitle_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nXin chao\n", encoding="utf-8")

    preset = load_export_preset(root_dir, "youtube-softsub")
    workspace = ProjectWorkspace(
        project_id="project-1",
        name="Demo Project",
        root_dir=root_dir,
        database_path=root_dir / "project.db",
        project_json_path=root_dir / "project.json",
        logs_dir=root_dir / "logs",
        cache_dir=root_dir / "cache",
        exports_dir=root_dir / "exports",
    )
    output_path = build_hardsub_output_path(workspace, preset)
    command = build_hardsub_command(
        ffmpeg_executable="ffmpeg.exe",
        source_video_path=source_video,
        subtitle_path=subtitle_path,
        output_path=output_path,
        export_preset=preset,
    )
    filter_graph, output_label = build_video_filter_graph(
        subtitle_path=subtitle_path,
        export_preset=preset,
    )

    assert output_path.name == "Demo_Project_youtube-softsub_softsub.mp4"
    assert "ass='" not in filter_graph
    assert output_label == "[vout]"
    assert command[:6] == ["ffmpeg.exe", "-y", "-i", str(source_video), "-i", str(subtitle_path)]
    assert "-map" in command
    assert "1:0" in command
    assert "0:a?" in command
    assert "mov_text" in command


def test_progress_parser_ignores_na_and_parses_numeric_values() -> None:
    assert _progress_percent_from_ffmpeg_line("out_time_ms=N/A", duration_ms=30_000) is None
    assert _progress_percent_from_ffmpeg_line("out_time_ms=15000000", duration_ms=30_000) == 50
