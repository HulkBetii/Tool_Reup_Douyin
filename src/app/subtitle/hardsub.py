from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

from app.core.hashing import build_stage_hash, fingerprint_path
from app.core.jobs import JobCancelledError, JobContext
from app.exporting.models import ExportPreset
from app.exporting.presets import get_export_preset
from app.project.models import ProjectWorkspace

DEFAULT_EXPORT_PRESET = ExportPreset(
    export_preset_id="youtube-16x9",
    name="YouTube 16:9",
    container="mp4",
    video_codec="h264",
    audio_codec="aac",
    resolution_mode="keep",
    target_aspect="16:9",
    target_width=1920,
    target_height=1080,
    crf=18,
    burn_subtitles=True,
)

VIDEO_CODEC_MAP = {
    "h264": "libx264",
    "h265": "libx265",
    "copy": "copy",
}

AUDIO_CODEC_MAP = {"aac": "aac", "copy": "copy"}
SUBTITLE_CODEC_BY_CONTAINER = {
    "mp4": "mov_text",
    "m4v": "mov_text",
    "mov": "mov_text",
    "mkv": "srt",
    "webm": "webvtt",
}


def load_export_preset(project_root: Path, preset_id: str | None = None) -> ExportPreset:
    return get_export_preset(project_root, preset_id) or DEFAULT_EXPORT_PRESET


def build_hardsub_stage_hash(
    *,
    source_video_path: Path,
    subtitle_path: Path,
    export_preset: ExportPreset,
    replacement_audio_path: Path | None = None,
    watermark_override_path: Path | None = None,
) -> str:
    return build_stage_hash(
        {
            "stage": "hardsub",
            "source_video": fingerprint_path(source_video_path),
            "subtitle_file": fingerprint_path(subtitle_path),
            "replacement_audio": fingerprint_path(replacement_audio_path)
            if replacement_audio_path and replacement_audio_path.exists()
            else None,
            "watermark_override": fingerprint_path(watermark_override_path)
            if watermark_override_path and watermark_override_path.exists()
            else None,
            "preset": export_preset.model_dump(mode="json"),
            "version": 1,
        }
    )


def escape_ffmpeg_filter_path(path: Path) -> str:
    value = str(path.resolve()).replace("\\", "/")
    return (
        value.replace(":", r"\:")
        .replace("'", r"\'")
        .replace("[", r"\[")
        .replace("]", r"\]")
        .replace(",", r"\,")
    )


def _resolution_filter(export_preset: ExportPreset) -> str | None:
    width = export_preset.target_width
    height = export_preset.target_height
    if not width or not height:
        return None

    mode = export_preset.resolution_mode.lower()
    if mode in {"pad", "fit"}:
        return (
            f"scale=w={width}:h={height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
        )
    if mode == "crop":
        return (
            f"scale=w={width}:h={height}:force_original_aspect_ratio=increase,"
            f"crop={width}:{height}"
        )
    if mode == "stretch":
        return f"scale={width}:{height}"
    return None


def _overlay_position_expr(position: str, margin: int) -> tuple[str, str]:
    normalized = position.lower()
    if normalized == "top-left":
        return str(margin), str(margin)
    if normalized == "bottom-left":
        return str(margin), f"main_h-overlay_h-{margin}"
    if normalized == "bottom-right":
        return f"main_w-overlay_w-{margin}", f"main_h-overlay_h-{margin}"
    if normalized == "center":
        return "(main_w-overlay_w)/2", "(main_h-overlay_h)/2"
    return f"main_w-overlay_w-{margin}", str(margin)


def resolve_watermark_path(
    project_root: Path,
    export_preset: ExportPreset,
    watermark_override_path: Path | None = None,
) -> Path | None:
    if watermark_override_path is not None:
        return watermark_override_path.resolve() if watermark_override_path.exists() else None
    configured = export_preset.watermark_path
    if not configured:
        return None
    candidate = Path(configured)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve() if candidate.exists() else None


def build_video_filter_graph(
    *,
    subtitle_path: Path,
    export_preset: ExportPreset,
    watermark_input_index: int | None = None,
) -> tuple[str, str]:
    current_label = "[0:v]"
    filters: list[str] = []
    step_index = 0

    resolution_filter = _resolution_filter(export_preset)
    if resolution_filter:
        next_label = f"[v{step_index}]"
        filters.append(f"{current_label}{resolution_filter}{next_label}")
        current_label = next_label
        step_index += 1

    if export_preset.burn_subtitles:
        next_label = f"[v{step_index}]"
        filters.append(f"{current_label}ass='{escape_ffmpeg_filter_path(subtitle_path)}'{next_label}")
        current_label = next_label
        step_index += 1

    if export_preset.watermark_enabled and watermark_input_index is not None:
        rgba_label = f"[wmrgba{step_index}]"
        wm_label = f"[wm{step_index}]"
        base_label = f"[base{step_index}]"
        filters.append(
            f"[{watermark_input_index}:v]format=rgba,colorchannelmixer=aa={export_preset.watermark_opacity:.3f}{rgba_label}"
        )
        filters.append(
            f"{rgba_label}{current_label}scale2ref=w=main_w*{export_preset.watermark_scale:.4f}:h=ow/mdar{wm_label}{base_label}"
        )
        overlay_x, overlay_y = _overlay_position_expr(
            export_preset.watermark_position,
            export_preset.watermark_margin,
        )
        next_label = "[vout]"
        filters.append(f"{base_label}{wm_label}overlay={overlay_x}:{overlay_y}{next_label}")
        current_label = next_label

    if current_label != "[vout]":
        filters.append(f"{current_label}null[vout]")
    return ";".join(filters), "[vout]"


def build_hardsub_command(
    *,
    ffmpeg_executable: str,
    source_video_path: Path,
    subtitle_path: Path,
    output_path: Path,
    export_preset: ExportPreset,
    replacement_audio_path: Path | None = None,
    watermark_path: Path | None = None,
) -> list[str]:
    effective_preset = export_preset.model_copy(
        update={"watermark_enabled": export_preset.watermark_enabled or watermark_path is not None}
    )
    video_codec = VIDEO_CODEC_MAP.get(effective_preset.video_codec.lower(), "libx264")
    audio_codec = AUDIO_CODEC_MAP.get(effective_preset.audio_codec.lower(), "aac")
    subtitle_codec = SUBTITLE_CODEC_BY_CONTAINER.get(effective_preset.container.lower(), "srt")
    command = [
        ffmpeg_executable,
        "-y",
        "-i",
        str(source_video_path),
    ]
    next_input_index = 1
    audio_map_label = "0:a?"
    if replacement_audio_path is not None:
        command.extend(
            [
                "-i",
                str(replacement_audio_path),
            ]
        )
        audio_map_label = f"{next_input_index}:a:0"
        next_input_index += 1
    subtitle_input_index: int | None = None
    if not effective_preset.burn_subtitles:
        subtitle_input_index = next_input_index
        command.extend(["-i", str(subtitle_path)])
        next_input_index += 1
    watermark_input_index: int | None = None
    if watermark_path is not None:
        watermark_input_index = next_input_index
        command.extend(["-i", str(watermark_path)])
    video_filter_graph, output_label = build_video_filter_graph(
        subtitle_path=subtitle_path,
        export_preset=effective_preset,
        watermark_input_index=watermark_input_index,
    )
    command.extend(["-filter_complex", video_filter_graph, "-map", output_label])
    command.extend(["-map", audio_map_label])
    if subtitle_input_index is not None:
        command.extend(["-map", f"{subtitle_input_index}:0"])
    command.extend(["-c:v", video_codec])
    if video_codec != "copy":
        command.extend(["-preset", "medium", "-crf", str(effective_preset.crf)])
    command.extend(["-pix_fmt", "yuv420p", "-movflags", "+faststart", "-c:a", audio_codec])
    if audio_codec != "copy":
        command.extend(["-b:a", "192k"])
    if subtitle_input_index is not None:
        command.extend(["-c:s", subtitle_codec])
    command.extend(["-progress", "pipe:1", "-nostats", str(output_path)])
    return command


def build_hardsub_output_path(workspace: ProjectWorkspace, export_preset: ExportPreset) -> Path:
    container = export_preset.container.lower() or "mp4"
    safe_name = re.sub(r'[<>:"/\\|?*\s]+', "_", workspace.name).strip("._") or "project"
    safe_preset = re.sub(r'[^a-zA-Z0-9_-]+', "_", export_preset.export_preset_id).strip("._") or "preset"
    mode_suffix = "hardsub" if export_preset.burn_subtitles else "softsub"
    return workspace.exports_dir / f"{safe_name}_{safe_preset}_{mode_suffix}.{container}"


def _progress_percent_from_ffmpeg_line(line: str, *, duration_ms: int | None) -> int | None:
    if not duration_ms or not line.startswith("out_time_ms="):
        return None
    raw_value = line.split("=", 1)[1].strip()
    if not raw_value or raw_value.upper() == "N/A":
        return None
    try:
        processed_us = int(raw_value)
    except ValueError:
        return None
    return min(99, max(20, int(processed_us / (duration_ms * 10))))


def export_hardsub_video(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    source_video_path: Path,
    subtitle_path: Path,
    ffmpeg_path: str | None,
    duration_ms: int | None = None,
    replacement_audio_path: Path | None = None,
    export_preset: ExportPreset | None = None,
    export_preset_id: str | None = None,
    watermark_override_path: Path | None = None,
) -> Path:
    ffmpeg_executable = ffmpeg_path or shutil.which("ffmpeg")
    if not ffmpeg_executable:
        raise RuntimeError("Khong tim thay ffmpeg.exe")
    if not source_video_path.exists():
        raise FileNotFoundError(f"Khong tim thay source video: {source_video_path}")
    if not subtitle_path.exists():
        raise FileNotFoundError(f"Khong tim thay subtitle file: {subtitle_path}")

    export_preset = export_preset or load_export_preset(workspace.root_dir, export_preset_id)
    watermark_path = resolve_watermark_path(
        workspace.root_dir,
        export_preset,
        watermark_override_path=watermark_override_path,
    )
    effective_preset = export_preset.model_copy(
        update={"watermark_enabled": export_preset.watermark_enabled or watermark_path is not None}
    )
    if effective_preset.watermark_enabled and watermark_path is None and watermark_override_path is not None:
        raise FileNotFoundError(f"Khong tim thay watermark file: {watermark_override_path}")
    output_path = build_hardsub_output_path(workspace, effective_preset)
    stage_hash = build_hardsub_stage_hash(
        source_video_path=source_video_path,
        subtitle_path=subtitle_path,
        export_preset=effective_preset,
        replacement_audio_path=replacement_audio_path,
        watermark_override_path=watermark_path,
    )
    manifest_dir = workspace.cache_dir / "export" / stage_hash
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "manifest.json"
    if manifest_path.exists() and output_path.exists():
        context.report_progress(100, "Dung cache hard-sub")
        return output_path

    command = build_hardsub_command(
        ffmpeg_executable=ffmpeg_executable,
        source_video_path=source_video_path,
        subtitle_path=subtitle_path,
        output_path=output_path,
        export_preset=effective_preset,
        replacement_audio_path=replacement_audio_path,
        watermark_path=watermark_path,
    )
    export_action_label = "burn-in ASS vao video" if effective_preset.burn_subtitles else "mux subtitle vao video"
    progress_label = "Dang render video hard-sub" if effective_preset.burn_subtitles else "Dang mux video soft-sub"
    context.report_progress(20, f"Dang {export_action_label}")
    last_lines: list[str] = []
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        if process.stdout is not None:
            for raw_line in process.stdout:
                context.cancellation_token.raise_if_canceled()
                line = raw_line.strip()
                if not line:
                    continue
                last_lines.append(line)
                last_lines = last_lines[-20:]
                progress = _progress_percent_from_ffmpeg_line(line, duration_ms=duration_ms)
                if progress is not None:
                    context.report_progress(progress, progress_label)
                elif line == "progress=end":
                    context.report_progress(99, "Dang hoan tat file video")
        return_code = process.wait()
    except JobCancelledError:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise

    if return_code != 0:
        raise RuntimeError("FFmpeg export video that bai:\n" + "\n".join(last_lines[-10:]))

    manifest = {
        "stage_hash": stage_hash,
        "source_video_path": str(source_video_path),
        "subtitle_path": str(subtitle_path),
        "replacement_audio_path": str(replacement_audio_path) if replacement_audio_path else None,
        "watermark_path": str(watermark_path) if watermark_path else None,
        "output_path": str(output_path),
        "export_preset": effective_preset.model_dump(mode="json"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    context.report_progress(100, "Da xuat video")
    return output_path
