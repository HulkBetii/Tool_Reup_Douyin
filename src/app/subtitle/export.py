from __future__ import annotations

import json
from pathlib import Path
from sqlite3 import Row

import pysubs2

from app.core.hashing import build_stage_hash
from app.project.profiles import normalize_subtitle_subtext_mode, resolve_subtitle_subtext_mode
from app.project.models import ProjectWorkspace


def _segment_fingerprint(
    segments: list[Row],
    *,
    allow_source_fallback: bool,
    subtitle_subtext_mode: str,
) -> list[dict[str, object]]:
    return [
        {
            "segment_id": row["segment_id"],
            "start_ms": row["start_ms"],
            "end_ms": row["end_ms"],
            "subtitle_text": row["subtitle_text"],
            "translated_text": row["translated_text"],
            "source_text": (
                row["source_text"]
                if (allow_source_fallback or subtitle_subtext_mode == "source_text")
                else None
            ),
        }
        for row in segments
    ]


def build_subtitle_stage_hash(
    segments: list[Row],
    *,
    format_name: str,
    allow_source_fallback: bool = True,
    subtitle_subtext_mode: str = "off",
) -> str:
    normalized_subtext_mode = normalize_subtitle_subtext_mode(subtitle_subtext_mode)
    return build_stage_hash(
        {
            "stage": "subtitle_export",
            "format": format_name,
            "allow_source_fallback": allow_source_fallback,
            "subtitle_subtext_mode": normalized_subtext_mode,
            "segments": _segment_fingerprint(
                segments,
                allow_source_fallback=allow_source_fallback,
                subtitle_subtext_mode=normalized_subtext_mode,
            ),
            "version": 2,
        }
    )


def _load_ass_style(project_root: Path) -> dict[str, object]:
    style_path = project_root / "presets" / "styles" / "default_ass_style.json"
    if not style_path.exists():
        return {}
    payload = json.loads(style_path.read_text(encoding="utf-8"))
    return payload.get("ass_style_json", {})


def _segment_subtitle_text(row: Row, *, allow_source_fallback: bool = True) -> str:
    if allow_source_fallback:
        return (row["subtitle_text"] or row["translated_text"] or row["source_text"] or "").strip()
    return (row["subtitle_text"] or row["translated_text"] or "").strip()


def _render_ass_with_source_subtext(primary_text: str, source_text: str, *, base_font_size: int) -> str:
    if not source_text.strip():
        return primary_text
    subtext_font_size = max(10, int(round(base_font_size * 0.75)))
    subtext_override = f"{{\\fs{subtext_font_size}\\1a&H55&\\3a&H55&}}"
    return f"{primary_text}\\N{subtext_override}{source_text}"


def _segment_render_text(
    row: Row,
    *,
    ass: bool,
    allow_source_fallback: bool,
    subtitle_subtext_mode: str,
    base_font_size: int,
) -> str:
    primary_text = _segment_subtitle_text(row, allow_source_fallback=allow_source_fallback)
    source_text = str(row["source_text"] or "").strip()
    normalized_subtext_mode = normalize_subtitle_subtext_mode(subtitle_subtext_mode)
    if ass:
        primary_text = primary_text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", r"\N")
        source_text = source_text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", r"\N")
        if normalized_subtext_mode == "source_text" and source_text:
            return _render_ass_with_source_subtext(primary_text, source_text, base_font_size=base_font_size)
        return primary_text
    if normalized_subtext_mode == "source_text" and source_text:
        return f"{primary_text}\n{source_text}" if primary_text else source_text
    return primary_text


def _build_subs_from_segments(
    project_root: Path,
    segments: list[Row],
    *,
    ass: bool,
    allow_source_fallback: bool,
    subtitle_subtext_mode: str,
) -> pysubs2.SSAFile:
    subs = pysubs2.SSAFile()
    style_config = _load_ass_style(project_root) if ass else {}
    base_font_size = int(style_config.get("FontSize", 42) or 42)
    for row in segments:
        text = _segment_render_text(
            row,
            ass=ass,
            allow_source_fallback=allow_source_fallback,
            subtitle_subtext_mode=subtitle_subtext_mode,
            base_font_size=base_font_size,
        )
        subs.append(
            pysubs2.SSAEvent(
                start=int(row["start_ms"]),
                end=int(row["end_ms"]),
                text=text,
            )
        )

    if ass:
        style = pysubs2.SSAStyle()
        for key, value in style_config.items():
            attr_name = key.lower()
            if hasattr(style, attr_name):
                if attr_name == "alignment" and isinstance(value, int):
                    value = pysubs2.Alignment(value)
                setattr(style, attr_name, value)
        subs.styles["Default"] = style
    return subs


def _write_subtitles_to_path(
    workspace: ProjectWorkspace,
    *,
    segments: list[Row],
    format_name: str,
    output_path: Path,
    allow_source_fallback: bool,
    subtitle_subtext_mode: str,
) -> Path:
    subs = _build_subs_from_segments(
        workspace.root_dir,
        segments,
        ass=format_name == "ass",
        allow_source_fallback=allow_source_fallback,
        subtitle_subtext_mode=subtitle_subtext_mode,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
    subs.save(str(temp_output_path))
    temp_output_path.replace(output_path)
    return output_path


def export_subtitles(
    workspace: ProjectWorkspace,
    *,
    segments: list[Row],
    format_name: str,
    allow_source_fallback: bool = True,
    subtitle_subtext_mode: str | None = None,
) -> Path:
    normalized_format = format_name.lower()
    if normalized_format not in {"srt", "ass"}:
        raise ValueError("Chi ho tro xuat srt hoac ass")
    resolved_subtext_mode = (
        normalize_subtitle_subtext_mode(subtitle_subtext_mode)
        if subtitle_subtext_mode is not None
        else resolve_subtitle_subtext_mode(workspace.root_dir)
    )

    stage_hash = build_subtitle_stage_hash(
        segments,
        format_name=normalized_format,
        allow_source_fallback=allow_source_fallback,
        subtitle_subtext_mode=resolved_subtext_mode,
    )
    cache_dir = workspace.cache_dir / "subs" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_path = cache_dir / f"track.{normalized_format}"
    return _write_subtitles_to_path(
        workspace,
        segments=segments,
        format_name=normalized_format,
        output_path=output_path,
        allow_source_fallback=allow_source_fallback,
        subtitle_subtext_mode=resolved_subtext_mode,
    )


def export_preview_subtitles(
    workspace: ProjectWorkspace,
    *,
    segments: list[Row],
    format_name: str = "ass",
    allow_source_fallback: bool = True,
    subtitle_subtext_mode: str | None = None,
) -> Path:
    normalized_format = format_name.lower()
    if normalized_format not in {"srt", "ass"}:
        raise ValueError("Chi ho tro xuat srt hoac ass")
    resolved_subtext_mode = (
        normalize_subtitle_subtext_mode(subtitle_subtext_mode)
        if subtitle_subtext_mode is not None
        else resolve_subtitle_subtext_mode(workspace.root_dir)
    )
    output_path = workspace.cache_dir / "preview" / f"live_preview.{normalized_format}"
    return _write_subtitles_to_path(
        workspace,
        segments=segments,
        format_name=normalized_format,
        output_path=output_path,
        allow_source_fallback=allow_source_fallback,
        subtitle_subtext_mode=resolved_subtext_mode,
    )
