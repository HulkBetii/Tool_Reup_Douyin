from __future__ import annotations

import json
from pathlib import Path
from sqlite3 import Row

import pysubs2

from app.core.hashing import build_stage_hash
from app.project.models import ProjectWorkspace


def _segment_fingerprint(
    segments: list[Row],
    *,
    allow_source_fallback: bool,
) -> list[dict[str, object]]:
    return [
        {
            "segment_id": row["segment_id"],
            "start_ms": row["start_ms"],
            "end_ms": row["end_ms"],
            "subtitle_text": row["subtitle_text"],
            "translated_text": row["translated_text"],
            "source_text": row["source_text"] if allow_source_fallback else None,
        }
        for row in segments
    ]


def build_subtitle_stage_hash(
    segments: list[Row],
    *,
    format_name: str,
    allow_source_fallback: bool = True,
) -> str:
    return build_stage_hash(
        {
            "stage": "subtitle_export",
            "format": format_name,
            "allow_source_fallback": allow_source_fallback,
            "segments": _segment_fingerprint(segments, allow_source_fallback=allow_source_fallback),
            "version": 1,
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


def _build_subs_from_segments(
    project_root: Path,
    segments: list[Row],
    *,
    ass: bool,
    allow_source_fallback: bool,
) -> pysubs2.SSAFile:
    subs = pysubs2.SSAFile()
    for row in segments:
        text = _segment_subtitle_text(row, allow_source_fallback=allow_source_fallback)
        if ass:
            text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", r"\N")
        subs.append(
            pysubs2.SSAEvent(
                start=int(row["start_ms"]),
                end=int(row["end_ms"]),
                text=text,
            )
        )

    if ass:
        style_config = _load_ass_style(project_root)
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
) -> Path:
    subs = _build_subs_from_segments(
        workspace.root_dir,
        segments,
        ass=format_name == "ass",
        allow_source_fallback=allow_source_fallback,
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
) -> Path:
    normalized_format = format_name.lower()
    if normalized_format not in {"srt", "ass"}:
        raise ValueError("Chi ho tro xuat srt hoac ass")

    stage_hash = build_subtitle_stage_hash(
        segments,
        format_name=normalized_format,
        allow_source_fallback=allow_source_fallback,
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
    )


def export_preview_subtitles(
    workspace: ProjectWorkspace,
    *,
    segments: list[Row],
    format_name: str = "ass",
    allow_source_fallback: bool = True,
) -> Path:
    normalized_format = format_name.lower()
    if normalized_format not in {"srt", "ass"}:
        raise ValueError("Chi ho tro xuat srt hoac ass")
    output_path = workspace.cache_dir / "preview" / f"live_preview.{normalized_format}"
    return _write_subtitles_to_path(
        workspace,
        segments=segments,
        format_name=normalized_format,
        output_path=output_path,
        allow_source_fallback=allow_source_fallback,
    )
