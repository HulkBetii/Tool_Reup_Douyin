from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(slots=True)
class RestoredPipelineState:
    subtitle_outputs: dict[str, Path] = field(default_factory=dict)
    tts_manifest_path: Path | None = None
    voice_track_path: Path | None = None
    mixed_audio_path: Path | None = None
    export_output_path: Path | None = None


def _row_value(row: Mapping[str, object], key: str) -> object:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def _parse_output_paths(raw_value: object) -> list[Path]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        values = raw_value
    else:
        try:
            values = json.loads(str(raw_value))
        except json.JSONDecodeError:
            return []
    if not isinstance(values, list):
        return []
    paths: list[Path] = []
    for value in values:
        candidate = Path(str(value))
        if candidate.exists():
            paths.append(candidate)
    return paths


def _first_matching_path(paths: list[Path], suffixes: set[str]) -> Path | None:
    for path in paths:
        if path.suffix.lower() in suffixes:
            return path
    return None


def restore_pipeline_state(job_runs: list[Mapping[str, object]]) -> RestoredPipelineState:
    state = RestoredPipelineState()
    for row in job_runs:
        status = str(_row_value(row, "status") or "").lower()
        if status != "success":
            continue

        stage = str(_row_value(row, "stage") or "").strip().lower()
        output_paths = _parse_output_paths(_row_value(row, "output_paths_json"))
        if not output_paths:
            continue

        if stage == "export_srt" and "srt" not in state.subtitle_outputs:
            output_path = _first_matching_path(output_paths, {".srt"})
            if output_path is not None:
                state.subtitle_outputs["srt"] = output_path
        elif stage == "export_ass" and "ass" not in state.subtitle_outputs:
            output_path = _first_matching_path(output_paths, {".ass"})
            if output_path is not None:
                state.subtitle_outputs["ass"] = output_path
        elif stage == "tts" and state.tts_manifest_path is None:
            state.tts_manifest_path = _first_matching_path(output_paths, {".json"})
        elif stage == "voice_track" and state.voice_track_path is None:
            state.voice_track_path = _first_matching_path(output_paths, {".wav"})
        elif stage == "mixdown" and state.mixed_audio_path is None:
            state.mixed_audio_path = _first_matching_path(output_paths, {".wav"})
        elif stage in {"export_video", "export_hardsub"} and state.export_output_path is None:
            state.export_output_path = _first_matching_path(output_paths, {".mp4", ".mkv", ".mov"})
    return state
