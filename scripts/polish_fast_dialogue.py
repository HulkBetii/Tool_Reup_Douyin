from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar
from uuid import uuid4

from app.audio.mixdown import mix_audio_tracks
from app.audio.voiceover_track import build_voice_track
from app.core.jobs import CancellationToken, JobContext
from app.core.logging import get_job_log_path
from app.core.settings import load_settings
from app.exporting.presets import get_export_preset
from app.media.ffprobe_service import probe_media
from app.project.bootstrap import sync_project_snapshot, utc_now_iso
from app.project.database import ProjectDatabase
from app.project.models import JobRunRecord, ProjectWorkspace, SubtitleTrackRecord
from app.subtitle.editing import build_subtitle_event_records
from app.subtitle.export import export_subtitles
from app.subtitle.hardsub import export_hardsub_video
from app.subtitle.qc import SubtitleQcConfig, SubtitleQcReport, analyze_subtitle_rows
from app.tts.factory import create_tts_engine
from app.tts.pipeline import synthesize_segments
from app.tts.presets import list_voice_presets

T = TypeVar("T")


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


@dataclass(slots=True)
class PolishResult:
    track_id: str
    before_qc: SubtitleQcReport
    after_qc: SubtitleQcReport
    summary_path: Path
    final_video_path: Path
    subtitle_srt: Path
    subtitle_ass: Path
    mixed_audio_path: Path


def _print(message: str) -> None:
    print(message, flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polish fast-dialogue subtitle rows and rerender project outputs.")
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--track-name", default="Polished Fast Dialogue")
    parser.add_argument("--target-download", type=Path)
    return parser.parse_args()


def _load_workspace(project_root: Path) -> ProjectWorkspace:
    payload = json.loads((project_root / "project.json").read_text(encoding="utf-8"))
    return ProjectWorkspace(
        project_id=payload["project_id"],
        name=payload["name"],
        root_dir=project_root,
        database_path=project_root / "project.db",
        project_json_path=project_root / "project.json",
        logs_dir=project_root / "logs",
        cache_dir=project_root / "cache",
        exports_dir=project_root / "exports",
        video_asset_id=payload.get("video_asset_id"),
        source_video_path=project_root / "source_input.mp4",
    )


def _normalize_qc_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "segment_id": str(row["segment_id"]),
            "segment_index": int(row["segment_index"]),
            "start_ms": int(row["start_ms"]),
            "end_ms": int(row["end_ms"]),
            "source_text": str(row.get("source_text", "") or ""),
            "translated_text": str(row.get("translated_text", "") or ""),
            "subtitle_text": str(row.get("subtitle_text", "") or ""),
        }
        for row in rows
    ]


def _analyze(rows: list[dict[str, object]]) -> SubtitleQcReport:
    return analyze_subtitle_rows(_normalize_qc_rows(rows), config=SubtitleQcConfig())


def _coerce_meta_json(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return {}
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _wrap_subtitle_text(text: str, *, soft_limit: int = 24) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= soft_limit or " " not in normalized:
        return normalized

    words = normalized.split()
    best_score: float | None = None
    best_text = normalized
    for split_index in range(1, len(words)):
        first = " ".join(words[:split_index]).strip()
        second = " ".join(words[split_index:]).strip()
        score = max(len(first), len(second)) + (abs(len(first) - len(second)) * 0.25)
        if best_score is None or score < best_score:
            best_score = score
            best_text = f"{first}\n{second}"
    return best_text


def _extend_short_durations(rows: list[dict[str, object]], *, total_duration_ms: int, min_gap_ms: int = 20) -> None:
    target_duration_ms = 800
    for index, row in enumerate(rows):
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        duration_ms = end_ms - start_ms
        if duration_ms >= target_duration_ms:
            continue

        needed = target_duration_ms - duration_ms
        prev_end_ms = int(rows[index - 1]["end_ms"]) if index > 0 else 0
        next_start_ms = (
            int(rows[index + 1]["start_ms"])
            if index + 1 < len(rows)
            else total_duration_ms
        )

        available_after = max(0, next_start_ms - min_gap_ms - end_ms)
        take_after = min(available_after, needed)
        end_ms += take_after
        needed -= take_after

        available_before = max(0, start_ms - (prev_end_ms + min_gap_ms))
        take_before = min(available_before, needed)
        start_ms -= take_before

        row["start_ms"] = start_ms
        row["end_ms"] = end_ms


def _parse_json_text(text: str) -> dict[str, object]:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        if candidate.startswith("json"):
            candidate = candidate[4:]
    candidate = candidate.strip()
    start = candidate.find("{")
    if start >= 0:
        candidate = candidate[start:]
    decoder = json.JSONDecoder()
    parsed, _ = decoder.raw_decode(candidate)
    return parsed


def _resolve_voice_preset(project_root: Path, preset_id: str):
    for preset in list_voice_presets(project_root):
        if preset.voice_preset_id == preset_id:
            return preset
    raise RuntimeError(f"Khong tim thay voice preset: {preset_id}")


def _resolve_audio_48k(project_root: Path) -> Path:
    candidates = sorted((project_root / "cache" / "extract_audio").rglob("audio_48k.wav"))
    if not candidates:
        raise RuntimeError("Khong tim thay audio_48k.wav trong cache/extract_audio")
    return candidates[-1]


def _build_rows_from_active_track(database: ProjectDatabase, workspace: ProjectWorkspace) -> list[dict[str, object]]:
    active_track = database.get_active_subtitle_track(workspace.project_id)
    if active_track is None:
        active_track = database.ensure_canonical_subtitle_track(workspace.project_id)
    return [dict(row) for row in database.list_subtitle_events(workspace.project_id, track_id=str(active_track["track_id"]))]


def _rewrite_flagged_rows(
    *,
    settings,
    rows: list[dict[str, object]],
    flagged_ids: list[str],
    model_name: str,
    progress: Callable[[int, str], None] | None = None,
    batch_size: int = 20,
) -> None:
    api_key = settings.openai_api_key
    if not api_key or not flagged_ids:
        return

    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("openai package chua duoc cai dat") from exc

    row_map = {str(row["segment_id"]): row for row in rows}
    client = OpenAI(api_key=api_key)
    batches = [flagged_ids[index : index + batch_size] for index in range(0, len(flagged_ids), batch_size)]
    total_batches = max(1, len(batches))

    instructions = (
        "You are polishing Vietnamese subtitle and dubbed dialogue for a comedic cartoon. "
        "Keep the original meaning and playful tone, but shorten lines so they fit fast delivery. "
        "Return Vietnamese only. subtitle_text must be concise, easy to read, max 2 lines. "
        "tts_text should sound spoken and natural, but if the duration is tight, keep it shorter rather than adding filler words. "
        "Do not add new facts. Preserve existing pronouns and intent when possible."
    )

    for batch_index, batch_ids in enumerate(batches, start=1):
        pending_ids = list(batch_ids)
        for attempt in range(3):
            payload = []
            for segment_id in pending_ids:
                row = row_map[segment_id]
                duration_ms = int(row["end_ms"]) - int(row["start_ms"])
                payload.append(
                    {
                        "segment_id": segment_id,
                        "duration_ms": duration_ms,
                        "target_subtitle_chars": max(4, int((duration_ms / 1000.0) * 17)),
                        "target_tts_chars": max(4, int((duration_ms / 1000.0) * 19)),
                        "source_text": row["source_text"],
                        "translated_text": row["translated_text"],
                        "subtitle_text": row["subtitle_text"],
                        "tts_text": row["tts_text"],
                    }
                )
            if progress:
                progress(
                    min(45, int((batch_index - 1) * 45 / total_batches)),
                    f"Rewrite batch {batch_index}/{total_batches} (attempt {attempt + 1})",
                )
            response = client.responses.create(
                model=model_name,
                instructions=instructions,
                input=json.dumps(payload, ensure_ascii=False, indent=2),
                temperature=0.2,
            )
            parsed = _parse_json_text(response.output_text)
            items = parsed.get("items", [])
            returned_ids = {str(item["segment_id"]) for item in items}
            expected_ids = set(pending_ids)
            extra_ids = sorted(returned_ids - expected_ids)
            if extra_ids:
                raise RuntimeError(f"Rewrite id mismatch. Extra={extra_ids}")
            for item in items:
                row = row_map[str(item["segment_id"])]
                row["subtitle_text"] = _wrap_subtitle_text(str(item["subtitle_text"]).strip())
                row["tts_text"] = " ".join(str(item["tts_text"]).split()).strip()
            pending_ids = sorted(expected_ids - returned_ids)
            if not pending_ids:
                break
        if pending_ids:
            print(f"Warning: rewrite left unchanged ids {pending_ids}", flush=True)


def _polish_rows(
    *,
    settings,
    rows: list[dict[str, object]],
    total_duration_ms: int,
    progress: Callable[[int, str], None] | None = None,
) -> tuple[list[dict[str, object]], SubtitleQcReport, SubtitleQcReport]:
    polished_rows = deepcopy(rows)
    for row in polished_rows:
        row["source_segment_id"] = row.get("source_segment_id") or row["segment_id"]
        row["subtitle_text"] = _wrap_subtitle_text(str(row.get("subtitle_text", "") or ""))
        row["tts_text"] = " ".join(str(row.get("tts_text", "") or "").split())
        row["status"] = "edited"
        row["meta_json"] = _coerce_meta_json(row.get("meta_json"))

    before_qc = _analyze(polished_rows)
    if progress:
        progress(5, f"QC truoc polish: {before_qc.warning_count} canh bao")

    _extend_short_durations(polished_rows, total_duration_ms=total_duration_ms)
    for row in polished_rows:
        row["subtitle_text"] = _wrap_subtitle_text(str(row.get("subtitle_text", "") or ""))

    intermediate_qc = _analyze(polished_rows)
    flagged_codes = {"high_cps", "high_cpl"}
    flagged_ids = [issue.segment_id for issue in intermediate_qc.issues if issue.code in flagged_codes]
    if progress:
        progress(10, f"Sau timing/wrap con {intermediate_qc.warning_count} canh bao")

    if flagged_ids:
        _rewrite_flagged_rows(
            settings=settings,
            rows=polished_rows,
            flagged_ids=flagged_ids,
            model_name=settings.default_translation_model,
            progress=progress,
        )
        for row in polished_rows:
            row["subtitle_text"] = _wrap_subtitle_text(str(row.get("subtitle_text", "") or ""))

    after_qc = _analyze(polished_rows)
    return polished_rows, before_qc, after_qc


class StageRunner:
    def __init__(self, workspace: ProjectWorkspace) -> None:
        self._workspace = workspace
        self._database = ProjectDatabase(workspace.database_path)

    def run(
        self,
        *,
        stage: str,
        description: str,
        handler: Callable[[JobContext], T],
        message_builder: Callable[[T], str],
        output_paths_builder: Callable[[T], list[Path]],
    ) -> T:
        job_id = str(uuid4())
        log_path = get_job_log_path(job_id)
        self._database.insert_job_run(
            JobRunRecord(
                job_id=job_id,
                project_id=self._workspace.project_id,
                stage=stage,
                description=description,
                status="running",
                progress=0,
                started_at=utc_now_iso(),
                log_path=str(log_path),
                message="Dang chay",
            )
        )

        def progress_callback(value: int, message: str) -> None:
            _print(f"[{stage}] {value:>3}% {message}")
            self._database.update_job_run(
                job_id,
                status="running",
                progress=value,
                message=message,
                log_path=str(log_path),
            )

        context = JobContext(
            job_id=job_id,
            logger_name=f"polish.{stage}",
            cancellation_token=CancellationToken(),
            progress_callback=progress_callback,
        )

        try:
            result = handler(context)
        except Exception as exc:
            self._database.update_job_run(
                job_id,
                status="failed",
                progress=0,
                message=str(exc),
                log_path=str(log_path),
                ended_at=utc_now_iso(),
                error_json={"message": str(exc)},
            )
            raise

        output_paths = output_paths_builder(result)
        message = message_builder(result)
        self._database.update_job_run(
            job_id,
            status="success",
            progress=100,
            message=message,
            log_path=str(log_path),
            ended_at=utc_now_iso(),
            output_paths=[str(path) for path in output_paths],
        )
        _print(f"[{stage}] OK {message}")
        return result


def main() -> int:
    args = _parse_args()
    project_root = args.project_root.expanduser().resolve()
    workspace = _load_workspace(project_root)
    settings = load_settings()
    database = ProjectDatabase(workspace.database_path)
    runner = StageRunner(workspace)

    metadata = probe_media(workspace.source_video_path, ffprobe_path=settings.dependency_paths.ffprobe_path)
    source_rows = _build_rows_from_active_track(database, workspace)

    def _run_polish(context: JobContext):
        return _polish_rows(
            settings=settings,
            rows=source_rows,
            total_duration_ms=metadata.duration_ms or max(int(row["end_ms"]) for row in source_rows),
            progress=context.report_progress,
        )

    polished_rows, before_qc, after_qc = runner.run(
        stage="polish_track",
        description="Polish fast-dialogue subtitle rows",
        handler=_run_polish,
        message_builder=lambda result: f"QC {result[1].warning_count} -> {result[2].warning_count} canh bao",
        output_paths_builder=lambda result: [],
    )

    new_track_id = str(uuid4())
    created_at = utc_now_iso()
    database.create_subtitle_track(
        SubtitleTrackRecord(
            track_id=new_track_id,
            project_id=workspace.project_id,
            name=args.track_name,
            kind="user",
            notes=f"Polished from active track. QC {before_qc.warning_count} -> {after_qc.warning_count}.",
            created_at=created_at,
            updated_at=created_at,
        ),
        set_active=True,
    )
    events = build_subtitle_event_records(workspace.project_id, new_track_id, polished_rows)
    database.replace_subtitle_events(workspace.project_id, new_track_id, events, updated_at=utc_now_iso())
    sync_project_snapshot(workspace)

    active_rows = database.list_subtitle_events(workspace.project_id, track_id=new_track_id)
    voice_preset_id = database.get_active_voice_preset_id(workspace.project_id) or "vieneu-default-vi"
    voice_preset = _resolve_voice_preset(workspace.root_dir, voice_preset_id)

    tts_result = runner.run(
        stage="tts_polish",
        description="Synthesize polished fast-dialogue track",
        handler=lambda context: synthesize_segments(
            context,
            workspace=workspace,
            segments=active_rows,
            preset=voice_preset,
            engine=create_tts_engine(voice_preset, project_root=workspace.root_dir),
            allow_source_fallback=False,
        ),
        message_builder=lambda result: f"Da tao {len(result.artifacts)} clip TTS",
        output_paths_builder=lambda result: [result.manifest_path],
    )
    database.apply_subtitle_event_audio_paths(
        workspace.project_id,
        new_track_id,
        [
            {
                "segment_id": item.segment_id,
                "audio_path": str(item.raw_wav_path),
                "status": "tts_ready",
            }
            for item in tts_result.artifacts
        ],
    )

    voice_track_result = runner.run(
        stage="voice_track_polish",
        description="Build polished voice track",
        handler=lambda context: build_voice_track(
            context,
            workspace=workspace,
            artifacts=tts_result.artifacts,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            total_duration_ms=metadata.duration_ms or max(int(row["end_ms"]) for row in active_rows),
        ),
        message_builder=lambda result: "Da tao voice track",
        output_paths_builder=lambda result: [result.manifest_path, result.voice_track_path],
    )
    database.apply_subtitle_event_audio_paths(
        workspace.project_id,
        new_track_id,
        [
            {
                "segment_id": item.segment_id,
                "audio_path": str(item.fitted_wav_path or item.raw_wav_path),
                "status": "voice_ready",
            }
            for item in voice_track_result.fitted_clips
        ],
    )

    mixed_audio_result = runner.run(
        stage="mixdown_polish",
        description="Mix polished voice track",
        handler=lambda context: mix_audio_tracks(
            context,
            workspace=workspace,
            original_audio_path=_resolve_audio_48k(workspace.root_dir),
            voice_track_path=voice_track_result.voice_track_path,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            original_volume=0.35,
            voice_volume=1.0,
            bgm_path=None,
            bgm_volume=0.15,
        ),
        message_builder=lambda result: "Da tron audio",
        output_paths_builder=lambda result: [result.manifest_path, result.mixed_audio_path],
    )

    final_rows = [dict(row) for row in database.list_subtitle_events(workspace.project_id, track_id=new_track_id)]
    subtitle_srt = runner.run(
        stage="export_srt_polish",
        description="Export polished SRT",
        handler=lambda context: export_subtitles(
            workspace,
            segments=final_rows,
            format_name="srt",
            allow_source_fallback=False,
        ),
        message_builder=lambda result: "Da xuat SRT",
        output_paths_builder=lambda result: [result],
    )
    subtitle_ass = runner.run(
        stage="export_ass_polish",
        description="Export polished ASS",
        handler=lambda context: export_subtitles(
            workspace,
            segments=final_rows,
            format_name="ass",
            allow_source_fallback=False,
        ),
        message_builder=lambda result: "Da xuat ASS",
        output_paths_builder=lambda result: [result],
    )

    export_preset_id = database.get_active_export_preset_id(workspace.project_id) or "youtube-16x9"
    export_preset = get_export_preset(workspace.root_dir, export_preset_id)
    if export_preset is None:
        raise RuntimeError(f"Khong tim thay export preset: {export_preset_id}")

    final_video_path = runner.run(
        stage="export_video_polish",
        description="Export polished hard-sub video",
        handler=lambda context: export_hardsub_video(
            context,
            workspace=workspace,
            source_video_path=workspace.source_video_path,
            subtitle_path=subtitle_ass,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            duration_ms=metadata.duration_ms,
            replacement_audio_path=mixed_audio_result.mixed_audio_path,
            export_preset=export_preset,
            export_preset_id=export_preset.export_preset_id,
        ),
        message_builder=lambda result: "Da xuat video polish",
        output_paths_builder=lambda result: [result],
    )

    refreshed_rows = [dict(row) for row in database.list_subtitle_events(workspace.project_id, track_id=new_track_id)]
    refreshed_qc = _analyze(refreshed_rows)
    summary = {
        "project_root": str(workspace.root_dir),
        "project_json_path": str(workspace.project_json_path),
        "active_track_id": new_track_id,
        "track_name": args.track_name,
        "subtitle_srt": str(subtitle_srt),
        "subtitle_ass": str(subtitle_ass),
        "mixed_audio": str(mixed_audio_result.mixed_audio_path),
        "final_video": str(final_video_path),
        "qc_before": {
            "errors": before_qc.error_count,
            "warnings": before_qc.warning_count,
            "total_segments": before_qc.total_segments,
        },
        "qc_after": {
            "errors": refreshed_qc.error_count,
            "warnings": refreshed_qc.warning_count,
            "total_segments": refreshed_qc.total_segments,
        },
    }
    if args.target_download:
        summary["target_download"] = str(args.target_download)
    summary_path = workspace.root_dir / "pipeline_result_polished.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    _print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
