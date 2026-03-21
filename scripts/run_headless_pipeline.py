from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable, TypeVar
from uuid import uuid4

from app.asr.faster_whisper_engine import FasterWhisperEngine
from app.asr.models import TranscriptionOptions
from app.asr.persistence import persist_transcription_result
from app.audio.mixdown import mix_audio_tracks
from app.audio.voiceover_track import build_voice_track
from app.core.jobs import CancellationToken, JobContext
from app.core.logging import get_job_log_path
from app.core.settings import load_settings
from app.exporting.presets import get_export_preset
from app.media.extract_audio import extract_audio_artifacts
from app.media.ffprobe_service import attach_source_video_to_project, probe_media
from app.project.bootstrap import bootstrap_project, sync_project_snapshot, utc_now_iso
from app.project.database import ProjectDatabase
from app.project.models import JobRunRecord, ProjectInitRequest, ProjectWorkspace
from app.project.profiles import resolve_project_profile_mix_defaults
from app.subtitle.export import export_subtitles
from app.subtitle.hardsub import export_hardsub_video
from app.subtitle.qc import SubtitleQcConfig, analyze_subtitle_rows
from app.translate.openai_engine import OpenAITranslationEngine
from app.translate.persistence import (
    build_translation_stage_hash,
    load_cached_translations,
    persist_translations,
)
from app.translate.presets import list_prompt_templates
from app.tts.base import build_tts_stage_hash
from app.tts.factory import create_tts_engine
from app.tts.pipeline import load_synthesized_segments, synthesize_segments
from app.tts.presets import list_voice_presets

T = TypeVar("T")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _print(message: str) -> None:
    print(message, flush=True)


def _cut_video(
    *,
    ffmpeg_path: str,
    input_video: Path,
    output_video: Path,
    duration_seconds: int,
) -> None:
    output_video.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_video),
        "-t",
        str(duration_seconds),
        "-c",
        "copy",
        str(output_video),
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Cắt video thất bại")


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
                message="Đang chạy",
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
            logger_name=f"headless.{stage}",
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


def _normalize_qc_rows(rows: list[object]) -> list[dict[str, object]]:
    return [
        {
            "segment_id": str(row["event_id"] if "event_id" in row.keys() else row["segment_id"]),
            "segment_index": int(row["event_index"] if "event_index" in row.keys() else row["segment_index"]),
            "start_ms": int(row["start_ms"]),
            "end_ms": int(row["end_ms"]),
            "source_text": row["source_text"] or "",
            "translated_text": row["translated_text"] or "",
            "subtitle_text": row["subtitle_text"] or "",
        }
        for row in rows
    ]


def _resolve_prompt_template(project_root: Path, template_id: str | None):
    templates = list_prompt_templates(project_root)
    if not templates:
        raise RuntimeError("Không tìm thấy prompt template trong dự án")
    if template_id:
        for template in templates:
            if template.template_id == template_id:
                return template
        raise RuntimeError(f"Không tìm thấy prompt template: {template_id}")
    return templates[0]


def _resolve_voice_preset(project_root: Path, preset_id: str):
    for preset in list_voice_presets(project_root):
        if preset.voice_preset_id == preset_id:
            return preset
    raise RuntimeError(f"Không tìm thấy voice preset: {preset_id}")


def _latest_active_track_rows(database: ProjectDatabase, workspace: ProjectWorkspace) -> tuple[object, list[object]]:
    active_track = database.get_active_subtitle_track(workspace.project_id)
    if active_track is None:
        active_track = database.ensure_canonical_subtitle_track(workspace.project_id)
        sync_project_snapshot(workspace)
    rows = database.list_subtitle_events(
        workspace.project_id,
        track_id=str(active_track["track_id"]),
    )
    return active_track, rows


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local video pipeline headlessly.")
    parser.add_argument("--settings-path", type=Path)
    parser.add_argument("--input-video", required=True, type=Path)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--project-name", default="Trung Viet 30s")
    parser.add_argument("--clip-duration", type=int, default=30)
    parser.add_argument("--source-language", default="zh")
    parser.add_argument("--target-language", default="vi")
    parser.add_argument("--asr-language", default="zh")
    parser.add_argument("--prompt-template-id", default="default-vi-style")
    parser.add_argument("--project-profile-id")
    parser.add_argument("--voice-preset-id", default="vieneu-default-vi")
    parser.add_argument("--export-preset-id", default="youtube-16x9")
    parser.add_argument("--original-volume", type=float)
    parser.add_argument("--voice-volume", type=float)
    parser.add_argument("--bgm-volume", type=float, default=0.15)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings_path = args.settings_path.expanduser().resolve() if args.settings_path else None
    settings = load_settings(settings_path=settings_path)
    if not settings.dependency_paths.ffmpeg_path:
        raise RuntimeError("Chưa có ffmpeg_path trong settings")
    if not settings.dependency_paths.ffprobe_path:
        raise RuntimeError("Chưa có ffprobe_path trong settings")
    if not settings.openai_api_key:
        raise RuntimeError("Chưa có OpenAI API key trong settings")

    input_video = args.input_video.expanduser().resolve()
    project_root = args.project_root.expanduser().resolve()
    if project_root.exists() and any(project_root.iterdir()):
        raise RuntimeError(f"Thư mục dự án đã có dữ liệu: {project_root}")
    project_root.mkdir(parents=True, exist_ok=True)

    clip_path = project_root / "source_30s.mp4"
    _print(f"Cắt {args.clip_duration}s đầu từ {input_video}")
    _cut_video(
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        input_video=input_video,
        output_video=clip_path,
        duration_seconds=args.clip_duration,
    )
    _print(f"Clip đầu vào: {clip_path}")

    workspace = bootstrap_project(
        ProjectInitRequest(
            name=args.project_name,
            root_dir=project_root,
            source_language=args.source_language,
            target_language=args.target_language,
            source_video_path=clip_path,
            project_profile_id=args.project_profile_id,
        )
    )
    database = ProjectDatabase(workspace.database_path)
    database.set_active_voice_preset_id(workspace.project_id, args.voice_preset_id)
    database.set_active_export_preset_id(workspace.project_id, args.export_preset_id)
    database.set_active_watermark_profile_id(workspace.project_id, "watermark-none")
    sync_project_snapshot(workspace)
    resolved_original_volume, resolved_voice_volume, profile_state = resolve_project_profile_mix_defaults(
        workspace.root_dir,
        original_volume=args.original_volume,
        voice_volume=args.voice_volume,
    )

    runner = StageRunner(workspace)

    metadata = runner.run(
        stage="probe_media",
        description="Đọc metadata clip 30 giây",
        handler=lambda context: probe_media(
            clip_path,
            ffprobe_path=settings.dependency_paths.ffprobe_path,
        ),
        message_builder=lambda result: "Đã đọc metadata video",
        output_paths_builder=lambda result: [clip_path],
    )
    attach_source_video_to_project(workspace, metadata)

    extracted_audio = runner.run(
        stage="extract_audio",
        description="Tách audio 16 kHz và 48 kHz",
        handler=lambda context: extract_audio_artifacts(
            context,
            workspace=workspace,
            metadata=metadata,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        ),
        message_builder=lambda result: "Đã tách audio cho ASR và mixdown",
        output_paths_builder=lambda result: [result.audio_16k_path, result.audio_48k_path, result.manifest_path],
    )

    asr_options = TranscriptionOptions(
        model_name=settings.default_asr_model,
        language=args.asr_language,
        vad_filter=True,
        word_timestamps=True,
    )
    def _run_asr(context: JobContext):
        transcribed = FasterWhisperEngine(settings).transcribe(
            context,
            audio_path=str(extracted_audio.audio_16k_path),
            options=asr_options,
            duration_ms=metadata.duration_ms,
        )
        return persist_transcription_result(
            workspace,
            result=transcribed,
            options=asr_options,
        )

    runner.run(
        stage="asr",
        description="Nhận diện lời nói bằng faster-whisper",
        handler=_run_asr,
        message_builder=lambda result: f"Đã ASR {result.segment_count} phân đoạn",
        output_paths_builder=lambda result: [result.segments_json_path],
    )

    segments = database.list_segments(workspace.project_id)
    template = _resolve_prompt_template(workspace.root_dir, args.prompt_template_id)
    model_name = settings.default_translation_model
    translation_stage_hash = build_translation_stage_hash(
        segments=segments,
        template=template,
        model=model_name,
        source_language=args.source_language,
        target_language=args.target_language,
    )

    def _translate(context: JobContext) -> Path:
        cached_items = load_cached_translations(workspace, translation_stage_hash)
        if cached_items:
            context.report_progress(95, "Dùng cache bản dịch")
            return persist_translations(
                workspace,
                translated_items=cached_items,
                stage_hash=translation_stage_hash,
                template=template,
                model=model_name,
                source_language=args.source_language,
                target_language=args.target_language,
            )
        translated_items = OpenAITranslationEngine(settings).translate_segments(
            context,
            segments=segments,
            template=template,
            source_language=args.source_language,
            target_language=args.target_language,
            model=model_name,
        )
        return persist_translations(
            workspace,
            translated_items=translated_items,
            stage_hash=translation_stage_hash,
            template=template,
            model=model_name,
            source_language=args.source_language,
            target_language=args.target_language,
        )

    translation_cache = runner.run(
        stage="translate",
        description="Dịch subtitle sang tiếng Việt",
        handler=_translate,
        message_builder=lambda result: "Đã dịch subtitle sang tiếng Việt",
        output_paths_builder=lambda result: [result],
    )

    voice_preset = _resolve_voice_preset(workspace.root_dir, args.voice_preset_id)
    active_track, subtitle_rows = _latest_active_track_rows(database, workspace)
    allow_source_fallback = args.source_language == args.target_language
    tts_stage_hash = build_tts_stage_hash(
        subtitle_rows,
        voice_preset,
        allow_source_fallback=allow_source_fallback,
    )

    def _run_tts(context: JobContext):
        cached = load_synthesized_segments(workspace, tts_stage_hash)
        if cached and all(item.raw_wav_path.exists() for item in cached.artifacts):
            context.report_progress(95, "Dùng cache TTS")
            return cached
        return synthesize_segments(
            context,
            workspace=workspace,
            segments=subtitle_rows,
            preset=voice_preset,
            engine=create_tts_engine(voice_preset, project_root=workspace.root_dir),
            allow_source_fallback=allow_source_fallback,
        )

    tts_result = runner.run(
        stage="tts",
        description="Tạo clip lồng tiếng tiếng Việt bằng VieNeu",
        handler=_run_tts,
        message_builder=lambda result: f"Đã tạo {len(result.artifacts)} clip TTS",
        output_paths_builder=lambda result: [result.manifest_path] + [item.raw_wav_path for item in result.artifacts],
    )
    database.apply_subtitle_event_audio_paths(
        workspace.project_id,
        str(active_track["track_id"]),
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
        stage="voice_track",
        description="Ghép clip TTS thành track giọng theo timeline",
        handler=lambda context: build_voice_track(
            context,
            workspace=workspace,
            artifacts=tts_result.artifacts,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            total_duration_ms=metadata.duration_ms or max(int(row["end_ms"]) for row in subtitle_rows),
        ),
        message_builder=lambda result: "Đã tạo track giọng",
        output_paths_builder=lambda result: [result.manifest_path, result.voice_track_path],
    )
    database.apply_subtitle_event_audio_paths(
        workspace.project_id,
        str(active_track["track_id"]),
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
        stage="mixdown",
        description="Trộn track giọng với audio gốc",
        handler=lambda context: mix_audio_tracks(
            context,
            workspace=workspace,
            original_audio_path=extracted_audio.audio_48k_path,
            voice_track_path=voice_track_result.voice_track_path,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            original_volume=resolved_original_volume,
            voice_volume=resolved_voice_volume,
            bgm_path=None,
            bgm_volume=args.bgm_volume,
        ),
        message_builder=lambda result: "Đã trộn audio",
        output_paths_builder=lambda result: [result.manifest_path, result.mixed_audio_path],
    )

    _track, subtitle_rows = _latest_active_track_rows(database, workspace)
    srt_path = runner.run(
        stage="export_srt",
        description="Xuất file SRT",
        handler=lambda context: export_subtitles(
            workspace,
            segments=subtitle_rows,
            format_name="srt",
            allow_source_fallback=allow_source_fallback,
        ),
        message_builder=lambda result: "Đã xuất SRT",
        output_paths_builder=lambda result: [result],
    )
    ass_path = runner.run(
        stage="export_ass",
        description="Xuất file ASS",
        handler=lambda context: export_subtitles(
            workspace,
            segments=subtitle_rows,
            format_name="ass",
            allow_source_fallback=allow_source_fallback,
        ),
        message_builder=lambda result: "Đã xuất ASS",
        output_paths_builder=lambda result: [result],
    )

    qc_report = analyze_subtitle_rows(_normalize_qc_rows(subtitle_rows), config=SubtitleQcConfig())
    _print(
        "QC phụ đề: "
        f"{qc_report.error_count} lỗi, {qc_report.warning_count} cảnh báo, {qc_report.total_segments} dòng"
    )

    export_preset = get_export_preset(workspace.root_dir, args.export_preset_id)
    if export_preset is None:
        raise RuntimeError(f"Không tìm thấy export preset: {args.export_preset_id}")
    final_video_path = runner.run(
        stage="export_video",
        description="Xuất video hard-sub với audio đã lồng tiếng",
        handler=lambda context: export_hardsub_video(
            context,
            workspace=workspace,
            source_video_path=clip_path,
            subtitle_path=ass_path,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            duration_ms=metadata.duration_ms,
            replacement_audio_path=mixed_audio_result.mixed_audio_path,
            export_preset=export_preset,
            export_preset_id=export_preset.export_preset_id,
        ),
        message_builder=lambda result: "Đã xuất video cuối",
        output_paths_builder=lambda result: [ass_path, result],
    )

    summary = {
        "project_root": str(workspace.root_dir),
        "project_profile_id": profile_state.project_profile_id if profile_state else None,
        "project_json_path": str(workspace.project_json_path),
        "input_video": str(input_video),
        "clip_video": str(clip_path),
        "translation_cache": str(translation_cache),
        "tts_manifest": str(tts_result.manifest_path),
        "voice_track": str(voice_track_result.voice_track_path),
        "mixed_audio": str(mixed_audio_result.mixed_audio_path),
        "subtitle_srt": str(srt_path),
        "subtitle_ass": str(ass_path),
        "final_video": str(final_video_path),
        "qc": {
            "errors": qc_report.error_count,
            "warnings": qc_report.warning_count,
            "total_segments": qc_report.total_segments,
        },
        "original_volume": resolved_original_volume,
        "voice_volume": resolved_voice_volume,
    }
    summary_path = workspace.root_dir / "pipeline_result.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    _print(f"Kết quả đã ghi vào: {summary_path}")
    _print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
