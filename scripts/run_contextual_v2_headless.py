from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

from app.asr.faster_whisper_engine import FasterWhisperEngine
from app.asr.models import TranscriptionOptions
from app.asr.persistence import persist_transcription_result
from app.core.jobs import CancellationToken, JobContext
from app.core.settings import load_settings
from app.media.extract_audio import extract_audio_artifacts
from app.media.ffprobe_service import attach_source_video_to_project, probe_media
from app.project.bootstrap import ProjectInitRequest, bootstrap_project
from app.project.database import ProjectDatabase
from app.project.profiles import load_project_profile_state
from app.translate.contextual_pipeline import (
    build_contextual_translation_stage_hash,
    persist_contextual_translation_result,
)
from app.translate.contextual_runtime import run_contextual_translation
from app.translate.openai_engine import OpenAITranslationEngine
from app.translate.presets import load_prompt_template

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _print(message: str) -> None:
    print(message, flush=True)


def _context(stage: str) -> JobContext:
    return JobContext(
        job_id=f"contextual-v2-{stage}",
        logger_name=f"scripts.contextual_v2_headless.{stage}",
        cancellation_token=CancellationToken(),
        progress_callback=lambda value, message: _print(f"[{stage}] {value:>3}% {message}"),
    )


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
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Cat video that bai")


def _review_samples(database: ProjectDatabase, project_id: str, limit: int = 10) -> tuple[dict[str, int], list[dict[str, object]]]:
    rows = database.list_review_queue_items(project_id)
    counter: Counter[str] = Counter()
    samples: list[dict[str, object]] = []
    for row in rows[:limit]:
        reason_codes = json.loads(row["review_reason_codes_json"] or "[]")
        counter.update(str(code) for code in reason_codes)
        samples.append(
            {
                "segment_index": int(row["segment_index"]),
                "scene_index": int(row["scene_index"]),
                "source_text": row["source_text"],
                "subtitle_text": row["approved_subtitle_text"],
                "tts_text": row["approved_tts_text"],
                "review_reason_codes": reason_codes,
                "review_question": row["review_question"],
                "scene_summary": row["short_scene_summary"],
            }
        )
    return dict(counter), samples


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Contextual V2 headlessly on a clipped sample video.")
    parser.add_argument("--input-video", required=True, type=Path)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--project-name", default="Contextual V2 120s Test")
    parser.add_argument("--clip-duration", type=int, default=120)
    parser.add_argument("--source-language", default="zh")
    parser.add_argument("--target-language", default="vi")
    parser.add_argument("--asr-language", default="zh")
    parser.add_argument("--prompt-template-id")
    parser.add_argument("--project-profile-id")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = load_settings()
    if not settings.dependency_paths.ffmpeg_path:
        raise RuntimeError("Chua co ffmpeg_path trong settings")
    if not settings.dependency_paths.ffprobe_path:
        raise RuntimeError("Chua co ffprobe_path trong settings")
    if not settings.openai_api_key:
        raise RuntimeError("Chua co OpenAI API key trong settings")

    input_video = args.input_video.expanduser().resolve()
    project_root = args.project_root.expanduser().resolve()
    if project_root.exists() and any(project_root.iterdir()):
        raise RuntimeError(f"Thu muc du an da co du lieu: {project_root}")
    project_root.mkdir(parents=True, exist_ok=True)

    clip_path = project_root / f"source_{args.clip_duration}s.mp4"
    _print(f"Cat {args.clip_duration}s dau tu {input_video}")
    _cut_video(
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        input_video=input_video,
        output_video=clip_path,
        duration_seconds=args.clip_duration,
    )
    _print(f"Clip dau vao: {clip_path}")

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
    profile_state = load_project_profile_state(project_root)
    prompt_template_id = (
        args.prompt_template_id
        or (profile_state.recommended_prompt_template_id if profile_state is not None else None)
        or "contextual_cartoon_fun_adaptation"
    )

    metadata = probe_media(
        clip_path,
        ffprobe_path=settings.dependency_paths.ffprobe_path,
    )
    attach_source_video_to_project(workspace, metadata)

    extracted_audio = extract_audio_artifacts(
        _context("extract_audio"),
        workspace=workspace,
        metadata=metadata,
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
    )

    asr_result = FasterWhisperEngine(settings).transcribe(
        _context("asr"),
        audio_path=str(extracted_audio.audio_16k_path),
        options=TranscriptionOptions(
            model_name=settings.default_asr_model,
            language=args.asr_language,
            vad_filter=True,
            word_timestamps=True,
        ),
        duration_ms=metadata.duration_ms,
    )
    persist_transcription_result(
        workspace,
        result=asr_result,
        options=TranscriptionOptions(
            model_name=settings.default_asr_model,
            language=args.asr_language,
            vad_filter=True,
            word_timestamps=True,
        ),
    )

    segments = database.list_segments(workspace.project_id)
    selected_template = load_prompt_template(project_root, prompt_template_id)
    source_language = segments[0]["source_lang"] or args.source_language
    model = settings.default_translation_model
    stage_hash = build_contextual_translation_stage_hash(
        segments=segments,
        template=selected_template,
        project_root=project_root,
        model=model,
        source_language=source_language,
        target_language=args.target_language,
    )
    contextual_result = run_contextual_translation(
        _context("contextual"),
        workspace=workspace,
        database=database,
        engine=OpenAITranslationEngine(settings),
        segments=segments,
        selected_template=selected_template,
        source_language=source_language,
        target_language=args.target_language,
        model=model,
    )
    cache_path = persist_contextual_translation_result(
        workspace,
        database=database,
        stage_hash=stage_hash,
        selected_template=selected_template,
        target_language=args.target_language,
        scenes=contextual_result["scenes"],
        character_profiles=contextual_result["character_profiles"],
        relationship_profiles=contextual_result["relationship_profiles"],
        analyses=contextual_result["segment_analyses"],
        route_decisions=contextual_result.get("route_decisions"),
        metrics=contextual_result.get("metrics"),
        term_entity_sheets=contextual_result.get("term_entity_sheets"),
    )

    review_reason_counts, review_samples = _review_samples(database, workspace.project_id)
    summary = {
        "project_root": str(project_root),
        "input_video": str(input_video),
        "clip_path": str(clip_path),
        "translation_mode": "contextual_v2",
        "project_profile_id": args.project_profile_id,
        "project_profile_prompt_template_id": profile_state.recommended_prompt_template_id if profile_state else None,
        "selected_template": selected_template.template_id,
        "fast_path": contextual_result.get("fast_path"),
        "route_decisions": [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in contextual_result.get("route_decisions", [])
        ],
        "term_entity_sheets": [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in contextual_result.get("term_entity_sheets", [])
        ],
        "metrics": (
            contextual_result["metrics"].model_dump(mode="json")
            if hasattr(contextual_result.get("metrics"), "model_dump")
            else contextual_result.get("metrics")
        ),
        "stage_hash": stage_hash,
        "asr_segment_count": len(segments),
        "scene_count": len(contextual_result["scenes"]),
        "character_profile_count": len(contextual_result["character_profiles"]),
        "relationship_profile_count": len(contextual_result["relationship_profiles"]),
        "pending_review_count": database.count_pending_segment_reviews(workspace.project_id),
        "semantic_qc": contextual_result["semantic_qc"],
        "review_reason_counts": review_reason_counts,
        "cache_path": str(cache_path),
        "review_samples": review_samples,
    }
    summary_path = project_root / "contextual_run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _print(f"Project: {workspace.root_dir}")
    _print(f"Summary: {summary_path}")
    _print(f"Cache: {cache_path}")
    _print(
        f"KQ: segments={len(segments)} scenes={len(contextual_result['scenes'])} "
        f"review={summary['pending_review_count']} "
        f"qc_errors={contextual_result['semantic_qc']['error_count']} "
        f"qc_warnings={contextual_result['semantic_qc']['warning_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
