from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.audio.mixdown import mix_audio_tracks
from app.audio.voiceover_track import build_voice_track
from app.core.jobs import CancellationToken, JobContext
from app.core.settings import load_settings
from app.media.extract_audio import extract_audio_artifacts, load_cached_audio_artifacts
from app.media.ffprobe_service import probe_media
from app.project.bootstrap import open_project, sync_project_snapshot
from app.project.database import ProjectDatabase
from app.subtitle.export import export_subtitles
from app.subtitle.hardsub import export_hardsub_video
from app.translate.contextual_pipeline import recompute_semantic_qc
from app.tts.factory import create_tts_engine
from app.tts.pipeline import synthesize_segments
from app.tts.presets import list_voice_presets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


REVIEW_FIXUPS: dict[int, dict[str, str | bool]] = {
    0: {
        "subtitle": "Ca diễn à?",
        "tts": "Ca diễn à?",
        "clear_review": True,
    },
    6: {
        "subtitle": "Sao lại bày cả đống dù thế này?",
        "tts": "Sao lại bày cả đống dù thế này hả?",
        "clear_review": True,
    },
    7: {
        "subtitle": "Mưa suốt nên chất cả đống dù.",
        "tts": "Tại mưa suốt nên chất cả đống dù luôn ấy!",
        "clear_review": True,
    },
    8: {
        "subtitle": "Dù để la liệt quá rồi đó.",
        "tts": "Để la liệt cả đống dù thế này rồi đó!",
        "clear_review": True,
    },
    12: {
        "subtitle": "Ra khỏi nhà mà cậu chẳng mang dù.",
        "tts": "Ra khỏi nhà mà cậu còn chẳng mang dù nữa chứ!",
        "clear_review": True,
    },
    14: {
        "subtitle": "Rồi lại đi mua một cái dù nhựa mới.",
        "tts": "Thế là lại đi mua một cái dù nhựa mới về!",
        "clear_review": True,
    },
    15: {
        "subtitle": "Nhưng lúc mình không mang dù...",
        "tts": "Nhưng lúc mình không mang dù...",
        "clear_review": True,
    },
    17: {
        "subtitle": "Trớ trêu là cứ quên dù thì trời lại mưa.",
        "tts": "Trớ trêu ghê, cứ hôm nào mình quên mang dù là trời lại đổ mưa!",
        "clear_review": True,
    },
    18: {
        "subtitle": "Ở nhà cậu nên dọn đống dù lại đi chứ.",
        "tts": "Ở nhà thì cậu cũng nên dọn đống dù đó lại đi chứ!",
        "clear_review": True,
    },
    20: {
        "subtitle": "Đống dù đó là cậu tự mua về mà.",
        "tts": "Đống dù đó là cậu tự mua về mà!",
        "clear_review": True,
    },
    21: {
        "subtitle": "Thì cậu phải tự dọn chứ.",
        "tts": "Thì cậu phải tự dọn đống dù đó chứ!",
        "clear_review": True,
    },
    28: {
        "subtitle": "Cái dù này hỏng rồi.",
        "tts": "Cái dù này hỏng rồi này!",
        "clear_review": False,
    },
    29: {
        "subtitle": "Không thì gãy nan dù, không thì rách toạc.",
        "tts": "Không thì gãy nan dù, không thì rách toạc ra thôi!",
        "clear_review": True,
    },
    30: {
        "subtitle": "Mấy cái dù hỏng đó còn giữ làm gì?",
        "tts": "Mấy cái dù hỏng đó còn giữ lại làm gì nữa?",
        "clear_review": True,
    },
}


def _print(message: str) -> None:
    print(message, flush=True)


def _context(stage: str) -> JobContext:
    return JobContext(
        job_id=f"finalize-contextual-{stage}",
        logger_name=f"scripts.finalize_contextual.{stage}",
        cancellation_token=CancellationToken(),
        progress_callback=lambda value, message: _print(f"[{stage}] {value:>3}% {message}"),
    )


def _resolve_voice_preset(project_root: Path, voice_preset_id: str):
    for preset in list_voice_presets(project_root):
        if preset.voice_preset_id == voice_preset_id:
            return preset
    raise RuntimeError(f"Khong tim thay voice preset: {voice_preset_id}")


def _apply_review_fixups(database: ProjectDatabase, project_id: str) -> list[int]:
    analyses_by_index = {
        int(row["segment_index"]): row
        for row in database.list_segment_analyses(project_id)
    }
    touched_indexes: list[int] = []
    for segment_index, fixup in REVIEW_FIXUPS.items():
        row = analyses_by_index.get(segment_index)
        if row is None:
            raise RuntimeError(f"Khong tim thay segment analysis index={segment_index}")
        clear_review = bool(fixup["clear_review"])
        database.update_segment_analysis_review(
            project_id,
            str(row["segment_id"]),
            approved_subtitle_text=str(fixup["subtitle"]),
            approved_tts_text=str(fixup["tts"]),
            needs_human_review=False if clear_review else None,
            review_status="approved" if clear_review else None,
            review_scope="line" if clear_review else None,
            review_reason_codes_json=[] if clear_review else None,
            review_question="" if clear_review else None,
            semantic_qc_issues_json=[] if clear_review else None,
        )
        touched_indexes.append(segment_index)
    return sorted(touched_indexes)


def _resolve_original_audio(workspace, database: ProjectDatabase, settings):
    cached_artifacts = load_cached_audio_artifacts(workspace)
    if cached_artifacts and cached_artifacts.audio_48k_path.exists():
        return cached_artifacts
    video_asset = database.get_primary_video_asset(workspace.project_id)
    if video_asset is None:
        raise RuntimeError("Khong tim thay video asset cua project")
    metadata = probe_media(
        Path(str(video_asset["path"])),
        ffprobe_path=settings.dependency_paths.ffprobe_path,
    )
    return extract_audio_artifacts(
        _context("extract_audio"),
        workspace=workspace,
        metadata=metadata,
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Finalize contextual sample review items and rerun downstream.")
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--voice-preset-id", default="vieneu-default-vi")
    parser.add_argument("--export-preset-id", default="youtube-16x9")
    parser.add_argument("--original-volume", type=float, default=0.35)
    parser.add_argument("--voice-volume", type=float, default=1.0)
    args = parser.parse_args()

    settings = load_settings()
    workspace = open_project(args.project_root.expanduser().resolve())
    database = ProjectDatabase(workspace.database_path)
    project_row = database.get_project()
    if project_row is None:
        raise RuntimeError("Khong tim thay project row")

    touched_indexes = _apply_review_fixups(database, workspace.project_id)
    semantic_qc = recompute_semantic_qc(
        database,
        project_id=workspace.project_id,
        target_language=str(project_row["target_language"] or "vi"),
    )
    pending_review_count = database.count_pending_segment_reviews(workspace.project_id)
    if pending_review_count != 0:
        raise RuntimeError(f"Van con {pending_review_count} review item sau khi finalize")

    database.set_active_voice_preset_id(workspace.project_id, args.voice_preset_id)
    database.set_active_export_preset_id(workspace.project_id, args.export_preset_id)
    sync_project_snapshot(workspace)

    voice_preset = _resolve_voice_preset(workspace.root_dir, args.voice_preset_id)
    segments = database.list_segments(workspace.project_id)
    total_duration_ms = max(int(row["end_ms"]) for row in segments) if segments else 0
    if total_duration_ms <= 0:
        raise RuntimeError("Khong co segment hop le de rerun TTS/export")

    original_audio = _resolve_original_audio(workspace, database, settings)

    synthesized = synthesize_segments(
        _context("tts"),
        workspace=workspace,
        segments=segments,
        preset=voice_preset,
        engine=create_tts_engine(voice_preset, project_root=workspace.root_dir),
        allow_source_fallback=False,
    )
    voice_track = build_voice_track(
        _context("voice_track"),
        workspace=workspace,
        artifacts=synthesized.artifacts,
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        total_duration_ms=total_duration_ms,
    )
    mixed_audio = mix_audio_tracks(
        _context("mixdown"),
        workspace=workspace,
        original_audio_path=original_audio.audio_48k_path,
        voice_track_path=voice_track.voice_track_path,
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        original_volume=args.original_volume,
        voice_volume=args.voice_volume,
    )
    srt_path = export_subtitles(
        workspace,
        segments=segments,
        format_name="srt",
        allow_source_fallback=False,
    )
    ass_path = export_subtitles(
        workspace,
        segments=segments,
        format_name="ass",
        allow_source_fallback=False,
    )
    output_video = export_hardsub_video(
        _context("export"),
        workspace=workspace,
        source_video_path=workspace.source_video_path or Path(str(database.get_primary_video_asset(workspace.project_id)["path"])),
        subtitle_path=ass_path,
        ffmpeg_path=settings.dependency_paths.ffmpeg_path,
        duration_ms=total_duration_ms,
        replacement_audio_path=mixed_audio.mixed_audio_path,
        export_preset_id=args.export_preset_id,
    )

    summary = {
        "project_root": str(workspace.root_dir),
        "touched_segment_indexes": touched_indexes,
        "pending_review_count": pending_review_count,
        "semantic_qc": semantic_qc,
        "voice_preset_id": args.voice_preset_id,
        "export_preset_id": args.export_preset_id,
        "tts_manifest": str(synthesized.manifest_path),
        "voice_track_path": str(voice_track.voice_track_path),
        "mixed_audio_path": str(mixed_audio.mixed_audio_path),
        "srt_path": str(srt_path),
        "ass_path": str(ass_path),
        "output_video": str(output_video),
    }
    summary_path = workspace.root_dir / "finalize_contextual_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _print(f"Review items da xu ly: {len(touched_indexes)}")
    _print(f"Summary: {summary_path}")
    _print(f"Video: {output_video}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
