from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.audio.narration_incremental import (
    build_final_audio_track,
    build_mixed_scene_chunk,
    build_scene_voice_track,
)
from app.audio.mixdown import mix_audio_tracks
from app.audio.voiceover_track import build_voice_track
from app.core.jobs import CancellationToken, JobContext
from app.ops.doctor import format_blocked_message, run_doctor
from app.ops.project_safety import create_workspace_backup, inspect_workspace
from app.core.settings import load_settings
from app.media.extract_audio import extract_audio_artifacts, load_cached_audio_artifacts
from app.media.ffprobe_service import probe_media
from app.project.bootstrap import open_project, sync_project_snapshot
from app.project.database import ProjectDatabase
from app.project.profiles import load_project_profile_state, resolve_project_profile_mix_defaults
from app.subtitle.export import export_subtitles
from app.subtitle.hardsub import export_hardsub_video, export_visual_base_video, load_export_preset, mux_final_video
from app.tts.factory import create_tts_engine
from app.tts.pipeline import synthesize_segments
from app.tts.speaker_binding import build_speaker_binding_plan, resolve_segment_voice_presets
from app.tts.presets import list_voice_presets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _print(message: str) -> None:
    print(message, flush=True)


def _context(stage: str) -> JobContext:
    return JobContext(
        job_id=f"rerun-contextual-{stage}",
        logger_name=f"scripts.rerun_contextual.{stage}",
        cancellation_token=CancellationToken(),
        progress_callback=lambda value, message: _print(f"[{stage}] {value:>3}% {message}"),
    )


def _resolve_voice_preset(project_root: Path, voice_preset_id: str):
    for preset in list_voice_presets(project_root):
        if preset.voice_preset_id == voice_preset_id:
            return preset
    raise RuntimeError(f"Khong tim thay voice preset: {voice_preset_id}")


def _resolve_segment_voice_plan(
    *,
    workspace,
    database: ProjectDatabase,
    segments: list[object],
    default_preset,
):
    available_presets = {preset.voice_preset_id: preset for preset in list_voice_presets(workspace.root_dir)}
    available_presets[default_preset.voice_preset_id] = default_preset
    binding_rows = database.list_speaker_bindings(workspace.project_id)
    voice_policy_rows = database.list_voice_policies(workspace.project_id)
    register_style_policy_rows = database.list_register_voice_style_policies(workspace.project_id)
    relationship_rows = database.list_relationship_profiles(workspace.project_id)
    analysis_rows = database.list_segment_analyses(workspace.project_id)
    plan = build_speaker_binding_plan(
        subtitle_rows=segments,
        analysis_rows=analysis_rows,
        binding_rows=binding_rows,
        voice_policy_rows=voice_policy_rows,
        relationship_rows=relationship_rows,
        register_style_policy_rows=register_style_policy_rows,
        available_preset_ids=set(available_presets),
    )
    if (
        not plan.active_bindings
        and not plan.active_voice_policies
        and not getattr(plan, "active_register_voice_styles", False)
    ):
        return None, plan.segment_speaker_keys or None, plan
    if plan.unresolved_speakers or plan.missing_preset_ids:
        lines = ["Voice plan hien chua day du, chua the rerun downstream an toan."]
        if plan.unresolved_speakers:
            lines.append(f"- Speaker chua gan preset: {', '.join(plan.unresolved_speakers)}")
        if plan.missing_preset_ids:
            lines.append(f"- Preset khong con ton tai: {', '.join(plan.missing_preset_ids)}")
        raise RuntimeError("\n".join(lines))
    segment_voice_presets = resolve_segment_voice_presets(
        plan=plan,
        default_preset=default_preset,
        available_presets=available_presets,
    )
    return segment_voice_presets or None, plan.segment_speaker_keys or None, plan


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


def _is_narration_incremental_candidate(*, workspace, database: ProjectDatabase, voice_plan) -> tuple[bool, str]:
    profile_state = load_project_profile_state(workspace.root_dir)
    profile_is_narration = bool(
        profile_state is not None and profile_state.project_profile_id.startswith("zh-vi-narration-")
    )
    analysis_rows = database.list_segment_analyses(workspace.project_id)

    def _route_family_id(row: object) -> str:
        if isinstance(row, dict):
            return str(row.get("source_template_family_id") or "").strip()
        try:
            return str(row["source_template_family_id"] or "").strip()
        except Exception:
            return ""

    all_fast_narration = bool(analysis_rows) and all(
        (_route_family_id(row) == "contextual-narration-fast-vi")
        for row in analysis_rows
    )
    if not profile_is_narration and not all_fast_narration:
        return False, "not_narration_profile_or_route"
    if bool(getattr(voice_plan, "active_bindings", False)):
        return False, "speaker_binding_active"
    if bool(getattr(voice_plan, "active_voice_policies", False)):
        return False, "voice_policy_active"
    return True, ""


def _resolve_narration_scene_rows(database: ProjectDatabase, project_id: str, segments: list[object]) -> tuple[list[object], str]:
    if not hasattr(database, "list_scene_memories"):
        return [], "missing_scene_rows"
    scene_rows = database.list_scene_memories(project_id)
    if not scene_rows:
        return [], "missing_scene_rows"
    ordered_segments = sorted(segments, key=lambda item: int(item["segment_index"]))
    position_by_index = {int(row["segment_index"]): idx for idx, row in enumerate(ordered_segments)}
    expected_cursor = 0
    validated_rows: list[object] = []
    for scene_row in sorted(scene_rows, key=lambda item: int(item["scene_index"])):
        start_index = int(scene_row["start_segment_index"])
        end_index = int(scene_row["end_segment_index"])
        if start_index > end_index:
            return [], "scene_range_invalid"
        if start_index not in position_by_index or end_index not in position_by_index:
            return [], "scene_segment_bounds_missing"
        start_pos = position_by_index[start_index]
        end_pos = position_by_index[end_index]
        if start_pos != expected_cursor:
            return [], "scene_segments_not_contiguous"
        if end_pos < start_pos:
            return [], "scene_segments_overlap"
        expected_cursor = end_pos + 1
        validated_rows.append(scene_row)
    if expected_cursor != len(ordered_segments):
        return [], "scene_coverage_incomplete"
    return validated_rows, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Rerun downstream TTS -> export for a contextual project.")
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--voice-preset-id", default="vieneu-default-vi")
    parser.add_argument("--export-preset-id", default="youtube-16x9")
    parser.add_argument("--original-volume", type=float)
    parser.add_argument("--voice-volume", type=float)
    args = parser.parse_args()

    settings = load_settings()
    workspace = open_project(args.project_root.expanduser().resolve())
    database = ProjectDatabase(workspace.database_path)
    project_row = database.get_project()
    if project_row is None:
        raise RuntimeError("Khong tim thay project row")

    pending_review_count = database.count_pending_segment_reviews(workspace.project_id)
    if pending_review_count != 0:
        raise RuntimeError(f"Van con {pending_review_count} review item, chua the rerun downstream")

    analysis_rows = database.list_segment_analyses(workspace.project_id)
    has_semantic_qc_error = any(not bool(row["semantic_qc_passed"]) for row in analysis_rows)
    if has_semantic_qc_error:
        raise RuntimeError("Semantic QC chua sach, chua the rerun downstream")

    database.set_active_voice_preset_id(workspace.project_id, args.voice_preset_id)
    database.set_active_export_preset_id(workspace.project_id, args.export_preset_id)
    sync_project_snapshot(workspace)
    resolved_original_volume, resolved_voice_volume, profile_state = resolve_project_profile_mix_defaults(
        workspace.root_dir,
        original_volume=args.original_volume,
        voice_volume=args.voice_volume,
    )

    voice_preset = _resolve_voice_preset(workspace.root_dir, args.voice_preset_id)
    doctor_report = run_doctor(
        settings=settings,
        workspace=workspace,
        requested_stages=["tts", "voice_track", "mixdown", "export_video"],
        voice_preset=voice_preset,
    )
    blocked_message = format_blocked_message(
        doctor_report,
        stages=["tts", "voice_track", "mixdown", "export_video"],
        action_label="rerun downstream",
    )
    if blocked_message:
        raise RuntimeError(blocked_message)
    repair_report = inspect_workspace(workspace)
    if repair_report.error_count:
        raise RuntimeError(
            "Blocked because workspace chua an toan de rerun:\n"
            + "\n".join(f"- {issue.message}" for issue in repair_report.issues if issue.severity == "error")
        )
    backup_manifest = create_workspace_backup(
        workspace,
        reason="Safe rerun downstream before TTS -> export",
        stage="rerun_downstream",
    )
    segments = database.list_segments(workspace.project_id)
    total_duration_ms = max(int(row["end_ms"]) for row in segments) if segments else 0
    if total_duration_ms <= 0:
        raise RuntimeError("Khong co segment hop le de rerun TTS/export")
    segment_voice_presets, segment_speaker_keys, voice_plan = _resolve_segment_voice_plan(
        workspace=workspace,
        database=database,
        segments=segments,
        default_preset=voice_preset,
    )

    original_audio = _resolve_original_audio(workspace, database, settings)
    source_video_path = workspace.source_video_path
    if source_video_path is None:
        video_asset = database.get_primary_video_asset(workspace.project_id)
        if video_asset is None:
            raise RuntimeError("Khong tim thay source video")
        source_video_path = Path(str(video_asset["path"]))
    export_preset = load_export_preset(workspace.root_dir, args.export_preset_id)

    synthesized = synthesize_segments(
        _context("tts"),
        workspace=workspace,
        segments=segments,
        preset=voice_preset,
        engine=create_tts_engine(voice_preset, project_root=workspace.root_dir),
        allow_source_fallback=False,
        segment_voice_presets=segment_voice_presets,
        segment_speaker_keys=segment_speaker_keys,
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
    incremental_active = False
    incremental_fallback_reason = ""
    voice_track = None
    mixed_audio = None
    final_audio_track = None
    visual_base = None
    final_mux = None
    voice_scene_chunks = []
    mixed_scene_chunks = []
    incremental_candidate, incremental_fallback_reason = _is_narration_incremental_candidate(
        workspace=workspace,
        database=database,
        voice_plan=voice_plan,
    )
    if incremental_candidate and not export_preset.burn_subtitles:
        incremental_candidate = False
        incremental_fallback_reason = "export_preset_softsub"
    if incremental_candidate:
        scene_rows, scene_reason = _resolve_narration_scene_rows(database, workspace.project_id, segments)
        if not scene_rows:
            incremental_candidate = False
            incremental_fallback_reason = scene_reason
        else:
            voice_scene_chunks = [
                build_scene_voice_track(
                    _context(f"voice_scene_{row['scene_index']}"),
                    workspace=workspace,
                    scene_row=row,
                    artifacts=synthesized.artifacts,
                    ffmpeg_path=settings.dependency_paths.ffmpeg_path,
                )
                for row in scene_rows
            ]
            mixed_scene_chunks = [
                build_mixed_scene_chunk(
                    _context(f"mix_scene_{row['scene_index']}"),
                    workspace=workspace,
                    scene_row=row,
                    original_audio_path=original_audio.audio_48k_path,
                    voice_scene_chunk_path=voice_scene_chunk.voice_track_path,
                    ffmpeg_path=settings.dependency_paths.ffmpeg_path,
                    original_volume=resolved_original_volume,
                    voice_volume=resolved_voice_volume,
                )
                for row, voice_scene_chunk in zip(scene_rows, voice_scene_chunks, strict=True)
            ]
            final_audio_track = build_final_audio_track(
                _context("final_audio"),
                workspace=workspace,
                scene_chunks=mixed_scene_chunks,
                ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            )
            visual_base = export_visual_base_video(
                _context("visual_base"),
                workspace=workspace,
                source_video_path=source_video_path,
                subtitle_path=ass_path,
                ffmpeg_path=settings.dependency_paths.ffmpeg_path,
                duration_ms=total_duration_ms,
                export_preset=export_preset,
            )
            final_mux = mux_final_video(
                _context("final_mux"),
                workspace=workspace,
                visual_base_path=visual_base.visual_base_path,
                final_audio_path=final_audio_track.final_audio_path,
                ffmpeg_path=settings.dependency_paths.ffmpeg_path,
                export_preset=export_preset,
            )
            output_video = final_mux.output_path
            incremental_active = True
    if not incremental_active:
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
            original_volume=resolved_original_volume,
            voice_volume=resolved_voice_volume,
        )
        output_video = export_hardsub_video(
            _context("export"),
            workspace=workspace,
            source_video_path=source_video_path,
            subtitle_path=ass_path,
            ffmpeg_path=settings.dependency_paths.ffmpeg_path,
            duration_ms=total_duration_ms,
            replacement_audio_path=mixed_audio.mixed_audio_path,
            export_preset=export_preset,
        )

    summary = {
        "project_root": str(workspace.root_dir),
        "project_profile_id": profile_state.project_profile_id if profile_state else None,
        "voice_preset_id": args.voice_preset_id,
        "export_preset_id": args.export_preset_id,
        "original_volume": resolved_original_volume,
        "voice_volume": resolved_voice_volume,
        "pending_review_count": pending_review_count,
        "doctor_error_count": doctor_report.error_count,
        "doctor_warning_count": doctor_report.warning_count,
        "workspace_repair_error_count": repair_report.error_count,
        "workspace_repair_warning_count": repair_report.warning_count,
        "backup_dir": str(backup_manifest.backup_dir),
        "speaker_binding_active": bool(getattr(voice_plan, "active_bindings", False)),
        "voice_policy_active": bool(getattr(voice_plan, "active_voice_policies", False)),
        "speaker_binding_unresolved": list(getattr(voice_plan, "unresolved_speakers", [])),
        "speaker_binding_missing_preset_ids": list(getattr(voice_plan, "missing_preset_ids", [])),
        "speaker_bound_segment_count": len(getattr(voice_plan, "segment_voice_preset_ids", {})),
        "character_policy_hits": int(getattr(voice_plan, "character_policy_hits", 0)),
        "relationship_policy_hits": int(getattr(voice_plan, "relationship_policy_hits", 0)),
        "character_style_hits": int(getattr(voice_plan, "character_style_hits", 0)),
        "relationship_style_hits": int(getattr(voice_plan, "relationship_style_hits", 0)),
        "register_style_hits": int(getattr(voice_plan, "register_style_hits", 0)),
        "incremental_rerun_active": incremental_active,
        "incremental_fallback_reason": None if incremental_active else incremental_fallback_reason,
        "scene_chunk_count": len(voice_scene_chunks),
        "tts_manifest": str(synthesized.manifest_path),
        "voice_track_path": str(voice_track.voice_track_path) if voice_track is not None else None,
        "mixed_audio_path": str(mixed_audio.mixed_audio_path) if mixed_audio is not None else None,
        "final_audio_track_path": str(final_audio_track.final_audio_path) if final_audio_track is not None else None,
        "visual_base_path": str(visual_base.visual_base_path) if visual_base is not None else None,
        "srt_path": str(srt_path),
        "ass_path": str(ass_path),
        "output_video": str(output_video),
    }
    summary_path = workspace.root_dir / "rerun_contextual_downstream_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _print(f"Summary: {summary_path}")
    _print(f"Video: {output_video}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
