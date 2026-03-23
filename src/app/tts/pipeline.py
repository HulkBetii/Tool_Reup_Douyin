from __future__ import annotations

import json
import wave
from pathlib import Path
from sqlite3 import Row

from app.core.jobs import JobContext
from app.project.models import ProjectWorkspace

from .base import TTSEngine, build_tts_clip_hash, build_tts_stage_hash
from .models import SynthesizedSegmentArtifact, SynthesizedSegmentsResult, VoicePreset


def _segment_tts_text(row: Row, *, allow_source_fallback: bool = True) -> str:
    if allow_source_fallback:
        return (row["tts_text"] or row["subtitle_text"] or row["translated_text"] or row["source_text"] or "").strip()
    return (row["tts_text"] or row["subtitle_text"] or row["translated_text"] or "").strip()


def _load_cached_artifact_metadata(manifest_path: Path) -> dict[str, dict[str, object]]:
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata_by_segment_id: dict[str, dict[str, object]] = {}
    for item in payload.get("artifacts", []):
        segment_id = str(item.get("segment_id") or "").strip()
        if segment_id:
            metadata_by_segment_id[segment_id] = item
    return metadata_by_segment_id


def _load_json_payload(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _probe_wav_duration_ms(path: Path) -> int:
    with wave.open(str(path), "rb") as handle:
        frame_rate = handle.getframerate()
        frame_count = handle.getnframes()
    if frame_rate <= 0:
        return 0
    return max(0, int(round(frame_count * 1000 / frame_rate)))


def synthesize_segments(
    context: JobContext,
    *,
    workspace: ProjectWorkspace,
    segments: list[Row],
    preset: VoicePreset,
    engine: TTSEngine,
    allow_source_fallback: bool = True,
    segment_voice_presets: dict[str, VoicePreset] | None = None,
    segment_speaker_keys: dict[str, str] | None = None,
) -> SynthesizedSegmentsResult:
    voice_preset_assignments = {
        str(segment_id): item.voice_preset_id
        for segment_id, item in (segment_voice_presets or {}).items()
        if item.voice_preset_id
    }
    stage_hash = build_tts_stage_hash(
        segments,
        preset,
        allow_source_fallback=allow_source_fallback,
        segment_voice_preset_ids=voice_preset_assignments,
        segment_voice_presets=segment_voice_presets,
    )
    cache_dir = workspace.cache_dir / "tts" / stage_hash
    raw_dir = cache_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    clip_cache_root = workspace.cache_dir / "tts" / "clips"
    manifest_path = cache_dir / "manifest.json"
    cached_metadata_by_segment_id = _load_cached_artifact_metadata(manifest_path)

    artifacts: list[SynthesizedSegmentArtifact] = []
    engine_cache: dict[str, TTSEngine] = {preset.voice_preset_id: engine}
    total = max(1, len(segments))
    for index, row in enumerate(segments, start=1):
        text = _segment_tts_text(row, allow_source_fallback=allow_source_fallback)
        if not text:
            continue
        segment_id = str(row["segment_id"])
        active_preset = (segment_voice_presets or {}).get(segment_id, preset)
        active_engine = engine_cache.get(active_preset.voice_preset_id)
        if active_engine is None:
            from .factory import create_tts_engine

            active_engine = create_tts_engine(active_preset, project_root=workspace.root_dir)
            engine_cache[active_preset.voice_preset_id] = active_engine
        legacy_output_path = raw_dir / f"{row['segment_index']:04d}_{row['segment_id']}.wav"
        cached_metadata = cached_metadata_by_segment_id.get(segment_id, {})
        cached_output_raw = str(cached_metadata.get("raw_wav_path") or "").strip()
        cached_output_path = Path(cached_output_raw) if cached_output_raw else None
        clip_hash = build_tts_clip_hash(text=text, preset=active_preset)
        clip_cache_dir = clip_cache_root / clip_hash
        shared_output_path = clip_cache_dir / "clip.wav"
        clip_manifest_path = clip_cache_dir / "manifest.json"
        output_path = next(
            (
                candidate
                for candidate in (cached_output_path, shared_output_path, legacy_output_path)
                if candidate is not None and candidate.exists() and candidate.stat().st_size > 44
            ),
            shared_output_path,
        )
        if output_path.exists() and output_path.stat().st_size > 44:
            clip_metadata = _load_json_payload(clip_manifest_path)
            metadata_source = (
                cached_metadata
                if cached_output_path is not None and output_path == cached_output_path and cached_metadata
                else clip_metadata
            )
            cached_duration_ms = int(metadata_source.get("duration_ms") or 0)
            if cached_duration_ms <= 0:
                cached_duration_ms = _probe_wav_duration_ms(output_path)
            artifact = SynthesizedSegmentArtifact(
                segment_id=segment_id,
                segment_index=int(row["segment_index"]),
                start_ms=int(row["start_ms"]),
                end_ms=int(row["end_ms"]),
                text=text,
                raw_wav_path=output_path,
                duration_ms=cached_duration_ms,
                sample_rate=int(metadata_source.get("sample_rate") or active_preset.sample_rate),
                voice_id=str(metadata_source.get("voice_id") or active_preset.voice_id),
                voice_preset_id=active_preset.voice_preset_id,
                speaker_key=(segment_speaker_keys or {}).get(segment_id),
                voice_speed=active_preset.speed,
                voice_volume=active_preset.volume,
                voice_pitch=active_preset.pitch,
            )
        else:
            context.cancellation_token.raise_if_canceled()
            clip_cache_dir.mkdir(parents=True, exist_ok=True)
            result = active_engine.synthesize(text=text, output_path=output_path, preset=active_preset)
            clip_manifest_path.write_text(
                json.dumps(
                    {
                        "clip_hash": clip_hash,
                        "text": text,
                        "raw_wav_path": str(result.wav_path),
                        "duration_ms": result.duration_ms,
                        "sample_rate": result.sample_rate,
                        "voice_id": result.voice_id,
                        "voice_preset": active_preset.model_dump(mode="json"),
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            artifact = SynthesizedSegmentArtifact(
                segment_id=segment_id,
                segment_index=int(row["segment_index"]),
                start_ms=int(row["start_ms"]),
                end_ms=int(row["end_ms"]),
                text=text,
                raw_wav_path=result.wav_path,
                duration_ms=result.duration_ms,
                sample_rate=result.sample_rate,
                voice_id=result.voice_id,
                voice_preset_id=active_preset.voice_preset_id,
                speaker_key=(segment_speaker_keys or {}).get(segment_id),
                voice_speed=active_preset.speed,
                voice_volume=active_preset.volume,
                voice_pitch=active_preset.pitch,
            )
        artifacts.append(artifact)
        context.report_progress(min(85, int(index * 85 / total)), f"TTS {index}/{total}")

    payload = {
        "stage_hash": stage_hash,
        "voice_preset": preset.model_dump(mode="json"),
        "artifacts": [
            {
                "segment_id": item.segment_id,
                "segment_index": item.segment_index,
                "start_ms": item.start_ms,
                "end_ms": item.end_ms,
                "text": item.text,
                "raw_wav_path": str(item.raw_wav_path),
                "duration_ms": item.duration_ms,
                "sample_rate": item.sample_rate,
                "voice_id": item.voice_id,
                "voice_preset_id": item.voice_preset_id,
                "speaker_key": item.speaker_key,
                "voice_speed": item.voice_speed,
                "voice_volume": item.voice_volume,
                "voice_pitch": item.voice_pitch,
            }
            for item in artifacts
        ],
    }
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    context.report_progress(100, "Da tao TTS clips")
    return SynthesizedSegmentsResult(
        stage_hash=stage_hash,
        cache_dir=cache_dir,
        manifest_path=manifest_path,
        artifacts=artifacts,
    )


def load_synthesized_segments(workspace: ProjectWorkspace, stage_hash: str) -> SynthesizedSegmentsResult | None:
    manifest_path = workspace.cache_dir / "tts" / stage_hash / "manifest.json"
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = [
        SynthesizedSegmentArtifact(
            segment_id=item["segment_id"],
            segment_index=int(item["segment_index"]),
            start_ms=int(item["start_ms"]),
            end_ms=int(item["end_ms"]),
            text=item["text"],
            raw_wav_path=Path(item["raw_wav_path"]),
            duration_ms=int(item.get("duration_ms", 0)),
            sample_rate=int(item.get("sample_rate", 0)),
            voice_id=item.get("voice_id"),
            voice_preset_id=item.get("voice_preset_id"),
            speaker_key=item.get("speaker_key"),
            voice_speed=item.get("voice_speed"),
            voice_volume=item.get("voice_volume"),
            voice_pitch=item.get("voice_pitch"),
        )
        for item in payload.get("artifacts", [])
    ]
    return SynthesizedSegmentsResult(
        stage_hash=payload["stage_hash"],
        cache_dir=manifest_path.parent,
        manifest_path=manifest_path,
        artifacts=artifacts,
    )
