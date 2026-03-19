from __future__ import annotations

import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from app.core.hashing import build_stage_hash, fingerprint_path
from app.project.database import ProjectDatabase
from app.project.models import ProjectWorkspace, SegmentRecord

from .models import PersistedTranscription, SegmentDraft, TranscriptionOptions, TranscriptionResult


def build_asr_stage_hash(source_audio_path: Path, options: TranscriptionOptions) -> str:
    return build_stage_hash(
        {
            "stage": "asr",
            "audio": fingerprint_path(source_audio_path),
            "model_name": options.model_name,
            "language": options.language,
            "vad_filter": options.vad_filter,
            "word_timestamps": options.word_timestamps,
            "compute_type": options.compute_type,
            "version": 1,
        }
    )


def _build_segment_record(
    workspace: ProjectWorkspace,
    draft: SegmentDraft,
    *,
    source_language: str | None,
) -> SegmentRecord:
    segment_id = str(
        uuid5(
            NAMESPACE_URL,
            f"{workspace.project_id}:{draft.segment_index}:{draft.start_ms}:{draft.end_ms}",
        )
    )
    normalized = " ".join(draft.source_text.split())
    return SegmentRecord(
        segment_id=segment_id,
        project_id=workspace.project_id,
        segment_index=draft.segment_index,
        start_ms=draft.start_ms,
        end_ms=draft.end_ms,
        source_lang=source_language,
        target_lang=None,
        source_text=draft.source_text,
        source_text_norm=normalized,
        subtitle_text=normalized,
        tts_text=normalized,
        status="transcribed",
        meta_json={
            "words": [
                {
                    "start_ms": word.start_ms,
                    "end_ms": word.end_ms,
                    "text": word.text,
                    "probability": word.probability,
                }
                for word in draft.words
            ]
        },
    )


def persist_transcription_result(
    workspace: ProjectWorkspace,
    *,
    result: TranscriptionResult,
    options: TranscriptionOptions,
) -> PersistedTranscription:
    stage_hash = build_asr_stage_hash(result.source_audio_path, options)
    cache_dir = workspace.cache_dir / "asr" / stage_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    segments_json_path = cache_dir / "segments.json"

    database = ProjectDatabase(workspace.database_path)
    segment_records = [
        _build_segment_record(
            workspace,
            draft,
            source_language=result.detected_language or draft.language,
        )
        for draft in result.segments
    ]
    database.replace_segments(workspace.project_id, segment_records)
    database.sync_canonical_subtitle_track(workspace.project_id)

    payload = {
        "stage_hash": stage_hash,
        "source_audio_path": str(result.source_audio_path),
        "detected_language": result.detected_language,
        "segment_count": len(result.segments),
        "segments": [
            {
                "segment_index": segment.segment_index,
                "start_ms": segment.start_ms,
                "end_ms": segment.end_ms,
                "source_text": segment.source_text,
                "language": segment.language,
                "words": [
                    {
                        "start_ms": word.start_ms,
                        "end_ms": word.end_ms,
                        "text": word.text,
                        "probability": word.probability,
                    }
                    for word in segment.words
                ],
            }
            for segment in result.segments
        ],
    }
    segments_json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return PersistedTranscription(
        stage_hash=stage_hash,
        cache_dir=cache_dir,
        segments_json_path=segments_json_path,
        segment_count=len(result.segments),
    )
