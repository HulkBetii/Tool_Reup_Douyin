from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from sqlite3 import Row

from app.core.hashing import build_stage_hash
from .models import SynthesisResult, VoicePreset


class TTSEngine(ABC):
    @abstractmethod
    def synthesize(
        self,
        *,
        text: str,
        output_path: Path,
        preset: VoicePreset,
    ) -> SynthesisResult:
        raise NotImplementedError


def build_tts_stage_hash(
    segments: list[Row],
    preset: VoicePreset,
    *,
    allow_source_fallback: bool = True,
    segment_voice_preset_ids: Mapping[str, str] | None = None,
    segment_voice_presets: Mapping[str, VoicePreset] | None = None,
) -> str:
    normalized_segment_voice_preset_ids = {
        str(segment_id): str(preset_id)
        for segment_id, preset_id in (segment_voice_preset_ids or {}).items()
        if str(segment_id).strip() and str(preset_id).strip()
    }
    normalized_segment_voice_presets = {
        str(segment_id): segment_preset.model_dump(mode="json")
        for segment_id, segment_preset in (segment_voice_presets or {}).items()
        if str(segment_id).strip()
    }
    return build_stage_hash(
        {
            "stage": "tts",
            "preset": preset.model_dump(mode="json"),
            "allow_source_fallback": allow_source_fallback,
            "segments": [
                {
                    "segment_id": row["segment_id"],
                    "segment_index": row["segment_index"],
                    "start_ms": row["start_ms"],
                    "end_ms": row["end_ms"],
                    "tts_text": row["tts_text"],
                    "subtitle_text": row["subtitle_text"],
                    "translated_text": row["translated_text"],
                    "voice_preset": normalized_segment_voice_presets.get(
                        str(row["segment_id"]),
                        {
                            **preset.model_dump(mode="json"),
                            "voice_preset_id": normalized_segment_voice_preset_ids.get(
                                str(row["segment_id"]),
                                preset.voice_preset_id,
                            ),
                        },
                    ),
                }
                for row in segments
            ],
            "version": 3,
        }
    )
