from __future__ import annotations

from abc import ABC, abstractmethod
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
) -> str:
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
                }
                for row in segments
            ],
            "version": 1,
        }
    )
