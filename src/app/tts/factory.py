from __future__ import annotations

from pathlib import Path

from .base import TTSEngine
from .models import VoicePreset
from .sapi_engine import SapiTTSEngine
from .vieneu_engine import VieneuTTSEngine


def create_tts_engine(preset: VoicePreset, *, project_root: Path | None = None) -> TTSEngine:
    engine_name = preset.engine.strip().lower()
    if engine_name == "sapi":
        return SapiTTSEngine()
    if engine_name == "vieneu":
        return VieneuTTSEngine(project_root=project_root)
    raise ValueError(f"Engine TTS chua duoc ho tro: {preset.engine}")
