from __future__ import annotations

from pathlib import Path

from app.core.jobs import JobContext
from app.core.settings import AppSettings

from .base import ASREngine
from .models import SegmentDraft, TranscriptionOptions, TranscriptionResult, WordTimestamp


class FasterWhisperEngine(ASREngine):
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings

    def transcribe(
        self,
        context: JobContext,
        *,
        audio_path: str,
        options: TranscriptionOptions,
        duration_ms: int | None = None,
    ) -> TranscriptionResult:
        try:
            from faster_whisper import WhisperModel
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("faster-whisper chua duoc cai dat") from exc

        model = WhisperModel(
            options.model_name,
            device="cuda" if self._settings.gpu_enabled else "cpu",
            compute_type=options.compute_type or ("float16" if self._settings.gpu_enabled else "int8"),
            download_root=self._settings.model_cache_dir,
        )
        context.report_progress(5, "Dang khoi tao faster-whisper")

        segments, info = model.transcribe(
            audio=audio_path,
            language=options.language,
            vad_filter=options.vad_filter,
            word_timestamps=options.word_timestamps,
        )

        draft_segments: list[SegmentDraft] = []
        detected_language = getattr(info, "language", options.language)
        for index, segment in enumerate(segments):
            context.cancellation_token.raise_if_canceled()
            words: list[WordTimestamp] = []
            for word in getattr(segment, "words", []) or []:
                words.append(
                    WordTimestamp(
                        start_ms=int(float(getattr(word, "start", 0.0)) * 1000),
                        end_ms=int(float(getattr(word, "end", 0.0)) * 1000),
                        text=str(getattr(word, "word", "")).strip(),
                        probability=float(getattr(word, "probability", 0.0))
                        if getattr(word, "probability", None) is not None
                        else None,
                    )
                )

            start_ms = int(float(getattr(segment, "start", 0.0)) * 1000)
            end_ms = int(float(getattr(segment, "end", 0.0)) * 1000)
            text = str(getattr(segment, "text", "")).strip()
            draft_segments.append(
                SegmentDraft(
                    segment_index=index,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    source_text=text,
                    language=detected_language,
                    words=words,
                )
            )

            if duration_ms:
                progress = min(95, max(10, int((end_ms / max(duration_ms, 1)) * 90)))
                context.report_progress(progress, f"ASR segment {index + 1}")

        context.report_progress(98, "Da xong transcribe, dang persist")
        return TranscriptionResult(
            source_audio_path=Path(audio_path),
            detected_language=detected_language,
            duration_ms=duration_ms,
            segments=draft_segments,
        )

