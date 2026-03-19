from __future__ import annotations

import json
from sqlite3 import Row

from app.core.jobs import JobContext
from app.core.settings import AppSettings

from .models import BatchTranslationOutput, TranslationPromptTemplate


def _chunk_rows(rows: list[Row], batch_size: int) -> list[list[Row]]:
    return [rows[index : index + batch_size] for index in range(0, len(rows), batch_size)]


class OpenAITranslationEngine:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings

    def translate_segments(
        self,
        context: JobContext,
        *,
        segments: list[Row],
        template: TranslationPromptTemplate,
        source_language: str,
        target_language: str,
        model: str | None = None,
        batch_size: int = 20,
    ) -> list[dict[str, str]]:
        api_key = self._settings.openai_api_key
        if not api_key:
            raise RuntimeError("Chua co OpenAI API key trong settings")

        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("openai package chua duoc cai dat") from exc

        client = OpenAI(api_key=api_key)
        selected_model = model or self._settings.default_translation_model

        translated_items: list[dict[str, str]] = []
        batches = _chunk_rows(segments, batch_size)
        total_batches = max(1, len(batches))

        for batch_index, batch in enumerate(batches, start=1):
            context.cancellation_token.raise_if_canceled()
            source_payload = [
                {
                    "segment_id": row["segment_id"],
                    "segment_index": row["segment_index"],
                    "start_ms": row["start_ms"],
                    "end_ms": row["end_ms"],
                    "source_text": row["source_text"],
                }
                for row in batch
            ]
            constraints = json.dumps(template.default_constraints_json, ensure_ascii=False)
            user_prompt = template.render(
                source=json.dumps(source_payload, ensure_ascii=False, indent=2),
                source_language=source_language,
                target_language=target_language,
                glossary="",
                constraints=constraints,
                context="",
            )

            progress = min(90, int(((batch_index - 1) / total_batches) * 100))
            context.report_progress(progress, f"Dang dich batch {batch_index}/{total_batches}")
            response = client.responses.parse(
                model=selected_model,
                instructions=template.system_prompt,
                input=user_prompt,
                text_format=BatchTranslationOutput,
                temperature=0.2,
            )

            parsed = response.output_parsed
            if not parsed:
                raise RuntimeError("Model khong tra ve du lieu co parse duoc")

            batch_ids = {row["segment_id"] for row in batch}
            returned_ids = {item.segment_id for item in parsed.items}
            if batch_ids != returned_ids:
                missing = sorted(batch_ids - returned_ids)
                extra = sorted(returned_ids - batch_ids)
                raise RuntimeError(
                    f"Mismatch segment ids trong translation output. Missing={missing} Extra={extra}"
                )

            for item in parsed.items:
                translated_items.append(
                    {
                        "segment_id": item.segment_id,
                        "translated_text": item.translated_text.strip(),
                        "translated_text_norm": " ".join(item.translated_text.split()),
                        "subtitle_text": item.subtitle_text.strip(),
                        "tts_text": item.tts_text.strip(),
                        "target_lang": target_language,
                        "status": "translated",
                    }
                )

        context.report_progress(95, "Da dich xong, dang persist")
        return translated_items
