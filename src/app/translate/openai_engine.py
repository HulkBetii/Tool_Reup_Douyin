from __future__ import annotations

import json
from sqlite3 import Row

from app.core.jobs import JobContext
from app.core.settings import AppSettings

from .models import (
    BatchTranslationOutput,
    DialogueAdaptationBatchOutput,
    ScenePlannerOutput,
    SemanticBatchOutput,
    SemanticCriticBatchOutput,
    TranslationPromptTemplate,
)


def _chunk_rows(rows: list[Row], batch_size: int) -> list[list[Row]]:
    return [rows[index : index + batch_size] for index in range(0, len(rows), batch_size)]


class OpenAITranslationEngine:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings

    def _build_client(self):
        api_key = self._settings.openai_api_key
        if not api_key:
            raise RuntimeError("Chua co OpenAI API key trong settings")

        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("openai package chua duoc cai dat") from exc
        return OpenAI(api_key=api_key)

    def _call_structured_output(
        self,
        *,
        client,
        model: str,
        template: TranslationPromptTemplate,
        user_prompt: str,
        output_model,
    ):
        response = client.responses.parse(
            model=model,
            instructions=template.system_prompt,
            input=user_prompt,
            text_format=output_model,
            temperature=0.2,
        )
        parsed = response.output_parsed
        if not parsed:
            raise RuntimeError("Model khong tra ve du lieu co parse duoc")
        return parsed

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
        client = self._build_client()
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
            parsed = self._call_structured_output(
                client=client,
                model=selected_model,
                template=template,
                user_prompt=user_prompt,
                output_model=BatchTranslationOutput,
            )

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

    def plan_scene(
        self,
        context: JobContext,
        *,
        template: TranslationPromptTemplate,
        scene_payload: dict[str, object],
        source_language: str,
        target_language: str,
        context_payload: dict[str, object],
        glossary_payload: dict[str, object],
        model: str | None = None,
    ) -> ScenePlannerOutput:
        client = self._build_client()
        selected_model = model or self._settings.default_translation_model
        user_prompt = template.render(
            source=json.dumps(scene_payload, ensure_ascii=False, indent=2),
            source_language=source_language,
            target_language=target_language,
            glossary=json.dumps(glossary_payload, ensure_ascii=False, indent=2),
            constraints=json.dumps(template.default_constraints_json, ensure_ascii=False, indent=2),
            context=json.dumps(context_payload, ensure_ascii=False, indent=2),
        )
        context.report_progress(25, "Dang lap ke hoach scene")
        return self._call_structured_output(
            client=client,
            model=selected_model,
            template=template,
            user_prompt=user_prompt,
            output_model=ScenePlannerOutput,
        )

    def analyze_semantics(
        self,
        context: JobContext,
        *,
        template: TranslationPromptTemplate,
        batch_payload: dict[str, object],
        source_language: str,
        target_language: str,
        context_payload: dict[str, object],
        glossary_payload: dict[str, object],
        model: str | None = None,
    ) -> SemanticBatchOutput:
        client = self._build_client()
        selected_model = model or self._settings.default_translation_model
        user_prompt = template.render(
            source=json.dumps(batch_payload, ensure_ascii=False, indent=2),
            source_language=source_language,
            target_language=target_language,
            glossary=json.dumps(glossary_payload, ensure_ascii=False, indent=2),
            constraints=json.dumps(template.default_constraints_json, ensure_ascii=False, indent=2),
            context=json.dumps(context_payload, ensure_ascii=False, indent=2),
        )
        context.report_progress(45, "Dang phan tich semantic/discourse")
        return self._call_structured_output(
            client=client,
            model=selected_model,
            template=template,
            user_prompt=user_prompt,
            output_model=SemanticBatchOutput,
        )

    def adapt_dialogue(
        self,
        context: JobContext,
        *,
        template: TranslationPromptTemplate,
        batch_payload: dict[str, object],
        source_language: str,
        target_language: str,
        context_payload: dict[str, object],
        glossary_payload: dict[str, object],
        model: str | None = None,
    ) -> DialogueAdaptationBatchOutput:
        client = self._build_client()
        selected_model = model or self._settings.default_translation_model
        user_prompt = template.render(
            source=json.dumps(batch_payload, ensure_ascii=False, indent=2),
            source_language=source_language,
            target_language=target_language,
            glossary=json.dumps(glossary_payload, ensure_ascii=False, indent=2),
            constraints=json.dumps(template.default_constraints_json, ensure_ascii=False, indent=2),
            context=json.dumps(context_payload, ensure_ascii=False, indent=2),
        )
        context.report_progress(65, "Dang bien tap subtitle va loi TTS")
        return self._call_structured_output(
            client=client,
            model=selected_model,
            template=template,
            user_prompt=user_prompt,
            output_model=DialogueAdaptationBatchOutput,
        )

    def critique_dialogue(
        self,
        context: JobContext,
        *,
        template: TranslationPromptTemplate,
        batch_payload: dict[str, object],
        source_language: str,
        target_language: str,
        context_payload: dict[str, object],
        glossary_payload: dict[str, object],
        model: str | None = None,
    ) -> SemanticCriticBatchOutput:
        client = self._build_client()
        selected_model = model or self._settings.default_translation_model
        user_prompt = template.render(
            source=json.dumps(batch_payload, ensure_ascii=False, indent=2),
            source_language=source_language,
            target_language=target_language,
            glossary=json.dumps(glossary_payload, ensure_ascii=False, indent=2),
            constraints=json.dumps(template.default_constraints_json, ensure_ascii=False, indent=2),
            context=json.dumps(context_payload, ensure_ascii=False, indent=2),
        )
        context.report_progress(80, "Dang review semantic")
        return self._call_structured_output(
            client=client,
            model=selected_model,
            template=template,
            user_prompt=user_prompt,
            output_model=SemanticCriticBatchOutput,
        )
