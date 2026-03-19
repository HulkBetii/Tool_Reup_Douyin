from __future__ import annotations

from pydantic import BaseModel, Field


class TranslationPromptTemplate(BaseModel):
    template_id: str
    name: str
    category: str = "default"
    source_lang: str = "auto"
    target_lang: str = "vi"
    system_prompt: str
    user_prompt_template: str
    output_schema_version: int = 1
    default_constraints_json: dict[str, object] = Field(default_factory=dict)
    notes: str = ""

    def render(
        self,
        *,
        source: str,
        source_language: str,
        target_language: str,
        glossary: str = "",
        constraints: str = "",
        context: str = "",
    ) -> str:
        return self.user_prompt_template.format(
            source=source,
            source_language=source_language,
            target_language=target_language,
            glossary=glossary,
            constraints=constraints,
            context=context,
        )


class TranslationOutputItem(BaseModel):
    segment_id: str
    translated_text: str
    subtitle_text: str
    tts_text: str


class BatchTranslationOutput(BaseModel):
    items: list[TranslationOutputItem]

