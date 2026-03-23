from __future__ import annotations

from types import SimpleNamespace

from app.translate.models import TranslationPromptTemplate
from app.translate.openai_engine import OpenAITranslationEngine


def _template(*, family_id: str = "contextual-narration-fast-vi", schema_version: int = 2) -> TranslationPromptTemplate:
    return TranslationPromptTemplate(
        template_id="contextual_narration_fast_adaptation",
        name="Narration Fast",
        family_id=family_id,
        translation_mode="contextual_v2",
        role="dialogue_adaptation",
        category="narration",
        source_lang="zh",
        target_lang="vi",
        system_prompt="System prompt",
        user_prompt_template=(
            "Instructions.\n\n"
            "Constraints placeholder:\n{constraints}\n\n"
            "Context placeholder:\n{context}\n\n"
            "Glossary placeholder:\n{glossary}\n\n"
            "Source placeholder:\n{source}"
        ),
        output_schema_version=schema_version,
        default_constraints_json={"style": "clear"},
    )


def test_build_prompt_cache_key_is_stable_for_same_route() -> None:
    template = _template()

    first = OpenAITranslationEngine.build_prompt_cache_key(
        template=template,
        model="gpt-5.4",
        source_language="zh",
        target_language="vi",
        route_mode="narration_fast",
        project_profile_id="zh-vi-narration-fast-vieneu",
    )
    second = OpenAITranslationEngine.build_prompt_cache_key(
        template=template,
        model="gpt-5.4",
        source_language="zh",
        target_language="vi",
        route_mode="narration_fast",
        project_profile_id="zh-vi-narration-fast-vieneu",
    )

    assert first == second


def test_build_prompt_cache_key_changes_when_route_or_schema_changes() -> None:
    template = _template(schema_version=2)
    updated_template = _template(schema_version=3)

    base_key = OpenAITranslationEngine.build_prompt_cache_key(
        template=template,
        model="gpt-5.4",
        source_language="zh",
        target_language="vi",
        route_mode="narration_fast",
        project_profile_id="zh-vi-narration-fast-vieneu",
    )
    route_key = OpenAITranslationEngine.build_prompt_cache_key(
        template=template,
        model="gpt-5.4",
        source_language="zh",
        target_language="vi",
        route_mode="dialogue",
        project_profile_id="zh-vi-narration-fast-vieneu",
    )
    schema_key = OpenAITranslationEngine.build_prompt_cache_key(
        template=updated_template,
        model="gpt-5.4",
        source_language="zh",
        target_language="vi",
        route_mode="narration_fast",
        project_profile_id="zh-vi-narration-fast-vieneu",
    )

    assert route_key != base_key
    assert schema_key != base_key


def test_build_structured_user_prompt_keeps_stable_prefix_layout() -> None:
    engine = OpenAITranslationEngine(SimpleNamespace())
    template = _template()

    prompt = engine._build_structured_user_prompt(
        template=template,
        source_payload='{"segments":[1,2]}',
        source_language="zh",
        target_language="vi",
        glossary_payload='{"terms":["term-a"]}',
        constraints_payload='{"style":"clear"}',
        context_payload='{"scene":"demo"}',
    )

    constraints_index = prompt.index("## Constraints")
    context_index = prompt.index("## Context")
    glossary_index = prompt.index("## Glossary")
    source_index = prompt.index("## Source")

    assert constraints_index < context_index < glossary_index < source_index
    assert prompt.rstrip().endswith('{"segments":[1,2]}')
