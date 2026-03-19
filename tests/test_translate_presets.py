from __future__ import annotations

from pathlib import Path

from app.translate.contextual_pipeline import build_contextual_translation_stage_hash
from app.translate.models import TranslationPromptTemplate
from app.translate.presets import (
    ensure_prompt_templates,
    list_prompt_templates,
    load_prompt_template,
    save_prompt_template,
)


def test_prompt_template_roundtrip(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    template = TranslationPromptTemplate(
        template_id="batch-vi",
        name="Batch VI",
        system_prompt="Translate subtitle segments.",
        user_prompt_template="Target {target_language}: {source}",
    )

    save_prompt_template(project_root, template)
    loaded = list_prompt_templates(project_root)

    assert len(loaded) == 1
    assert loaded[0].template_id == "batch-vi"
    assert loaded[0].name == "Batch VI"


def test_ensure_prompt_templates_writes_contextual_family_for_zh_to_vi(tmp_path: Path) -> None:
    project_root = tmp_path / "project"

    ensure_prompt_templates(project_root, "zh", "vi")
    loaded = list_prompt_templates(
        project_root,
        translation_mode="contextual_v2",
        role="dialogue_adaptation",
    )

    template_ids = {item.template_id for item in loaded}
    assert "contextual_default_adaptation" in template_ids
    assert "contextual_cartoon_fun_adaptation" in template_ids


def test_contextual_stage_hash_changes_when_family_prompt_changes(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    ensure_prompt_templates(project_root, "zh", "vi")
    selected_template = load_prompt_template(project_root, "contextual_default_adaptation")
    segments = [
        {
            "segment_id": "seg-001",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 1200,
            "source_text": "你好",
        }
    ]

    first_hash = build_contextual_translation_stage_hash(
        segments=segments,
        template=selected_template,
        project_root=project_root,
        model="gpt-4.1-mini",
        source_language="zh",
        target_language="vi",
    )

    semantic_template = load_prompt_template(project_root, "contextual_default_semantic")
    save_prompt_template(
        project_root,
        semantic_template.model_copy(
            update={
                "system_prompt": semantic_template.system_prompt + " Force a different semantic policy."
            }
        ),
    )
    updated_selected_template = load_prompt_template(project_root, "contextual_default_adaptation")

    second_hash = build_contextual_translation_stage_hash(
        segments=segments,
        template=updated_selected_template,
        project_root=project_root,
        model="gpt-4.1-mini",
        source_language="zh",
        target_language="vi",
    )

    assert first_hash != second_hash
