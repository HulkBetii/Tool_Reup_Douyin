from __future__ import annotations

from pathlib import Path

from app.translate.models import TranslationPromptTemplate
from app.translate.presets import list_prompt_templates, save_prompt_template


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

