from __future__ import annotations

import json
from pathlib import Path

from .models import TranslationPromptTemplate


def get_prompt_presets_dir(project_root: Path) -> Path:
    return project_root / "presets" / "prompts"


def list_prompt_templates(project_root: Path) -> list[TranslationPromptTemplate]:
    presets_dir = get_prompt_presets_dir(project_root)
    if not presets_dir.exists():
        return []

    templates: list[TranslationPromptTemplate] = []
    for path in sorted(presets_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            templates.append(TranslationPromptTemplate.model_validate(payload))
        except Exception:
            continue
    return templates


def load_prompt_template(project_root: Path, template_id: str) -> TranslationPromptTemplate:
    for template in list_prompt_templates(project_root):
        if template.template_id == template_id:
            return template
    raise FileNotFoundError(f"Khong tim thay prompt template: {template_id}")


def save_prompt_template(project_root: Path, template: TranslationPromptTemplate) -> Path:
    presets_dir = get_prompt_presets_dir(project_root)
    presets_dir.mkdir(parents=True, exist_ok=True)
    path = presets_dir / f"{template.template_id}.json"
    path.write_text(
        json.dumps(template.model_dump(mode="json"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path

