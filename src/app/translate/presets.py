from __future__ import annotations

import json
from pathlib import Path

from .models import TranslationPromptTemplate


def get_prompt_presets_dir(project_root: Path) -> Path:
    return project_root / "presets" / "prompts"


def default_translation_mode_for_languages(source_language: str, target_language: str) -> str:
    if source_language.lower() == "zh" and target_language.lower() == "vi":
        return "contextual_v2"
    return "legacy"


def _contextual_templates() -> list[TranslationPromptTemplate]:
    return [
        TranslationPromptTemplate(
            template_id="contextual_default_scene_planner",
            family_id="contextual-default-vi",
            translation_mode="contextual_v2",
            role="scene_planner",
            name="Contextual mặc định / Planner",
            category="contextual",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "You plan Chinese-to-Vietnamese dialogue translation at the scene level. "
                "Summarize the scene, identify likely participants, track ambiguities, and propose "
                "character or directional relationship updates only when there is evidence. "
                "Use hypothesized or unknown values instead of inventing certainty."
            ),
            user_prompt_template=(
                "Plan the current scene for contextual dialogue translation from {source_language} to "
                "{target_language}. Use Context, Glossary, and Constraints to keep discourse memory stable. "
                "Ground every update in the provided scene payload and keep ambiguous points inside the "
                "scene output rather than resolving them aggressively. "
                "Context: {context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Contextual V2 planner tuned to keep hypotheses explicit.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_default_semantic",
            family_id="contextual-default-vi",
            translation_mode="contextual_v2",
            role="semantic_pass",
            name="Contextual mặc định / Semantic",
            category="contextual",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Analyze Chinese-to-Vietnamese dialogue semantics. Return exactly one semantic item for "
                "every segment in scene.segments. Preserve each segment_id verbatim. Never merge, omit, "
                "duplicate, or reorder segments. Focus on who is speaking to whom, honorific policy, "
                "ellipsis resolution, and safe in-world meaning. If anything is ambiguous, keep the "
                "semantic_translation conservative and move the uncertainty into review fields. If an "
                "uncertain noun repeats within a scene, keep one stable short neutral Vietnamese noun "
                "phrase, such as 'mon do' or 'thu do' when the referent is unclear. Never emit technical "
                "placeholder tokens or control words in user-facing text."
            ),
            user_prompt_template=(
                "Analyze the semantics of the current batch from {source_language} to {target_language}. "
                "The payload may be only part of a larger scene, but you must still return exactly one "
                "item per segment inside scene.segments. Use Context, Glossary, and Constraints to keep "
                "speaker, listener, and relationship decisions stable. If the same unresolved noun "
                "repeats across nearby lines, keep one stable short neutral Vietnamese noun phrase across "
                "those lines and never emit literal placeholder text. "
                "Context: {context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Contextual V2 semantic pass tuned on real zh->vi dialogue data.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_default_adaptation",
            family_id="contextual-default-vi",
            translation_mode="contextual_v2",
            role="dialogue_adaptation",
            name="Contextual mặc định",
            category="contextual",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Adapt approved semantic dialogue into subtitle_text and tts_text. Return exactly one "
                "item for every segment in scene.segments and preserve each segment_id verbatim. "
                "subtitle_text must stay concise and readable. tts_text may sound more oral, but it must "
                "preserve the same honorific policy, speaker-listener relationship, and narrative intent. "
                "Do not output translator notes or explanations to the audience. If a key noun remains "
                "uncertain, keep one stable short neutral Vietnamese noun phrase across the scene and "
                "never print technical placeholder text."
            ),
            user_prompt_template=(
                "Write subtitle_text and tts_text for the current contextual batch from {source_language} "
                "to {target_language}. Return exactly one item per segment inside scene.segments. Use "
                "Context, Glossary, and Constraints to preserve the approved honorific policy. Keep all "
                "user-facing text as in-world dialogue only. If a term remains ambiguous, choose the "
                "safest natural line and move the uncertainty into review fields instead of explaining it "
                "inside subtitle_text or tts_text. Keep recurring unresolved nouns consistent with a "
                "short neutral Vietnamese phrase and never output placeholder tokens. Context: {context}. Glossary: {glossary}. "
                "Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            default_constraints_json={"max_lines": 2, "max_cpl": 42, "target_cps": 18},
            notes="Contextual V2 adaptation tuned to reduce honorific drift and viewer-facing notes.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_default_critic",
            family_id="contextual-default-vi",
            translation_mode="contextual_v2",
            role="semantic_critic",
            name="Contextual mặc định / Critic",
            category="contextual",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Review contextual dialogue outputs for semantic consistency. Return exactly one critic "
                "item for every segment in scene.segments and preserve each segment_id verbatim. Catch "
                "honorific drift, wrong addressee, relationship drift, unjustified pronoun insertion, "
                "and divergence between subtitle_text and tts_text. Treat literal placeholder tokens or "
                "control words in user-facing text as an issue."
            ),
            user_prompt_template=(
                "Review the current contextual batch from {source_language} to {target_language}. Return "
                "exactly one critic item per segment inside scene.segments. Use Context, Glossary, and "
                "Constraints to check discourse consistency instead of re-translating from scratch. "
                "Context: {context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Contextual V2 critic tuned to keep one result per segment.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_fast_scene_planner",
            family_id="contextual-narration-fast-vi",
            translation_mode="contextual_v2",
            role="scene_planner",
            name="Narration Fast / Planner",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "You plan Chinese-to-Vietnamese narration translation for non-dialogue videos. "
                "Assume a neutral narrator unless the source clearly switches speaker. Avoid "
                "character and relationship updates unless there is explicit evidence."
            ),
            user_prompt_template=(
                "Plan the current narration scene from {source_language} to {target_language}. "
                "Keep the plan lightweight, prefer neutral narration, and record ambiguity instead "
                "of forcing dialogue assumptions. Context: {context}. Glossary: {glossary}. "
                "Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Narration fast path uses a local planner by default; this template stays as a fallback contract.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_fast_semantic",
            family_id="contextual-narration-fast-vi",
            translation_mode="contextual_v2",
            role="semantic_pass",
            name="Narration Fast / Semantic",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Analyze Chinese-to-Vietnamese narration semantics. Return exactly one semantic item for "
                "every segment in scene.segments, in the exact same order as scene.segments. Do not emit "
                "segment_id fields, do not merge, omit, duplicate, or reorder items. Treat the speaker "
                "as a neutral narrator by default, keep honorific_policy empty unless the line explicitly "
                "addresses an audience, and prefer conservative narration over dialogue-like guessing. If "
                "a technical term or referent is unclear, keep the translation neutral and move the "
                "uncertainty into review fields."
            ),
            user_prompt_template=(
                "Analyze the semantics of the current narration batch from {source_language} to "
                "{target_language}. Return exactly one item per segment inside scene.segments and keep "
                "the output order identical to scene.segments. Use the lightweight Context and "
                "Constraints to keep tone neutral and informative. Do not invent character relationships "
                "or honorific policy for plain narration. Context: {context}. Glossary: {glossary}. "
                "Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Narration fast semantic pass biases toward neutral voice-over and review routing for unclear terms.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_fast_term_entity",
            family_id="contextual-narration-fast-vi",
            translation_mode="contextual_v2",
            role="term_entity_pass",
            name="Narration Fast / Terms",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Review a Chinese-to-Vietnamese narration scene and extract only the recurring or "
                "meaning-critical technical terms and named entities that should stay stable across "
                "the scene. Return at most 6 items. Use zero-based segment_positions that point to the "
                "matching entries inside scene.segments. Use status='prefer' when a stable Vietnamese "
                "rendering is safe enough to reuse. Use status='needs_review' only when the term or "
                "entity is important and still too uncertain to recommend confidently. If nothing needs "
                "special handling, return an empty items list."
            ),
            user_prompt_template=(
                "Build a lightweight term/entity sheet for the current narration scene from "
                "{source_language} to {target_language}. Focus on recurring technical terms, scientific "
                "labels, proper names, and other meaning-critical nouns that could cause repeated drift "
                "later. Keep the sheet short and practical. Use Context, Glossary, and Constraints for "
                "existing stable choices, but do not invent certainty. Context: {context}. Glossary: "
                "{glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=1,
            default_constraints_json={
                "max_items": 6,
                "allow_empty": True,
                "zero_based_segment_positions": True,
            },
            notes="Narration term/entity mini-pass builds a reusable term sheet before semantic/adaptation.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_fast_adaptation",
            family_id="contextual-narration-fast-vi",
            translation_mode="contextual_v2",
            role="dialogue_adaptation",
            name="Narration Fast",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Adapt approved narration semantics into subtitle_text and tts_text. Return exactly one "
                "item for every segment in scene.segments, in the exact same order as scene.segments. Do "
                "not emit segment_id fields, do not merge, omit, duplicate, or reorder items. "
                "subtitle_text must stay concise, factual, and easy to read. tts_text should usually stay "
                "very close to subtitle_text; only make small oral adjustments when they do not add new "
                "pronouns, audience address, or semantic detail. Never add dialogue-style flourishes."
            ),
            user_prompt_template=(
                "Write subtitle_text and tts_text for the current narration batch from {source_language} "
                "to {target_language}. Return exactly one item per segment inside scene.segments and keep "
                "the output order identical to scene.segments. Keep the Vietnamese natural, concise, and "
                "easy to narrate. Prefer `tts_text = subtitle_text` unless a tiny oral smoothing change "
                "is clearly safe. Do not add audience-address terms "
                "such as 'quý vị' or 'các bạn' unless they are explicit in the approved semantics. "
                "Context: {context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            default_constraints_json={"max_lines": 2, "max_cpl": 38, "target_cps": 16},
            notes="Narration fast adaptation keeps subtitle/TTS close to reduce review churn and reruns.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_semantic_v2",
            family_id="contextual-narration-fast-v2-vi",
            translation_mode="contextual_v2",
            role="semantic_pass",
            name="Narration Fast V2 / Semantic",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Translate Chinese narration into one stable Vietnamese canonical_text per segment. "
                "Return exactly one item for every render unit in span.render_units, in the same order. "
                "Do not emit segment_id fields, do not merge, omit, duplicate, or reorder items. "
                "canonical_text must stay factual, concise, and neutral. subtitle_text and tts_text will "
                "be derived locally from canonical_text, so do not create dialogue flourishes. Use risk_flags "
                "only for concrete risk classes such as entity_new, number_sensitive, pronoun_ambiguous, "
                "idiom_ambiguous, unsafe_to_guess, or needs_shortening."
            ),
            user_prompt_template=(
                "Produce one Vietnamese canonical_text per narration render unit from {source_language} to "
                "{target_language}. Return exactly one item per render unit in span.render_units and keep "
                "the order identical. Keep output compact and semantic-first. Use approved glossary entries "
                "when present. If a fact, entity, or referent is not safe to guess, set flags instead of "
                "inventing certainty. Context: {context}. Glossary: {glossary}. Constraints: {constraints}. "
                "Data: {source}"
            ),
            output_schema_version=1,
            default_constraints_json={
                "canonical_only": True,
                "max_lines": 2,
                "max_cpl": 38,
                "target_cps": 16,
            },
            notes="Narration fast v2 base semantic pass returns canonical_text only.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_entity_micro",
            family_id="contextual-narration-fast-v2-vi",
            translation_mode="contextual_v2",
            role="entity_micro_pass",
            name="Narration Fast V2 / Entity",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Resolve only the entity or technical-term spans provided. Return short approved_target "
                "choices when safe, otherwise keep status conservative. Never rewrite the whole narration."
            ),
            user_prompt_template=(
                "Resolve entity or technical-term spans from {source_language} to {target_language}. "
                "Only work on the listed candidate terms and their short local context. Prefer approved "
                "memory if it fits. If a term is still not safe to finalize, keep it conservative and mark "
                "the status accordingly. Context: {context}. Glossary: {glossary}. Constraints: {constraints}. "
                "Data: {source}"
            ),
            output_schema_version=1,
            default_constraints_json={"micro_pass": "entity", "max_items": 6},
            notes="Sparse entity resolution for narration fast v2.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_ambiguity_micro",
            family_id="contextual-narration-fast-v2-vi",
            translation_mode="contextual_v2",
            role="ambiguity_micro_pass",
            name="Narration Fast V2 / Ambiguity",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Resolve only the listed ambiguous narration spans. Return one item per input span and keep "
                "the output conservative. If the ambiguity is still unsafe, keep unsafe_to_guess=true."
            ),
            user_prompt_template=(
                "Resolve the listed ambiguous narration spans from {source_language} to {target_language}. "
                "Only decide the minimum needed wording for the listed spans. Do not rewrite unrelated text. "
                "If context is still insufficient, keep unsafe_to_guess true. Context: {context}. Glossary: "
                "{glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=1,
            default_constraints_json={"micro_pass": "ambiguity"},
            notes="Sparse ambiguity resolution for narration fast v2.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_narration_slot_rewrite",
            family_id="contextual-narration-fast-v2-vi",
            translation_mode="contextual_v2",
            role="dialogue_adaptation",
            name="Narration Fast V2",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Rewrite narration canonical_text only when slot pressure requires shortening. Return exactly "
                "one item per input item, in the same order. Keep facts, entities, and numbers unchanged. "
                "Do not add audience address or dialogue flourishes."
            ),
            user_prompt_template=(
                "Rewrite only the listed canonical_text items from {source_language} to {target_language} "
                "to fit subtitle/TTS slot limits without changing facts. Protected entities and numbers must "
                "stay unchanged. Return one item per input item in the same order. Context: {context}. "
                "Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=1,
            default_constraints_json={"micro_pass": "slot_rewrite", "max_lines": 2, "max_cpl": 38, "target_cps": 16},
            notes="Narration fast v2 slot rewrite micro-pass; also acts as the UI-selectable family anchor.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_cartoon_fun_scene_planner",
            family_id="contextual-cartoon-fun-vi",
            translation_mode="contextual_v2",
            role="scene_planner",
            name="Hoạt hình hài / Planner",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "You plan translation for humorous animation dialogue from Chinese to Vietnamese. "
                "Identify the comedic setup, likely participants, relationship hints, and any ambiguity "
                "that may affect pronouns, honorifics, or punchlines. Prefer explicit uncertainty over "
                "confident guessing."
            ),
            user_prompt_template=(
                "Plan the current humorous-animation scene from {source_language} to {target_language}. "
                "Use Context, Glossary, and Constraints to preserve character relationships and comic "
                "timing. If a key joke term stays ambiguous, record it in the scene output instead of "
                "forcing a single interpretation. Context: {context}. Glossary: {glossary}. "
                "Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Tuned on real Shinchan data to keep ambiguities visible for review.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_cartoon_fun_semantic",
            family_id="contextual-cartoon-fun-vi",
            translation_mode="contextual_v2",
            role="semantic_pass",
            name="Hoạt hình hài / Semantic",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Analyze humorous animation dialogue from Chinese to Vietnamese. Return exactly one "
                "semantic item for every segment in scene.segments. Preserve each segment_id verbatim. "
                "Never merge, omit, duplicate, or reorder segments. Keep the joke intent, but if "
                "speaker, listener, honorifics, or a key term are ambiguous, use unknown values and "
                "needs_human_review=true instead of guessing. semantic_translation must stay as in-world "
                "dialogue, not translator notes or explanations for the audience. If an uncertain noun or "
                "joke term repeats within a scene, keep one stable short neutral Vietnamese noun phrase, "
                "such as 'mon do' or 'thu do' when needed, and never emit technical placeholder text."
            ),
            user_prompt_template=(
                "Analyze the semantics of the current humorous-animation batch from {source_language} to "
                "{target_language}. The payload may be only part of a larger scene, but you must still "
                "return exactly one item per segment inside scene.segments. Use Context, Glossary, and "
                "Constraints to keep relationships stable. If any term is ambiguous, keep the "
                "semantic_translation as the safest in-world utterance and move the uncertainty into "
                "review fields instead of explaining it in the dialogue text. If the same unresolved term "
                "repeats, keep one stable short neutral Vietnamese noun phrase and never print placeholder "
                "tokens. Context: {context}. "
                "Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Tuned on real Shinchan sample to reduce missing segment ids and overconfident guesses.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_cartoon_fun_adaptation",
            family_id="contextual-cartoon-fun-vi",
            translation_mode="contextual_v2",
            role="dialogue_adaptation",
            name="Hoạt hình hài hước",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Adapt humorous animation dialogue into subtitle_text and tts_text. Return exactly one "
                "item for every segment in scene.segments and preserve every segment_id verbatim. Do not "
                "merge, omit, duplicate, or reorder segments. subtitle_text should be punchy and "
                "readable. tts_text can sound more playful and oral, but it must keep the same "
                "speaker-listener relationship and honorific policy. Never output translator notes, "
                "explanations, or ambiguity glosses inside subtitle_text or tts_text. If a term is "
                "ambiguous, choose the safest in-world line and send the uncertainty to review instead. "
                "Keep repeated unresolved nouns consistent with a short neutral Vietnamese phrase and "
                "never emit technical placeholder text."
            ),
            user_prompt_template=(
                "Write subtitle_text and tts_text for the current humorous-animation batch from "
                "{source_language} to {target_language}. Return exactly one item per segment inside "
                "scene.segments. Use Context, Glossary, and Constraints to preserve the approved "
                "honorific policy while keeping the dialogue lively. Keep all user-facing text as "
                "in-world dialogue only; do not explain ambiguity to the audience. If something stays "
                "uncertain, keep the line concise and move the doubt into review fields. Keep repeated "
                "unresolved nouns consistent with a short neutral Vietnamese phrase and never output "
                "placeholder tokens. Context: "
                "{context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            default_constraints_json={"max_lines": 2, "max_cpl": 40, "target_cps": 18},
            notes="Tuned on real Shinchan sample to keep one output per segment and avoid meta dialogue.",
        ),
        TranslationPromptTemplate(
            template_id="contextual_cartoon_fun_critic",
            family_id="contextual-cartoon-fun-vi",
            translation_mode="contextual_v2",
            role="semantic_critic",
            name="Hoạt hình hài / Critic",
            category="style",
            source_lang="zh",
            target_lang="vi",
            system_prompt=(
                "Review humorous animation dialogue for semantic consistency. Return exactly one critic "
                "item for every segment in scene.segments. Preserve every segment_id verbatim. Catch "
                "honorific drift, wrong addressee, relationship drift, and cases where the rewrite "
                "changed the social tone or comedic intent. Treat literal placeholder tokens or control "
                "words in user-facing dialogue as issues."
            ),
            user_prompt_template=(
                "Review the current humorous-animation batch from {source_language} to {target_language}. "
                "Return exactly one critic item per segment inside scene.segments. Use Context, Glossary, "
                "and Constraints to verify discourse consistency without inventing missing facts. "
                "Context: {context}. Glossary: {glossary}. Constraints: {constraints}. Data: {source}"
            ),
            output_schema_version=2,
            notes="Tuned on real Shinchan sample to avoid partial critic outputs.",
        ),
    ]


def _default_prompt_templates(source_language: str, target_language: str) -> list[TranslationPromptTemplate]:
    templates = [
        TranslationPromptTemplate(
            template_id="default-vi-style",
            family_id="legacy-default-vi",
            translation_mode="legacy",
            role="legacy_translate",
            name="Dịch tự nhiên",
            category="mặc định",
            source_lang=source_language or "auto",
            target_lang=target_language or "vi",
            system_prompt="You are a subtitle editor. Keep the meaning accurate, concise, and easy to read.",
            user_prompt_template="Translate the following content to {target_language}: {source}",
            output_schema_version=1,
            default_constraints_json={"max_lines": 2, "max_cpl": 42, "target_cps": 18},
        )
    ]
    if default_translation_mode_for_languages(source_language, target_language) != "contextual_v2":
        return templates
    templates.extend(_contextual_templates())
    return templates


def ensure_prompt_templates(project_root: Path, source_language: str, target_language: str) -> list[Path]:
    presets_dir = get_prompt_presets_dir(project_root)
    presets_dir.mkdir(parents=True, exist_ok=True)
    existing_ids = {path.stem for path in presets_dir.glob("*.json")}
    written_paths: list[Path] = []
    for template in _default_prompt_templates(source_language, target_language):
        path = presets_dir / f"{template.template_id}.json"
        if template.template_id in existing_ids and path.exists():
            continue
        path.write_text(
            json.dumps(template.model_dump(mode="json"), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        written_paths.append(path)
    return written_paths


def list_prompt_templates(
    project_root: Path,
    *,
    translation_mode: str | None = None,
    role: str | None = None,
) -> list[TranslationPromptTemplate]:
    presets_dir = get_prompt_presets_dir(project_root)
    if not presets_dir.exists():
        return []

    templates: list[TranslationPromptTemplate] = []
    for path in sorted(presets_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            template = TranslationPromptTemplate.model_validate(payload)
            if translation_mode and template.translation_mode != translation_mode:
                continue
            if role and template.role != role:
                continue
            templates.append(template)
        except Exception:
            continue
    return templates


def load_prompt_template(project_root: Path, template_id: str) -> TranslationPromptTemplate:
    for template in list_prompt_templates(project_root):
        if template.template_id == template_id:
            return template
    raise FileNotFoundError(f"Khong tim thay prompt template: {template_id}")


def resolve_prompt_family(
    project_root: Path,
    selected_template: TranslationPromptTemplate,
) -> dict[str, TranslationPromptTemplate]:
    family_id = selected_template.family_id or selected_template.template_id
    templates = list_prompt_templates(
        project_root,
        translation_mode=selected_template.translation_mode,
    )
    family = {
        template.role: template
        for template in templates
        if (template.family_id or template.template_id) == family_id
    }
    if selected_template.role not in family:
        family[selected_template.role] = selected_template
    return family


def is_narration_fast_template(selected_template: TranslationPromptTemplate) -> bool:
    family_id = selected_template.family_id or selected_template.template_id
    return family_id in {"contextual-narration-fast-vi", "contextual-narration-fast-v2-vi"}


def is_narration_fast_v2_template(selected_template: TranslationPromptTemplate) -> bool:
    family_id = selected_template.family_id or selected_template.template_id
    return family_id == "contextual-narration-fast-v2-vi"


def save_prompt_template(project_root: Path, template: TranslationPromptTemplate) -> Path:
    presets_dir = get_prompt_presets_dir(project_root)
    presets_dir.mkdir(parents=True, exist_ok=True)
    path = presets_dir / f"{template.template_id}.json"
    path.write_text(
        json.dumps(template.model_dump(mode="json"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path
