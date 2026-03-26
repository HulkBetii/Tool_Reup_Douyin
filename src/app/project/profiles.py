from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field

from app.project.database import ProjectDatabase
from app.tts.presets import list_voice_presets, save_voice_preset


class ProjectProfile(BaseModel):
    project_profile_id: str
    name: str
    description: str = ""
    source_language: str | None = None
    target_language: str | None = None
    translation_mode: str | None = None
    recommended_prompt_template_id: str | None = None
    active_voice_preset_id: str | None = None
    active_export_preset_id: str | None = None
    active_watermark_profile_id: str | None = None
    recommended_original_volume: float | None = None
    recommended_voice_volume: float | None = None
    voice_preset_overrides: dict[str, dict[str, object]] = Field(default_factory=dict)
    style_preset_overrides: dict[str, dict[str, object]] = Field(default_factory=dict)
    notes: str = ""


class ProjectProfileState(BaseModel):
    project_profile_id: str
    name: str
    applied_at: str
    recommended_prompt_template_id: str | None = None
    active_voice_preset_id: str | None = None
    active_export_preset_id: str | None = None
    active_watermark_profile_id: str | None = None
    recommended_original_volume: float | None = None
    recommended_voice_volume: float | None = None
    subtitle_subtext_mode: str = "off"


VALID_SUBTITLE_SUBTEXT_MODES = {"off", "source_text"}


def get_project_profiles_dir(project_root: Path) -> Path:
    return project_root / "presets" / "project_profiles"


def get_project_profile_state_path(project_root: Path) -> Path:
    return project_root / ".ops" / "project_profile_state.json"


def _default_project_profiles() -> list[ProjectProfile]:
    return [
        ProjectProfile(
            project_profile_id="zh-vi-narration-clear-vieneu",
            name="Narration Clear VieNeu",
            description=(
                "Preset zh->vi cho video thuyet minh/kham pha: VieNeu cham nhe, "
                "chu 12, giam nen goc de de nghe."
            ),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            recommended_prompt_template_id="contextual_default_adaptation",
            active_voice_preset_id="vieneu-default-vi",
            active_export_preset_id="youtube-16x9",
            active_watermark_profile_id="watermark-none",
            recommended_original_volume=0.07,
            recommended_voice_volume=1.0,
            voice_preset_overrides={
                "vieneu-default-vi": {
                    "speed": 0.93,
                    "volume": 1.0,
                    "pitch": 0.0,
                    "sample_rate": 24000,
                    "language": "vi",
                    "notes": (
                        "VieNeu cham nhe cho video narration zh->vi, uu tien ro y "
                        "va giam cam giac doc voi."
                    ),
                }
            },
            style_preset_overrides={
                "default-ass": {
                    "FontSize": 12,
                    "Outline": 2,
                    "Shadow": 0,
                    "Alignment": 2,
                    "MarginV": 48,
                }
            },
            notes=(
                "Mau duoc rut ra tu cac video khoa hoc/kham pha/narration dai. "
                "Nen giu cau gon, trung tinh, de nghe va uu tien fit/slot an toan."
            ),
        ),
        ProjectProfile(
            project_profile_id="zh-vi-narration-fast-vieneu",
            name="Narration Fast VieNeu",
            description=(
                "Preset zh->vi cho video thuyet minh dai: giu giong/ASS giong profile narration "
                "clear, nhung uu tien fast path voi planner noi bo, batch lon hon va context nhe hon."
            ),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            recommended_prompt_template_id="contextual_narration_fast_adaptation",
            active_voice_preset_id="vieneu-default-vi",
            active_export_preset_id="youtube-16x9",
            active_watermark_profile_id="watermark-none",
            recommended_original_volume=0.07,
            recommended_voice_volume=1.0,
            voice_preset_overrides={
                "vieneu-default-vi": {
                    "speed": 0.93,
                    "volume": 1.0,
                    "pitch": 0.0,
                    "sample_rate": 24000,
                    "language": "vi",
                    "notes": (
                        "VieNeu cham nhe cho fast-path narration zh->vi, uu tien doc ro y "
                        "va khong qua voi."
                    ),
                }
            },
            style_preset_overrides={
                "default-ass": {
                    "FontSize": 12,
                    "Outline": 2,
                    "Shadow": 0,
                    "Alignment": 2,
                    "MarginV": 48,
                }
            },
            notes=(
                "Fast path dung cho video khoa hoc/kham pha/hoang da it doi thoai. "
                "Uu tien narration trung tinh, giam token/call va giam review gia do memory dialogue."
            ),
        ),
        ProjectProfile(
            project_profile_id="zh-vi-narration-fast-v2-vieneu",
            name="Narration Fast V2 VieNeu",
            description=(
                "Preset zh->vi cho video thuyet minh dai: span-based, canonical-only narration, "
                "subtext mac dinh tat, sparse escalation va budget governor."
            ),
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
            recommended_prompt_template_id="contextual_narration_slot_rewrite",
            active_voice_preset_id="vieneu-default-vi",
            active_export_preset_id="youtube-16x9",
            active_watermark_profile_id="watermark-none",
            recommended_original_volume=0.07,
            recommended_voice_volume=1.0,
            voice_preset_overrides={
                "vieneu-default-vi": {
                    "speed": 0.93,
                    "volume": 1.0,
                    "pitch": 0.0,
                    "sample_rate": 24000,
                    "language": "vi",
                    "notes": (
                        "VieNeu cham nhe cho narration fast v2 zh->vi, uu tien de nghe "
                        "va giam can goi adaptation LLM."
                    ),
                }
            },
            style_preset_overrides={
                "default-ass": {
                    "FontSize": 12,
                    "Outline": 2,
                    "Shadow": 0,
                    "Alignment": 2,
                    "MarginV": 48,
                }
            },
            notes=(
                "Narration Fast Path v2: span-based, canonical_text only, sparse escalation, "
                "term memory, budget governor <= 0.30 USD va subtext goc mac dinh tat."
            ),
        ),
    ]


def default_project_profiles() -> list[ProjectProfile]:
    return [profile.model_copy(deep=True) for profile in _default_project_profiles()]


def ensure_project_profiles(project_root: Path) -> list[Path]:
    profiles_dir = get_project_profiles_dir(project_root)
    profiles_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    existing_ids = {path.stem for path in profiles_dir.glob("*.json")}
    for profile in default_project_profiles():
        path = profiles_dir / f"{profile.project_profile_id}.json"
        if profile.project_profile_id in existing_ids and path.exists():
            continue
        path.write_text(
            json.dumps(profile.model_dump(mode="json"), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        written_paths.append(path)
    return written_paths


def list_project_profiles(project_root: Path) -> list[ProjectProfile]:
    profiles_dir = get_project_profiles_dir(project_root)
    if not profiles_dir.exists():
        return []
    profiles: list[ProjectProfile] = []
    for path in sorted(profiles_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            profiles.append(ProjectProfile.model_validate(payload))
        except Exception:
            continue
    return profiles


def load_project_profile(project_root: Path, project_profile_id: str) -> ProjectProfile:
    for profile in list_project_profiles(project_root):
        if profile.project_profile_id == project_profile_id:
            return profile
    raise FileNotFoundError(f"Khong tim thay project profile: {project_profile_id}")


def load_project_profile_state(project_root: Path) -> ProjectProfileState | None:
    state_path = get_project_profile_state_path(project_root)
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        payload["subtitle_subtext_mode"] = normalize_subtitle_subtext_mode(
            payload.get("subtitle_subtext_mode")
        )
        return ProjectProfileState.model_validate(payload)
    except Exception:
        return None


def normalize_subtitle_subtext_mode(value: object) -> str:
    raw_value = str(value or "off").strip().lower()
    if raw_value not in VALID_SUBTITLE_SUBTEXT_MODES:
        return "off"
    return raw_value


def save_project_profile_state(project_root: Path, state: ProjectProfileState) -> ProjectProfileState:
    normalized_state = state.model_copy(
        update={"subtitle_subtext_mode": normalize_subtitle_subtext_mode(state.subtitle_subtext_mode)}
    )
    state_path = get_project_profile_state_path(project_root)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(normalized_state.model_dump(mode="json"), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return normalized_state


def ensure_project_profile_state(
    project_root: Path,
    *,
    project_profile_id: str | None = None,
    name: str | None = None,
    applied_at: str | None = None,
) -> ProjectProfileState:
    existing_state = load_project_profile_state(project_root)
    if existing_state is not None:
        return save_project_profile_state(project_root, existing_state)
    resolved_profile_id = project_profile_id or "manual"
    resolved_name = name or resolved_profile_id
    state = ProjectProfileState(
        project_profile_id=resolved_profile_id,
        name=resolved_name,
        applied_at=applied_at or "",
        subtitle_subtext_mode="off",
    )
    return save_project_profile_state(project_root, state)


def set_project_subtitle_subtext_mode(project_root: Path, mode: str, *, applied_at: str = "") -> ProjectProfileState:
    state = ensure_project_profile_state(project_root, applied_at=applied_at)
    updated_state = state.model_copy(
        update={
            "subtitle_subtext_mode": normalize_subtitle_subtext_mode(mode),
            "applied_at": applied_at or state.applied_at,
        }
    )
    return save_project_profile_state(project_root, updated_state)


def resolve_subtitle_subtext_mode(project_root: Path) -> str:
    state = load_project_profile_state(project_root)
    if state is None:
        return "off"
    return normalize_subtitle_subtext_mode(state.subtitle_subtext_mode)


def resolve_project_profile_mix_defaults(
    project_root: Path,
    *,
    original_volume: float | None,
    voice_volume: float | None,
) -> tuple[float, float, ProjectProfileState | None]:
    state = load_project_profile_state(project_root)
    resolved_original_volume = original_volume
    resolved_voice_volume = voice_volume
    if state is not None:
        if resolved_original_volume is None:
            resolved_original_volume = state.recommended_original_volume
        if resolved_voice_volume is None:
            resolved_voice_volume = state.recommended_voice_volume
    if resolved_original_volume is None:
        resolved_original_volume = 0.35
    if resolved_voice_volume is None:
        resolved_voice_volume = 1.0
    return resolved_original_volume, resolved_voice_volume, state


def _find_style_preset_path(project_root: Path, style_preset_id: str) -> Path | None:
    styles_dir = project_root / "presets" / "styles"
    if not styles_dir.exists():
        return None
    for path in sorted(styles_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("style_preset_id") == style_preset_id:
            return path
    return None


def _apply_voice_preset_overrides(project_root: Path, profile: ProjectProfile) -> None:
    presets_by_id = {preset.voice_preset_id: preset for preset in list_voice_presets(project_root)}
    for preset_id, overrides in profile.voice_preset_overrides.items():
        base_preset = presets_by_id.get(preset_id)
        if base_preset is None:
            raise FileNotFoundError(f"Khong tim thay voice preset de apply profile: {preset_id}")
        update_payload = dict(overrides)
        engine_options_override = update_payload.pop("engine_options", None)
        if isinstance(engine_options_override, dict):
            update_payload["engine_options"] = {
                **dict(base_preset.engine_options),
                **engine_options_override,
            }
        save_voice_preset(project_root, base_preset.model_copy(update=update_payload))


def _apply_style_preset_overrides(project_root: Path, profile: ProjectProfile) -> None:
    for style_preset_id, overrides in profile.style_preset_overrides.items():
        path = _find_style_preset_path(project_root, style_preset_id)
        if path is None:
            raise FileNotFoundError(f"Khong tim thay style preset de apply profile: {style_preset_id}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        ass_style = dict(payload.get("ass_style_json") or {})
        ass_style.update(overrides)
        payload["ass_style_json"] = ass_style
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def apply_project_profile(
    project_root: Path,
    *,
    project_id: str,
    database: ProjectDatabase,
    project_profile_id: str,
    applied_at: str,
) -> ProjectProfileState:
    profile = load_project_profile(project_root, project_profile_id)
    existing_state = load_project_profile_state(project_root)
    _apply_voice_preset_overrides(project_root, profile)
    _apply_style_preset_overrides(project_root, profile)

    if profile.translation_mode:
        database.set_translation_mode(project_id, profile.translation_mode, updated_at=applied_at)
    if profile.active_voice_preset_id is not None:
        database.set_active_voice_preset_id(
            project_id,
            profile.active_voice_preset_id,
            updated_at=applied_at,
        )
    if profile.active_export_preset_id is not None:
        database.set_active_export_preset_id(
            project_id,
            profile.active_export_preset_id,
            updated_at=applied_at,
        )
    if profile.active_watermark_profile_id is not None:
        database.set_active_watermark_profile_id(
            project_id,
            profile.active_watermark_profile_id,
            updated_at=applied_at,
        )

    state = ProjectProfileState(
        project_profile_id=profile.project_profile_id,
        name=profile.name,
        applied_at=applied_at,
        recommended_prompt_template_id=profile.recommended_prompt_template_id,
        active_voice_preset_id=profile.active_voice_preset_id,
        active_export_preset_id=profile.active_export_preset_id,
        active_watermark_profile_id=profile.active_watermark_profile_id,
        recommended_original_volume=profile.recommended_original_volume,
        recommended_voice_volume=profile.recommended_voice_volume,
        subtitle_subtext_mode=(
            existing_state.subtitle_subtext_mode
            if existing_state is not None
            else "off"
        ),
    )
    return save_project_profile_state(project_root, state)
