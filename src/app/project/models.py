from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass(slots=True)
class ProjectInitRequest:
    name: str
    root_dir: Path
    source_language: str = "auto"
    target_language: str = "vi"
    source_video_path: Path | None = None
    translation_mode: str | None = None
    project_profile_id: str | None = None


@dataclass(slots=True)
class ProjectRecord:
    project_id: str
    name: str
    root_dir: str
    source_language: str
    target_language: str
    created_at: str
    updated_at: str
    translation_mode: str = "legacy"
    video_asset_id: str | None = None
    active_subtitle_track_id: str | None = None
    active_voice_preset_id: str | None = None
    active_export_preset_id: str | None = None
    active_watermark_profile_id: str | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class MediaAssetRecord:
    asset_id: str
    project_id: str
    asset_type: str
    path: str
    created_at: str
    sha256: str | None = None
    duration_ms: int | None = None
    fps: float | None = None
    width: int | None = None
    height: int | None = None
    audio_channels: int | None = None
    sample_rate: int | None = None


@dataclass(slots=True)
class JobRunRecord:
    job_id: str
    project_id: str | None
    stage: str
    description: str
    status: str
    started_at: str
    progress: int = 0
    input_hash: str = ""
    output_paths: list[str] = field(default_factory=list)
    log_path: str | None = None
    error_json: dict[str, object] = field(default_factory=dict)
    ended_at: str | None = None
    retry_of_job_id: str | None = None
    message: str = ""


@dataclass(slots=True)
class SegmentRecord:
    segment_id: str
    project_id: str
    segment_index: int
    start_ms: int
    end_ms: int
    source_lang: str | None = None
    target_lang: str | None = None
    source_text: str = ""
    source_text_norm: str = ""
    translated_text: str = ""
    translated_text_norm: str = ""
    subtitle_text: str = ""
    tts_text: str = ""
    audio_path: str | None = None
    status: str = "draft"
    meta_json: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class SubtitleTrackRecord:
    track_id: str
    project_id: str
    name: str
    kind: str
    created_at: str
    updated_at: str
    notes: str = ""


@dataclass(slots=True)
class SubtitleEventRecord:
    event_id: str
    track_id: str
    project_id: str
    event_index: int
    start_ms: int
    end_ms: int
    source_segment_id: str | None = None
    source_lang: str | None = None
    target_lang: str | None = None
    source_text: str = ""
    source_text_norm: str = ""
    translated_text: str = ""
    translated_text_norm: str = ""
    subtitle_text: str = ""
    tts_text: str = ""
    audio_path: str | None = None
    status: str = "draft"
    meta_json: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class CharacterProfileRecord:
    character_id: str
    project_id: str
    canonical_name_zh: str = ""
    canonical_name_vi: str = ""
    aliases_json: list[str] = field(default_factory=list)
    gender_hint: str | None = None
    age_role: str | None = None
    social_role: str | None = None
    speech_style: str | None = None
    default_register_profile_json: dict[str, object] = field(default_factory=dict)
    default_self_terms_json: list[str] = field(default_factory=list)
    default_address_terms_json: list[str] = field(default_factory=list)
    forbidden_terms_json: list[str] = field(default_factory=list)
    evidence_segment_ids_json: list[str] = field(default_factory=list)
    confidence: float = 0.0
    status: str = "hypothesized"
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class RelationshipProfileRecord:
    relationship_id: str
    project_id: str
    from_character_id: str
    to_character_id: str
    relation_type: str = "unknown"
    power_delta: str | None = None
    age_delta: str | None = None
    intimacy_level: str | None = None
    default_self_term: str | None = None
    default_address_term: str | None = None
    allowed_alternates_json: list[str] | dict[str, list[str]] = field(default_factory=list)
    scope: str = "scene"
    status: str = "hypothesized"
    evidence_segment_ids_json: list[str] = field(default_factory=list)
    last_updated_scene_id: str | None = None
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class SpeakerBindingRecord:
    binding_id: str
    project_id: str
    speaker_type: str
    speaker_key: str
    voice_preset_id: str
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class VoicePolicyRecord:
    policy_id: str
    project_id: str
    policy_scope: str
    speaker_character_id: str
    listener_character_id: str | None = None
    voice_preset_id: str = ""
    speed_override: float | None = None
    volume_override: float | None = None
    pitch_override: float | None = None
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class RegisterVoiceStylePolicyRecord:
    policy_id: str
    project_id: str
    politeness: str | None = None
    power_direction: str | None = None
    emotional_tone: str | None = None
    turn_function: str | None = None
    relation_type: str | None = None
    speed_override: float | None = None
    volume_override: float | None = None
    pitch_override: float | None = None
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class SceneMemoryRecord:
    scene_id: str
    project_id: str
    scene_index: int
    start_segment_index: int
    end_segment_index: int
    start_ms: int
    end_ms: int
    participants_json: list[str] = field(default_factory=list)
    location: str | None = None
    time_context: str | None = None
    short_scene_summary: str = ""
    recent_turn_digest: str = ""
    active_topic: str | None = None
    current_conflict: str | None = None
    current_emotional_tone: str | None = None
    temporary_addressing_mode: str | None = None
    who_knows_what_json: list[dict[str, object]] = field(default_factory=list)
    open_ambiguities_json: list[str] = field(default_factory=list)
    unresolved_references_json: list[str] = field(default_factory=list)
    status: str = "planned"
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class SegmentAnalysisRecord:
    segment_id: str
    project_id: str
    scene_id: str
    segment_index: int
    speaker_json: dict[str, object] = field(default_factory=dict)
    listeners_json: list[dict[str, object]] = field(default_factory=list)
    register_json: dict[str, object] = field(default_factory=dict)
    turn_function: str | None = None
    resolved_ellipsis_json: dict[str, object] = field(default_factory=dict)
    honorific_policy_json: dict[str, object] = field(default_factory=dict)
    semantic_translation: str = ""
    glossary_hits_json: list[str] = field(default_factory=list)
    risk_flags_json: list[str] = field(default_factory=list)
    confidence_json: dict[str, object] = field(default_factory=dict)
    needs_human_review: bool = False
    review_status: str = "draft"
    review_scope: str | None = None
    review_reason_codes_json: list[str] = field(default_factory=list)
    review_question: str = ""
    approved_subtitle_text: str = ""
    approved_tts_text: str = ""
    semantic_qc_passed: bool = False
    semantic_qc_issues_json: list[dict[str, object]] = field(default_factory=list)
    source_template_family_id: str | None = None
    adaptation_template_family_id: str | None = None
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class ProjectWorkspace:
    project_id: str
    name: str
    root_dir: Path
    database_path: Path
    project_json_path: Path
    logs_dir: Path
    cache_dir: Path
    exports_dir: Path
    video_asset_id: str | None = None
    source_video_path: Path | None = None
