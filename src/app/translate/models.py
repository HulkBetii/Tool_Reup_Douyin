from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


STRICT_MODEL_CONFIG = ConfigDict(populate_by_name=True, extra="forbid")


class TranslationPromptTemplate(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    template_id: str
    name: str
    family_id: str | None = None
    translation_mode: str = "legacy"
    role: str = "legacy_translate"
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
    model_config = STRICT_MODEL_CONFIG

    segment_id: str
    translated_text: str
    subtitle_text: str
    tts_text: str


class BatchTranslationOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[TranslationOutputItem]


class CharacterSeed(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    character_id: str
    canonical_name_zh: str = ""
    canonical_name_vi: str = ""
    aliases: list[str] = Field(default_factory=list)
    gender_hint: str | None = None
    age_role: str | None = None
    social_role: str | None = None
    speech_style: str | None = None
    default_self_terms: list[str] = Field(default_factory=list)
    default_address_terms: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    status: str = "hypothesized"
    evidence_segment_ids: list[str] = Field(default_factory=list)


class RelationshipSeed(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    relationship_id: str
    from_character_id: str
    to_character_id: str
    relation_type: str = "unknown"
    power_delta: str | None = None
    age_delta: str | None = None
    intimacy_level: str | None = None
    default_self_term: str | None = None
    default_address_term: str | None = None
    allowed_alternates: list[str] | dict[str, list[str]] = Field(default_factory=list)
    scope: str = "scene"
    confidence: float = 0.0
    status: str = "hypothesized"
    evidence_segment_ids: list[str] = Field(default_factory=list)


class KnowledgeState(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    character_id: str = "unknown"
    summary: str = ""


class PatchSuggestion(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    field_name: str
    value: str


class ScenePlannerOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    scene_id: str
    scene_summary: str
    participants: list[str] = Field(default_factory=list)
    location: str | None = None
    time_context: str | None = None
    active_topic: str | None = None
    current_conflict: str | None = None
    current_emotional_tone: str | None = None
    temporary_addressing_mode: str | None = None
    recent_turn_digest: str = ""
    who_knows_what: list[KnowledgeState] = Field(default_factory=list)
    open_ambiguities: list[str] = Field(default_factory=list)
    unresolved_references: list[str] = Field(default_factory=list)
    character_updates: list[CharacterSeed] = Field(default_factory=list)
    relationship_updates: list[RelationshipSeed] = Field(default_factory=list)


class SpeakerDecision(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    character_id: str = "unknown"
    speaker_cluster_id: str | None = None
    source: str = "inferred"
    confidence: float = 0.0


class ListenerDecision(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    character_id: str = "unknown"
    role: str = "primary"
    confidence: float = 0.0


class RegisterDecision(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    politeness: str = "informal"
    power_direction: str = "peer"
    emotional_tone: str = "neutral"
    confidence: float = 0.0


class ResolvedEllipsis(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    omitted_subject: str | None = None
    omitted_object: str | None = None
    confidence: float = 0.0


class HonorificPolicy(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    policy_id: str = ""
    self_term: str = ""
    address_term: str = ""
    locked: bool = False
    confidence: float = 0.0


class ConfidenceBreakdown(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    overall: float = 0.0
    speaker: float = 0.0
    listener: float = 0.0
    register_score: float = Field(default=0.0, alias="register")
    relation: float = 0.0
    translation: float = 0.0


class SegmentSemanticAnalysisItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    segment_id: str
    scene_id: str
    speaker: SpeakerDecision
    listeners: list[ListenerDecision] = Field(default_factory=list)
    turn_function: str = "statement"
    register_data: RegisterDecision = Field(alias="register")
    resolved_ellipsis: ResolvedEllipsis = Field(default_factory=ResolvedEllipsis)
    honorific_policy: HonorificPolicy = Field(default_factory=HonorificPolicy)
    semantic_translation: str
    glossary_hits: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    confidence: ConfidenceBreakdown = Field(default_factory=ConfidenceBreakdown)
    needs_human_review: bool = False
    review_reason_codes: list[str] = Field(default_factory=list)
    review_question: str = ""


class SemanticBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[SegmentSemanticAnalysisItem]


class NarrationSemanticAnalysisItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    speaker: SpeakerDecision
    listeners: list[ListenerDecision] = Field(default_factory=list)
    turn_function: str = "statement"
    register_data: RegisterDecision = Field(alias="register")
    resolved_ellipsis: ResolvedEllipsis = Field(default_factory=ResolvedEllipsis)
    honorific_policy: HonorificPolicy = Field(default_factory=HonorificPolicy)
    semantic_translation: str
    glossary_hits: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    confidence: ConfidenceBreakdown = Field(default_factory=ConfidenceBreakdown)
    needs_human_review: bool = False
    review_reason_codes: list[str] = Field(default_factory=list)
    review_question: str = ""


class NarrationSemanticBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[NarrationSemanticAnalysisItem]


class DialogueAdaptationItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    segment_id: str
    honorific_policy: HonorificPolicy = Field(default_factory=HonorificPolicy)
    subtitle_text: str
    tts_text: str
    risk_flags: list[str] = Field(default_factory=list)
    needs_human_review: bool = False
    review_reason_codes: list[str] = Field(default_factory=list)


class DialogueAdaptationBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[DialogueAdaptationItem]


class NarrationAdaptationItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    honorific_policy: HonorificPolicy = Field(default_factory=HonorificPolicy)
    subtitle_text: str
    tts_text: str
    risk_flags: list[str] = Field(default_factory=list)
    needs_human_review: bool = False
    review_reason_codes: list[str] = Field(default_factory=list)


class NarrationAdaptationBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[NarrationAdaptationItem]


class NarrationTermEntityItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    source_term: str
    preferred_vi: str = ""
    category: str = "term"
    status: str = "prefer"
    confidence: float = 0.0
    segment_positions: list[int] = Field(default_factory=list)
    notes: str = ""


class NarrationTermEntityBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[NarrationTermEntityItem] = Field(default_factory=list)


class ResolvedNarrationTermEntityItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    source_term: str
    preferred_vi: str = ""
    category: str = "term"
    status: str = "prefer"
    confidence: float = 0.0
    segment_ids: list[str] = Field(default_factory=list)
    notes: str = ""


class SceneTermEntitySheet(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    scene_id: str
    route_mode: str = "narration_fast"
    active: bool = True
    items: list[ResolvedNarrationTermEntityItem] = Field(default_factory=list)


class SemanticCriticIssue(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    code: str
    severity: str
    message: str


class SemanticCriticItem(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    segment_id: str
    passed: bool = True
    review_needed: bool = False
    error_codes: list[str] = Field(default_factory=list)
    issues: list[SemanticCriticIssue] = Field(default_factory=list)
    minimal_patch: list[PatchSuggestion] = Field(default_factory=list)


class SemanticCriticBatchOutput(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    items: list[SemanticCriticItem]


class SceneRouteDecision(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    scene_id: str
    scene_index: int
    route_mode: str
    prompt_family_id: str
    narration_score: float
    speaker_dominance: float = 0.0
    speaker_switch_density: float = 0.0
    question_density: float = 0.0
    vocative_density: float = 0.0
    backchannel_density: float = 0.0
    short_utterance_ratio: float = 0.0
    long_sentence_ratio: float = 0.0
    prior_review_penalty: float = 0.0
    fallback_reason: str = ""


class LLMCallMetric(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    role: str
    route_mode: str
    scene_id: str = ""
    batch_index: int | None = None
    batch_count: int | None = None
    prompt_cache_key: str = ""
    input_token_count: int | None = None
    output_token_count: int | None = None


class ContextualRunMetrics(BaseModel):
    model_config = STRICT_MODEL_CONFIG

    llm_call_count: int = 0
    llm_retry_count: int = 0
    batch_count: int = 0
    narration_batch_size_caps: dict[str, int] = Field(default_factory=dict)
    term_entity_pass_scene_count: int = 0
    term_entity_entry_count: int = 0
    term_entity_review_hint_count: int = 0
    call_metrics: list[LLMCallMetric] = Field(default_factory=list)
