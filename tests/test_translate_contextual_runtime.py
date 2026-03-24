from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from pydantic import ValidationError

from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest
from app.translate.contextual_runtime import (
    _fallback_term_anchor_segment_ids,
    _normalize_review_reason_code,
    _route_scene,
    _run_stage_batch_with_positional_retry,
    _run_stage_batch_with_retry,
    run_contextual_translation,
)
from app.translate.models import (
    ConfidenceBreakdown,
    DialogueAdaptationBatchOutput,
    DialogueAdaptationItem,
    HonorificPolicy,
    ListenerDecision,
    LLMCallMetric,
    NarrationAdaptationBatchOutput,
    NarrationAdaptationItem,
    NarrationSemanticAnalysisItem,
    NarrationSemanticBatchOutput,
    NarrationTermEntityBatchOutput,
    NarrationTermEntityItem,
    RegisterDecision,
    ResolvedEllipsis,
    ScenePlannerOutput,
    SemanticCriticBatchOutput,
    SemanticCriticItem,
    SegmentSemanticAnalysisItem,
    SemanticBatchOutput,
    SpeakerDecision,
)
from app.translate.presets import load_prompt_template
from app.translate.scene_chunker import SceneChunk


class _DummyToken:
    def raise_if_canceled(self) -> None:
        return None


class _DummyContext:
    def __init__(self) -> None:
        self.cancellation_token = _DummyToken()
        self.logger = logging.getLogger("test.contextual_runtime")

    def report_progress(self, _value: int, _message: str) -> None:
        return None


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_run_stage_batch_with_retry_splits_when_ids_are_missing() -> None:
    context = _DummyContext()
    rows = [
        {"segment_id": "seg-1"},
        {"segment_id": "seg-2"},
    ]
    calls: list[list[str]] = []

    def stage_runner(current_rows, _batch_index, _batch_count):
        current_ids = [str(row["segment_id"]) for row in current_rows]
        calls.append(current_ids)
        if len(current_rows) > 1:
            return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])
        return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])

    items = _run_stage_batch_with_retry(
        context=context,
        stage_label="Semantic pass",
        scene_id="scene-1",
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
    )

    assert sorted(items) == ["seg-1", "seg-2"]
    assert calls == [["seg-1", "seg-2"], ["seg-1"], ["seg-2"]]


def test_run_stage_batch_with_retry_splits_when_dialogue_adaptation_json_is_truncated() -> None:
    fixture = _load_regression_fixture("zh-vi-dialogue-adaptation-invalid-json-batch-retry.json")
    context = _DummyContext()
    rows = list(fixture["batch_rows"])
    calls: list[list[str]] = []

    def stage_runner(current_rows, _batch_index, _batch_count):
        current_ids = [str(row["segment_id"]) for row in current_rows]
        calls.append(current_ids)
        if len(current_rows) > 1:
            return DialogueAdaptationBatchOutput.model_validate_json(str(fixture["invalid_json"]))
        return SimpleNamespace(items=[SimpleNamespace(segment_id=current_ids[0])])

    items = _run_stage_batch_with_retry(
        context=context,
        stage_label=str(fixture["stage_label"]),
        scene_id=str(fixture["scene_id"]),
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
    )

    assert sorted(items) == ["seg-1", "seg-2"]
    assert calls == [["seg-1", "seg-2"], ["seg-1"], ["seg-2"]]


def test_run_stage_batch_with_retry_preserves_single_row_validation_error() -> None:
    fixture = _load_regression_fixture("zh-vi-dialogue-adaptation-invalid-json-batch-retry.json")
    context = _DummyContext()
    rows = [dict(fixture["batch_rows"][0])]

    def stage_runner(_current_rows, _batch_index, _batch_count):
        return DialogueAdaptationBatchOutput.model_validate_json(str(fixture["invalid_json"]))

    try:
        _run_stage_batch_with_retry(
            context=context,
            stage_label=str(fixture["stage_label"]),
            scene_id=str(fixture["scene_id"]),
            batch_rows=rows,
            batch_index=1,
            batch_count=1,
            stage_runner=stage_runner,
        )
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for a single-row retryable stage failure")


def test_normalize_review_reason_code_collapses_real_world_variants() -> None:
    assert _normalize_review_reason_code("ambiguous_term_scattered_items") == "ambiguous_term"
    assert _normalize_review_reason_code("Ambiguous term '散'") == "ambiguous_term"
    assert _normalize_review_reason_code("uncertain_speaker_identity") == "uncertain_speaker"
    assert _normalize_review_reason_code("ambiguous_object_reference") == "ambiguous_object_reference"


def _build_scene(scene_id: str, texts: list[str]) -> SceneChunk:
    segments = [
        {
            "segment_id": f"{scene_id}-seg-{index}",
            "segment_index": index,
            "start_ms": index * 1000,
            "end_ms": index * 1000 + 900,
            "source_text": text,
        }
        for index, text in enumerate(texts)
    ]
    return SceneChunk(
        scene_id=scene_id,
        scene_index=0,
        start_segment_index=0,
        end_segment_index=len(segments) - 1,
        start_ms=0,
        end_ms=max(900, len(segments) * 1000),
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=segments,
    )


def test_route_scene_prefers_narration_for_monologue_like_scene() -> None:
    scene = _build_scene(
        "scene-narration",
        [
            "这是一次对地球深层结构的持续观察，我们会一步一步看到地下世界远比想象复杂。",
            "科学家在深层岩石中找到了来自远古海洋生命留下的痕迹，这改变了很多旧判断。",
        ],
    )

    decision = _route_scene(
        scene,
        prior_analysis_rows={},
        narration_family_id="contextual-narration-fast-vi",
        dialogue_family_id="contextual-default-vi",
    )

    assert decision.route_mode == "narration_fast"
    assert decision.prompt_family_id == "contextual-narration-fast-vi"
    assert decision.narration_score >= 0.75


def test_route_scene_falls_back_to_dialogue_for_dialogue_like_scene() -> None:
    scene = _build_scene(
        "scene-dialogue",
        [
            "你看见了吗？",
            "嗯。",
            "那我们现在怎么办？",
            "快走！",
        ],
    )

    decision = _route_scene(
        scene,
        prior_analysis_rows={},
        narration_family_id="contextual-narration-fast-vi",
        dialogue_family_id="contextual-default-vi",
    )

    assert decision.route_mode == "dialogue"
    assert decision.prompt_family_id == "contextual-default-vi"
    assert decision.narration_score <= 0.45


def test_route_scene_uses_dialogue_fallback_for_borderline_scene() -> None:
    scene = _build_scene(
        "scene-borderline",
        [
            "我们接下来会继续往下看这一层岩石结构。",
            "这层结构说明了很多问题，也让后面的判断变得更复杂。",
        ],
    )
    prior_analysis_rows = {
        "scene-borderline-seg-0": {"review_reason_codes_json": ["uncertain_speaker"]},
        "scene-borderline-seg-1": {"review_reason_codes_json": ["uncertain_speaker"]},
    }

    decision = _route_scene(
        scene,
        prior_analysis_rows=prior_analysis_rows,
        narration_family_id="contextual-narration-fast-vi",
        dialogue_family_id="contextual-default-vi",
    )

    assert decision.route_mode == "dialogue"
    assert decision.fallback_reason == "borderline_narration_score"
    assert 0.45 < decision.narration_score < 0.75


def test_fallback_term_anchor_accepts_sqlite_rows() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("create table segments (segment_id text, source_text text)")
        connection.executemany(
            "insert into segments(segment_id, source_text) values (?, ?)",
            [
                ("seg-001", "科学家把这种结构称为深海声散射层，它会在夜间整体上移。"),
                ("seg-002", "深海声散射层每天都在移动，也会改变声纳回波。"),
                ("seg-003", "一种名叫X17层的深层结构仍然没有公认的稳定译名。"),
            ],
        )
        rows = connection.execute("select segment_id, source_text from segments order by rowid").fetchall()
    finally:
        connection.close()

    scene = SimpleNamespace(segments=rows)
    assert _fallback_term_anchor_segment_ids(scene, "深海声散射层") == ["seg-001", "seg-002"]
    assert _fallback_term_anchor_segment_ids(scene, "X17层") == ["seg-003"]


def test_run_stage_batch_with_positional_retry_retries_same_batch_before_split() -> None:
    context = _DummyContext()
    rows = [
        {"segment_id": "seg-1"},
        {"segment_id": "seg-2"},
    ]
    calls: list[list[str]] = []

    def stage_runner(current_rows, _batch_index, _batch_count):
        current_ids = [str(row["segment_id"]) for row in current_rows]
        calls.append(current_ids)
        if len(current_rows) > 1:
            return NarrationAdaptationBatchOutput(
                items=[
                    NarrationAdaptationItem(
                        honorific_policy=HonorificPolicy(),
                        subtitle_text="Ban thu nhat",
                        tts_text="Ban thu nhat",
                    )
                ]
            )
        return NarrationAdaptationBatchOutput(
            items=[
                NarrationAdaptationItem(
                    honorific_policy=HonorificPolicy(),
                    subtitle_text=f"Phat ngon {current_ids[0]}",
                    tts_text=f"Phat ngon {current_ids[0]}",
                )
            ]
        )

    items = _run_stage_batch_with_positional_retry(
        context=context,
        stage_label="Dialogue adaptation",
        scene_id="scene-1",
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
    )

    assert sorted(items) == ["seg-1", "seg-2"]
    assert calls == [["seg-1", "seg-2"], ["seg-1", "seg-2"], ["seg-1"], ["seg-2"]]


def test_run_stage_batch_with_positional_retry_reports_future_batch_downshift() -> None:
    fixture = _load_regression_fixture("zh-vi-narration-positional-under-return-batch-downshift.json")
    context = _DummyContext()
    rows = [
        {"segment_id": f"seg-{index:02d}"}
        for index in range(int(fixture["initial_batch_size"]))
    ]
    downshift_sizes: list[int] = []

    def _build_narration_item(text: str) -> NarrationSemanticAnalysisItem:
        return NarrationSemanticAnalysisItem(
            speaker=SpeakerDecision(character_id="narrator", source="narration", confidence=0.99),
            listeners=[ListenerDecision(character_id="audience", role="audience", confidence=0.99)],
            turn_function="inform",
            register=RegisterDecision(
                politeness="neutral",
                power_direction="neutral",
                emotional_tone="informative",
                confidence=0.95,
            ),
            resolved_ellipsis=ResolvedEllipsis(confidence=0.9),
            honorific_policy=HonorificPolicy(),
            semantic_translation=text,
            glossary_hits=[],
            risk_flags=[],
            confidence=ConfidenceBreakdown(
                overall=0.9,
                speaker=0.99,
                listener=0.99,
                register=0.95,
                relation=0.0,
                translation=0.9,
            ),
            needs_human_review=False,
            review_reason_codes=[],
            review_question="",
        )

    def stage_runner(current_rows, _batch_index, _batch_count):
        if len(current_rows) > int(fixture["reduced_batch_size"]):
            return NarrationSemanticBatchOutput(
                items=[
                    _build_narration_item(f"Under-return {index}")
                    for index in range(int(fixture["under_return_count"]))
                ]
            )
        return NarrationSemanticBatchOutput(
            items=[
                _build_narration_item(f"Row {index}")
                for index, _row in enumerate(current_rows)
            ]
        )

    items = _run_stage_batch_with_positional_retry(
        context=context,
        stage_label=str(fixture["stage_label"]),
        scene_id=str(fixture["scene_id"]),
        batch_rows=rows,
        batch_index=1,
        batch_count=1,
        stage_runner=stage_runner,
        on_backoff=downshift_sizes.append,
    )

    assert len(items) == int(fixture["initial_batch_size"])
    assert downshift_sizes == [int(fixture["reduced_batch_size"])]


class _NarrationFastPathEngine:
    def __init__(self) -> None:
        self.semantic_batch_sizes: list[int] = []
        self.adaptation_batch_sizes: list[int] = []
        self.context_character_profile_counts: list[int] = []
        self.context_relationship_profile_counts: list[int] = []
        self.glossary_counts: list[int] = []

    def plan_scene(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call the LLM scene planner")

    def analyze_semantics(self, _context, **kwargs):
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        self.semantic_batch_sizes.append(len(scene_segments))
        self.context_character_profile_counts.append(len(kwargs["context_payload"]["character_profiles"]))
        self.context_relationship_profile_counts.append(len(kwargs["context_payload"]["relationship_profiles"]))
        self.glossary_counts.append(len(kwargs["glossary_payload"]["relationship_glossary"]))
        return SemanticBatchOutput(
            items=[
                SegmentSemanticAnalysisItem(
                    segment_id=str(item["segment_id"]),
                    scene_id=str(kwargs["batch_payload"]["scene"]["scene_id"]),
                    speaker=SpeakerDecision(
                        character_id="narrator",
                        source="narration",
                        confidence=0.99,
                    ),
                    listeners=[
                        ListenerDecision(
                            character_id="general humanity",
                            role="audience",
                            confidence=0.99,
                        )
                    ],
                    turn_function="inform",
                    register=RegisterDecision(
                        politeness="neutral",
                        power_direction="neutral",
                        emotional_tone="informative",
                        confidence=0.95,
                    ),
                    resolved_ellipsis=ResolvedEllipsis(confidence=0.9),
                    honorific_policy=HonorificPolicy(),
                    semantic_translation=f"VI {item['segment_index']}",
                    glossary_hits=[],
                    risk_flags=[],
                    confidence=ConfidenceBreakdown(
                        overall=0.92,
                        speaker=0.99,
                        listener=0.99,
                        register=0.95,
                        relation=0.0,
                        translation=0.9,
                    ),
                    needs_human_review=False,
                    review_reason_codes=[],
                    review_question="",
                )
                for item in scene_segments
            ]
        )

    def adapt_dialogue(self, _context, **kwargs):
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        self.adaptation_batch_sizes.append(len(scene_segments))
        return DialogueAdaptationBatchOutput(
            items=[
                DialogueAdaptationItem(
                    segment_id=str(item["segment_id"]),
                    honorific_policy=HonorificPolicy(),
                    subtitle_text=f"VI {item['segment_index']}",
                    tts_text=f"VI {item['segment_index']}",
                    risk_flags=[],
                    needs_human_review=False,
                    review_reason_codes=[],
                )
                for item in scene_segments
            ]
        )

    def critique_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call semantic critic")


def test_run_contextual_translation_uses_narration_fast_path(monkeypatch, tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Fast Runtime",
            root_dir=tmp_path / "narration-fast-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _NarrationFastPathEngine()

    segments = [
        {
            "segment_id": f"seg-{index:02d}",
            "segment_index": index,
            "start_ms": index * 1000,
            "end_ms": index * 1000 + 900,
            "source_text": f"源文本 {index}",
        }
        for index in range(9)
    ]
    scene = SceneChunk(
        scene_id="scene_0000",
        scene_index=0,
        start_segment_index=0,
        end_segment_index=8,
        start_ms=0,
        end_ms=8900,
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=list(segments),
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    assert result["fast_path"]["active"] is True
    assert result["fast_path"]["planner_mode"] == "deterministic"
    assert result["fast_path"]["critic_enabled"] is False
    assert result["fast_path"]["semantic_batch_size"] == 16
    assert engine.semantic_batch_sizes == [9]
    assert engine.adaptation_batch_sizes == [9]
    assert engine.context_character_profile_counts == [0]
    assert engine.context_relationship_profile_counts == [0]
    assert engine.glossary_counts == [0]
    assert result["semantic_qc"]["error_count"] == 0


class _NarrationTermSheetEngine:
    def __init__(self, fixture: dict[str, object]) -> None:
        self.fixture = fixture
        self.term_pass_calls = 0
        self.semantic_glossaries: list[dict[str, object]] = []
        self.adaptation_glossaries: list[dict[str, object]] = []

    def plan_scene(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call the LLM scene planner")

    def extract_term_entities(self, _context, **_kwargs):
        self.term_pass_calls += 1
        return NarrationTermEntityBatchOutput(
            items=[NarrationTermEntityItem.model_validate(item) for item in self.fixture["term_sheet"]]
        )

    def analyze_semantics(self, _context, **kwargs):
        self.semantic_glossaries.append(dict(kwargs["glossary_payload"]))
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        return NarrationSemanticBatchOutput(
            items=[
                NarrationSemanticAnalysisItem(
                    speaker=SpeakerDecision(character_id="narrator", source="narration", confidence=0.99),
                    listeners=[ListenerDecision(character_id="audience", role="audience", confidence=0.99)],
                    turn_function="inform",
                    register=RegisterDecision(
                        politeness="neutral",
                        power_direction="neutral",
                        emotional_tone="informative",
                        confidence=0.95,
                    ),
                    resolved_ellipsis=ResolvedEllipsis(confidence=0.9),
                    honorific_policy=HonorificPolicy(),
                    semantic_translation=f"VI {item['segment_index']}",
                    glossary_hits=[],
                    risk_flags=[],
                    confidence=ConfidenceBreakdown(
                        overall=0.92,
                        speaker=0.99,
                        listener=0.99,
                        register=0.95,
                        relation=0.0,
                        translation=0.9,
                    ),
                    needs_human_review=False,
                    review_reason_codes=[],
                    review_question="",
                )
                for item in scene_segments
            ]
        )

    def adapt_dialogue(self, _context, **kwargs):
        self.adaptation_glossaries.append(dict(kwargs["glossary_payload"]))
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        return NarrationAdaptationBatchOutput(
            items=[
                NarrationAdaptationItem(
                    honorific_policy=HonorificPolicy(),
                    subtitle_text=f"VI {item['segment_index']}",
                    tts_text=f"VI {item['segment_index']}",
                    risk_flags=[],
                    needs_human_review=False,
                    review_reason_codes=[],
                )
                for item in scene_segments
            ]
        )

    def critique_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call semantic critic")


def test_run_contextual_translation_builds_narration_term_sheet_and_routes_review(monkeypatch, tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-narration-term-entity-mini-pass.json")
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Term Sheet Runtime",
            root_dir=tmp_path / "narration-term-sheet-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _NarrationTermSheetEngine(fixture)

    segments = list(fixture["segments"])
    scene = SceneChunk(
        scene_id=str(fixture["scene_id"]),
        scene_index=0,
        start_segment_index=0,
        end_segment_index=len(segments) - 1,
        start_ms=0,
        end_ms=int(segments[-1]["end_ms"]),
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=segments,
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    assert engine.term_pass_calls == 1
    assert len(engine.semantic_glossaries) == 1
    assert len(engine.semantic_glossaries[0]["narration_term_sheet"]) == 2
    assert len(engine.adaptation_glossaries[0]["narration_term_sheet"]) == 2
    assert result["metrics"].term_entity_pass_scene_count == 1
    assert result["metrics"].term_entity_entry_count == 2
    assert result["metrics"].term_entity_review_hint_count == 1
    assert result["fast_path"]["term_entity_pass"] == {
        "scene_count": 1,
        "entry_count": 2,
        "review_hint_count": 1,
    }
    assert result["term_entity_sheets"][0].items[0].preferred_vi == "bơm carbon sinh học"
    analyses_by_segment = {item.segment_id: item for item in result["segment_analyses"]}
    assert analyses_by_segment["seg-001"].needs_human_review is False
    assert analyses_by_segment["seg-002"].needs_human_review is False
    assert analyses_by_segment["seg-003"].needs_human_review is True
    assert "technical_term_uncertainty" in analyses_by_segment["seg-003"].review_reason_codes_json
    assert "X17层" in analyses_by_segment["seg-003"].review_question


def test_run_contextual_translation_falls_back_to_source_text_term_anchoring(monkeypatch, tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-narration-term-entity-anchor-fallback.json")
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Term Anchor Runtime",
            root_dir=tmp_path / "narration-term-anchor-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _NarrationTermSheetEngine(fixture)

    segments = list(fixture["segments"])
    scene = SceneChunk(
        scene_id=str(fixture["scene_id"]),
        scene_index=0,
        start_segment_index=0,
        end_segment_index=len(segments) - 1,
        start_ms=0,
        end_ms=int(segments[-1]["end_ms"]),
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=segments,
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    anchored_items = {item.source_term: item for item in result["term_entity_sheets"][0].items}
    assert anchored_items["深海声散射层"].segment_ids == ["seg-001", "seg-002"]
    assert anchored_items["X17层"].segment_ids == ["seg-003"]
    analyses_by_segment = {item.segment_id: item for item in result["segment_analyses"]}
    assert analyses_by_segment["seg-001"].needs_human_review is False
    assert analyses_by_segment["seg-002"].needs_human_review is False
    assert analyses_by_segment["seg-003"].needs_human_review is True
    assert "technical_term_uncertainty" in analyses_by_segment["seg-003"].review_reason_codes_json
    assert "X17层" in analyses_by_segment["seg-003"].review_question


class _NarrationTermSheetSkipEngine(_NarrationFastPathEngine):
    def __init__(self) -> None:
        super().__init__()
        self.term_pass_calls = 0

    def extract_term_entities(self, *_args, **_kwargs):
        self.term_pass_calls += 1
        return NarrationTermEntityBatchOutput(items=[])


def test_run_contextual_translation_skips_term_sheet_for_simple_short_narration(monkeypatch, tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Term Sheet Skip Runtime",
            root_dir=tmp_path / "narration-term-sheet-skip-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _NarrationTermSheetSkipEngine()

    segments = [
        {
            "segment_id": "seg-a",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 900,
            "source_text": "镜头继续下潜。",
        },
        {
            "segment_id": "seg-b",
            "segment_index": 1,
            "start_ms": 1000,
            "end_ms": 1900,
            "source_text": "海水渐渐变暗。",
        },
    ]
    scene = SceneChunk(
        scene_id="scene_short_narration",
        scene_index=0,
        start_segment_index=0,
        end_segment_index=1,
        start_ms=0,
        end_ms=1900,
        segment_ids=["seg-a", "seg-b"],
        segments=segments,
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    assert engine.term_pass_calls == 0
    assert result["metrics"].term_entity_pass_scene_count == 0
    assert result["fast_path"]["term_entity_pass"]["scene_count"] == 0


class _MixedRouteEngine:
    def __init__(self) -> None:
        self.plan_scene_calls = 0
        self.critic_calls = 0
        self.semantic_route_modes: list[str] = []
        self.adaptation_route_modes: list[str] = []

    def plan_scene(self, _context, **kwargs):
        self.plan_scene_calls += 1
        record_call = kwargs.get("record_call")
        if record_call is not None:
            record_call(
                LLMCallMetric(
                    role="scene_planner",
                    route_mode=str(kwargs["route_mode"]),
                    scene_id=str(kwargs["scene_payload"]["scene_id"]),
                )
            )
        scene_id = str(kwargs["scene_payload"]["scene_id"])
        return ScenePlannerOutput(
            scene_id=scene_id,
            scene_summary=f"Scene {scene_id}",
            participants=[],
            recent_turn_digest="",
            open_ambiguities=[],
            unresolved_references=[],
            character_updates=[],
            relationship_updates=[],
        )

    def analyze_semantics(self, _context, **kwargs):
        self.semantic_route_modes.append(str(kwargs["route_mode"]))
        record_call = kwargs.get("record_call")
        if record_call is not None:
            record_call(
                LLMCallMetric(
                    role="semantic_pass",
                    route_mode=str(kwargs["route_mode"]),
                    scene_id=str(kwargs["batch_payload"]["scene"]["scene_id"]),
                )
            )
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        if kwargs["output_model"] is NarrationSemanticBatchOutput:
            return NarrationSemanticBatchOutput(
                items=[
                    NarrationSemanticAnalysisItem(
                        speaker=SpeakerDecision(character_id="narrator", source="narration", confidence=0.99),
                        listeners=[ListenerDecision(character_id="audience", role="audience", confidence=0.99)],
                        turn_function="inform",
                        register=RegisterDecision(
                            politeness="neutral",
                            power_direction="neutral",
                            emotional_tone="informative",
                            confidence=0.95,
                        ),
                        resolved_ellipsis=ResolvedEllipsis(confidence=0.9),
                        honorific_policy=HonorificPolicy(),
                        semantic_translation=f"VI {item['segment_index']}",
                        glossary_hits=[],
                        risk_flags=[],
                        confidence=ConfidenceBreakdown(
                            overall=0.9,
                            speaker=0.99,
                            listener=0.99,
                            register=0.95,
                            relation=0.0,
                            translation=0.9,
                        ),
                        needs_human_review=False,
                        review_reason_codes=[],
                        review_question="",
                    )
                    for item in scene_segments
                ]
            )
        return SemanticBatchOutput(
            items=[
                SegmentSemanticAnalysisItem(
                    segment_id=str(item["segment_id"]),
                    scene_id=str(kwargs["batch_payload"]["scene"]["scene_id"]),
                    speaker=SpeakerDecision(character_id="speaker-a", source="explicit", confidence=0.88),
                    listeners=[ListenerDecision(character_id="speaker-b", role="listener", confidence=0.85)],
                    turn_function="question",
                    register=RegisterDecision(
                        politeness="neutral",
                        power_direction="neutral",
                        emotional_tone="curious",
                        confidence=0.8,
                    ),
                    resolved_ellipsis=ResolvedEllipsis(confidence=0.8),
                    honorific_policy=HonorificPolicy(),
                    semantic_translation=f"VI {item['segment_index']}",
                    glossary_hits=[],
                    risk_flags=[],
                    confidence=ConfidenceBreakdown(
                        overall=0.82,
                        speaker=0.88,
                        listener=0.85,
                        register=0.8,
                        relation=0.0,
                        translation=0.8,
                    ),
                    needs_human_review=False,
                    review_reason_codes=[],
                    review_question="",
                )
                for item in scene_segments
            ]
        )

    def adapt_dialogue(self, _context, **kwargs):
        self.adaptation_route_modes.append(str(kwargs["route_mode"]))
        record_call = kwargs.get("record_call")
        if record_call is not None:
            record_call(
                LLMCallMetric(
                    role="dialogue_adaptation",
                    route_mode=str(kwargs["route_mode"]),
                    scene_id=str(kwargs["batch_payload"]["scene"]["scene_id"]),
                )
            )
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        if kwargs["output_model"] is NarrationAdaptationBatchOutput:
            return NarrationAdaptationBatchOutput(
                items=[
                    NarrationAdaptationItem(
                        honorific_policy=HonorificPolicy(),
                        subtitle_text=f"VI {item['segment_index']}",
                        tts_text=f"VI {item['segment_index']}",
                        risk_flags=[],
                        needs_human_review=False,
                        review_reason_codes=[],
                    )
                    for item in scene_segments
                ]
            )
        return DialogueAdaptationBatchOutput(
            items=[
                DialogueAdaptationItem(
                    segment_id=str(item["segment_id"]),
                    honorific_policy=HonorificPolicy(),
                    subtitle_text=f"VI {item['segment_index']}",
                    tts_text=f"VI {item['segment_index']}",
                    risk_flags=[],
                    needs_human_review=False,
                    review_reason_codes=[],
                )
                for item in scene_segments
            ]
        )

    def critique_dialogue(self, _context, **kwargs):
        self.critic_calls += 1
        record_call = kwargs.get("record_call")
        if record_call is not None:
            record_call(
                LLMCallMetric(
                    role="semantic_critic",
                    route_mode=str(kwargs["route_mode"]),
                    scene_id=str(kwargs["batch_payload"]["scene"]["scene_id"]),
                )
            )
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        return SemanticCriticBatchOutput(
            items=[
                SemanticCriticItem(
                    segment_id=str(item["segment_id"]),
                    passed=True,
                    review_needed=False,
                    error_codes=[],
                    issues=[],
                    minimal_patch=[],
                )
                for item in scene_segments
            ]
        )


def test_run_contextual_translation_routes_mixed_scenes_and_records_metrics(monkeypatch, tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Mixed Runtime",
            root_dir=tmp_path / "narration-mixed-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _MixedRouteEngine()

    narration_segments = [
        {
            "segment_id": "narr-1",
            "segment_index": 0,
            "start_ms": 0,
            "end_ms": 900,
            "source_text": "这是一次对深海生态的持续记录，我们会从最外层慢慢往里看。",
        },
        {
            "segment_id": "narr-2",
            "segment_index": 1,
            "start_ms": 1000,
            "end_ms": 1900,
            "source_text": "镜头拍到的每一个细节，都说明这种生物远比传说更复杂。",
        },
    ]
    dialogue_segments = [
        {
            "segment_id": "dialogue-1",
            "segment_index": 2,
            "start_ms": 2000,
            "end_ms": 2900,
            "source_text": "你看见了吗？",
        },
        {
            "segment_id": "dialogue-2",
            "segment_index": 3,
            "start_ms": 3000,
            "end_ms": 3900,
            "source_text": "嗯，快走！",
        },
    ]
    narration_scene = SceneChunk(
        scene_id="scene_0000",
        scene_index=0,
        start_segment_index=0,
        end_segment_index=1,
        start_ms=0,
        end_ms=1900,
        segment_ids=["narr-1", "narr-2"],
        segments=narration_segments,
    )
    dialogue_scene = SceneChunk(
        scene_id="scene_0001",
        scene_index=1,
        start_segment_index=2,
        end_segment_index=3,
        start_ms=2000,
        end_ms=3900,
        segment_ids=["dialogue-1", "dialogue-2"],
        segments=dialogue_segments,
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [narration_scene, dialogue_scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=[*narration_segments, *dialogue_segments],
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    assert result["fast_path"]["active"] is True
    assert result["fast_path"]["mode"] == "mixed"
    assert result["fast_path"]["route_counts"] == {"narration_fast": 1, "dialogue": 1}
    assert [item.route_mode for item in result["route_decisions"]] == ["narration_fast", "dialogue"]
    assert engine.plan_scene_calls == 1
    assert engine.critic_calls == 1
    assert engine.semantic_route_modes == ["narration_fast", "dialogue"]
    assert engine.adaptation_route_modes == ["narration_fast", "dialogue"]
    assert result["metrics"].llm_call_count >= 4
    assert any(item.route_mode == "narration_fast" for item in result["metrics"].call_metrics)
    assert any(item.route_mode == "dialogue" for item in result["metrics"].call_metrics)


class _NarrationUnderReturnEngine:
    def __init__(self, under_return_threshold: int, under_return_count: int) -> None:
        self.under_return_threshold = under_return_threshold
        self.under_return_count = under_return_count
        self.semantic_batch_sizes: list[int] = []
        self.adaptation_batch_sizes: list[int] = []

    def plan_scene(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call the LLM scene planner")

    def analyze_semantics(self, _context, **kwargs):
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        self.semantic_batch_sizes.append(len(scene_segments))
        if len(scene_segments) > self.under_return_threshold:
            scene_segments = scene_segments[: self.under_return_count]
        return NarrationSemanticBatchOutput(
            items=[
                NarrationSemanticAnalysisItem(
                    speaker=SpeakerDecision(character_id="narrator", source="narration", confidence=0.99),
                    listeners=[ListenerDecision(character_id="audience", role="audience", confidence=0.99)],
                    turn_function="inform",
                    register=RegisterDecision(
                        politeness="neutral",
                        power_direction="neutral",
                        emotional_tone="informative",
                        confidence=0.95,
                    ),
                    resolved_ellipsis=ResolvedEllipsis(confidence=0.9),
                    honorific_policy=HonorificPolicy(),
                    semantic_translation=f"VI {item['segment_index']}",
                    glossary_hits=[],
                    risk_flags=[],
                    confidence=ConfidenceBreakdown(
                        overall=0.92,
                        speaker=0.99,
                        listener=0.99,
                        register=0.95,
                        relation=0.0,
                        translation=0.9,
                    ),
                    needs_human_review=False,
                    review_reason_codes=[],
                    review_question="",
                )
                for item in scene_segments
            ]
        )

    def adapt_dialogue(self, _context, **kwargs):
        scene_segments = list(kwargs["batch_payload"]["scene"]["segments"])
        self.adaptation_batch_sizes.append(len(scene_segments))
        return NarrationAdaptationBatchOutput(
            items=[
                NarrationAdaptationItem(
                    honorific_policy=HonorificPolicy(),
                    subtitle_text=f"VI {item['segment_index']}",
                    tts_text=f"VI {item['segment_index']}",
                    risk_flags=[],
                    needs_human_review=False,
                    review_reason_codes=[],
                )
                for item in scene_segments
            ]
        )

    def critique_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Narration fast path should not call semantic critic")


def test_run_contextual_translation_downshifts_future_narration_batches_after_under_return(
    monkeypatch, tmp_path: Path
) -> None:
    fixture = _load_regression_fixture("zh-vi-narration-positional-under-return-batch-downshift.json")
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Under Return Runtime",
            root_dir=tmp_path / "narration-under-return-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_fast_adaptation")
    context = _DummyContext()
    engine = _NarrationUnderReturnEngine(
        under_return_threshold=int(fixture["reduced_batch_size"]),
        under_return_count=int(fixture["under_return_count"]),
    )

    segments = [
        {
            "segment_id": f"seg-{index:02d}",
            "segment_index": index,
            "start_ms": index * 1000,
            "end_ms": index * 1000 + 900,
            "source_text": f"科学讲述 {index}",
        }
        for index in range(int(fixture["segment_count"]))
    ]
    scene = SceneChunk(
        scene_id=str(fixture["scene_id"]),
        scene_index=0,
        start_segment_index=0,
        end_segment_index=len(segments) - 1,
        start_ms=0,
        end_ms=len(segments) * 1000,
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=segments,
    )
    monkeypatch.setattr(
        "app.translate.contextual_runtime.chunk_segments_into_scenes",
        lambda _segments: [scene],
    )

    result = run_contextual_translation(
        context,
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-test",
    )

    assert result["semantic_qc"]["error_count"] == 0
    assert engine.semantic_batch_sizes == [16, 16, 8, 8, 8, 8, 8]
    assert engine.adaptation_batch_sizes == [16, 16, 8]
    assert result["metrics"].narration_batch_size_caps["semantic_pass"] == int(fixture["reduced_batch_size"])
    assert result["fast_path"]["adaptive_batch_caps"]["semantic_pass"] == int(fixture["reduced_batch_size"])
