from __future__ import annotations

import json
import logging
from pathlib import Path

from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest, SegmentRecord
from app.translate.contextual_runtime import run_contextual_translation
from app.translate.models import (
    ContextualRunMetrics,
    NarrationBudgetPolicy,
    NarrationCanonicalEntityItem,
    NarrationCanonicalItem,
    NarrationCanonicalSpanOutput,
    NarrationSlotRewriteItem,
    NarrationSlotRewriteOutput,
    SceneRouteDecision,
)
from app.translate.narration_fast_v2 import (
    DIALOGUE_LEGACY_ROUTE,
    NARRATION_FAST_V2_ROUTE,
    _apply_scientific_notation_autofix,
    _build_narration_spans,
    _budget_allows_soft_escalation,
    _route_scene_v2,
)
from app.translate.presets import load_prompt_template
from app.translate.scene_chunker import SceneChunk


class _DummyToken:
    def raise_if_canceled(self) -> None:
        return None


class _DummyContext:
    def __init__(self) -> None:
        self.cancellation_token = _DummyToken()
        self.logger = logging.getLogger("test.narration_fast_v2")

    def report_progress(self, _value: int, _message: str) -> None:
        return None


def _load_regression_fixture(name: str) -> dict[str, object]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "regression" / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


class _NarrationFastV2Engine:
    def __init__(self) -> None:
        self.base_calls = 0
        self.slot_rewrite_calls = 0
        self.entity_calls = 0
        self.ambiguity_calls = 0

    def analyze_narration_canonical_span(self, _context, **kwargs):
        self.base_calls += 1
        render_units = list(kwargs["span_payload"]["span"]["render_units"])
        return NarrationCanonicalSpanOutput(
            items=[
                NarrationCanonicalItem(
                    canonical_text="Con người có thể vượt qua giới hạn sinh học này không",
                    risk_flags=[],
                    entities=[
                        NarrationCanonicalEntityItem(
                            src="120岁",
                            dst="120 tuổi",
                            status="approved",
                            confidence=0.99,
                        )
                    ],
                    needs_shortening=False,
                    unsafe_to_guess=False,
                )
                for _item in render_units
            ]
        )

    def resolve_narration_entities(self, *_args, **_kwargs):
        self.entity_calls += 1
        raise AssertionError("Entity micro-pass should not run for this safe narration sample")

    def resolve_narration_ambiguity(self, *_args, **_kwargs):
        self.ambiguity_calls += 1
        raise AssertionError("Ambiguity micro-pass should not run for this safe narration sample")

    def rewrite_narration_slot(self, *_args, **_kwargs):
        self.slot_rewrite_calls += 1
        raise AssertionError("Slot rewrite should not run for this safe narration sample")

    def plan_scene(self, *_args, **_kwargs):
        raise AssertionError("Dialogue planner should not run for narration_fast_v2 sample")

    def analyze_semantics(self, *_args, **_kwargs):
        raise AssertionError("Dialogue semantic pass should not run for narration_fast_v2 sample")

    def adapt_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Dialogue adaptation should not run for narration_fast_v2 sample")

    def critique_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Semantic critic should not run for narration_fast_v2 sample")

    @staticmethod
    def estimate_total_cost_usd(_metrics, *, model: str) -> float:
        del model
        return 0.05


class _ScientificNotationEngine:
    def __init__(self, fixture: dict[str, object]) -> None:
        self.fixture = fixture
        self.base_calls = 0
        self.slot_rewrite_calls = 0
        self.entity_calls = 0
        self.ambiguity_calls = 0

    def analyze_narration_canonical_span(self, _context, **kwargs):
        self.base_calls += 1
        render_units = list(kwargs["span_payload"]["span"]["render_units"])
        items = [
            NarrationCanonicalItem.model_validate(item)
            for item in self.fixture["canonical_items"][: len(render_units)]
        ]
        return NarrationCanonicalSpanOutput(items=items)

    def resolve_narration_entities(self, *_args, **_kwargs):
        self.entity_calls += 1
        raise AssertionError("Entity micro-pass should not run for scientific notation auto-fix sample")

    def resolve_narration_ambiguity(self, *_args, **_kwargs):
        self.ambiguity_calls += 1
        raise AssertionError("Ambiguity micro-pass should not run when scientific notation auto-fix is sufficient")

    def rewrite_narration_slot(self, *_args, **_kwargs):
        self.slot_rewrite_calls += 1
        items = list(_kwargs["span_payload"]["span"]["items"])
        return NarrationSlotRewriteOutput(
            items=[
                NarrationSlotRewriteItem(
                    canonical_text=str(item["canonical_text"]),
                    changed=False,
                    risk_flags=[],
                    review_reason_codes=[],
                )
                for item in items
            ]
        )

    def plan_scene(self, *_args, **_kwargs):
        raise AssertionError("Dialogue planner should not run for narration_fast_v2 sample")

    def analyze_semantics(self, *_args, **_kwargs):
        raise AssertionError("Dialogue semantic pass should not run for narration_fast_v2 sample")

    def adapt_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Dialogue adaptation should not run for narration_fast_v2 sample")

    def critique_dialogue(self, *_args, **_kwargs):
        raise AssertionError("Semantic critic should not run for narration_fast_v2 sample")

    @staticmethod
    def estimate_total_cost_usd(_metrics, *, model: str) -> float:
        del model
        return 0.01


def _build_scene(scene_id: str, texts: list[str]) -> SceneChunk:
    segments = [
        {
            "segment_id": f"{scene_id}-seg-{index}",
            "segment_index": index,
            "start_ms": index * 4000,
            "end_ms": index * 4000 + 3500,
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
        end_ms=max(3500, len(segments) * 4000),
        segment_ids=[str(row["segment_id"]) for row in segments],
        segments=segments,
    )


def test_route_scene_v2_falls_back_for_direct_speech() -> None:
    scene = _build_scene(
        "scene-quoted",
        [
            "他说：“我们现在不能再继续下潜了。”",
            "接着他又补充说这只是暂时的判断。",
        ],
    )

    decision = _route_scene_v2(
        scene,
        prior_analysis_rows={},
        narration_family_id="contextual-narration-fast-v2-vi",
        dialogue_family_id="contextual-default-vi",
        prefer_narration=True,
    )

    assert decision.route_mode == DIALOGUE_LEGACY_ROUTE


def test_build_narration_spans_groups_adjacent_narration_scenes() -> None:
    scenes = [
        SceneChunk(
            scene_id="scene_0000",
            scene_index=0,
            start_segment_index=0,
            end_segment_index=1,
            start_ms=0,
            end_ms=4000,
            segment_ids=["seg-0", "seg-1"],
            segments=[
                {"segment_id": "seg-0", "segment_index": 0, "start_ms": 0, "end_ms": 1800, "source_text": "这是第一段旁白。"},
                {"segment_id": "seg-1", "segment_index": 1, "start_ms": 2000, "end_ms": 4000, "source_text": "这是第二段旁白。"},
            ],
        ),
        SceneChunk(
            scene_id="scene_0001",
            scene_index=1,
            start_segment_index=2,
            end_segment_index=2,
            start_ms=4500,
            end_ms=7000,
            segment_ids=["seg-2"],
            segments=[
                {"segment_id": "seg-2", "segment_index": 2, "start_ms": 4500, "end_ms": 7000, "source_text": "这是第三段旁白。"},
            ],
        ),
    ]
    route_decisions = [
        SceneRouteDecision(
            scene_id="scene_0000",
            scene_index=0,
            route_mode=NARRATION_FAST_V2_ROUTE,
            prompt_family_id="contextual-narration-fast-v2-vi",
            narration_score=0.9,
        ),
        SceneRouteDecision(
            scene_id="scene_0001",
            scene_index=1,
            route_mode=NARRATION_FAST_V2_ROUTE,
            prompt_family_id="contextual-narration-fast-v2-vi",
            narration_score=0.92,
        ),
    ]

    spans, span_by_scene_id = _build_narration_spans(scenes, route_decisions)

    assert len(spans) == 1
    assert spans[0].render_unit_count == 3
    assert span_by_scene_id["scene_0000"].span_id == spans[0].span_id
    assert span_by_scene_id["scene_0001"].span_id == spans[0].span_id


def test_apply_scientific_notation_autofix_repairs_incomplete_exponent_when_hint_is_unique() -> None:
    fixture = _load_regression_fixture("zh-vi-narration-scientific-notation-autofix.json")
    render_rows = [dict(item) for item in fixture["segments"]]
    canonical_by_segment = {
        str(segment["segment_id"]): {
            "segment_id": str(segment["segment_id"]),
            "scene_id": str(fixture["scene_id"]),
            "segment_index": int(segment["segment_index"]),
            "source_text": str(segment["source_text"]),
            "canonical_text": str(canonical["canonical_text"]),
            "risk_flags": list(canonical.get("risk_flags", [])),
            "needs_shortening": bool(canonical.get("needs_shortening", False)),
            "unsafe_to_guess": bool(canonical.get("unsafe_to_guess", False)),
            "entities": [],
            "slot_pressure": 0.0,
            "review_reason_codes": [],
            "review_question": "",
        }
        for segment, canonical in zip(fixture["segments"], fixture["canonical_items"], strict=True)
    }

    changed_segment_ids = _apply_scientific_notation_autofix(
        render_rows=render_rows,
        canonical_by_segment=canonical_by_segment,
    )

    segment_id = str(fixture["expected_segment_id"])
    assert changed_segment_ids == [segment_id]
    assert canonical_by_segment[segment_id]["canonical_text"] == str(fixture["expected_canonical_text"])
    assert canonical_by_segment[segment_id]["unsafe_to_guess"] is False


def test_apply_scientific_notation_autofix_keeps_review_when_hints_conflict() -> None:
    fixture = _load_regression_fixture("zh-vi-narration-scientific-notation-autofix.json")
    render_rows = [dict(item) for item in fixture["segments"]]
    render_rows[2]["source_text"] = "这个数字指向12的量级。"
    conflicting_payloads = [dict(item) for item in fixture["canonical_items"]]
    conflicting_payloads[2]["canonical_text"] = "Con số này chỉ ra cấp 12."
    conflicting_payloads[1]["canonical_text"] = "18 lần."
    canonical_by_segment = {
        str(segment["segment_id"]): {
            "segment_id": str(segment["segment_id"]),
            "scene_id": str(fixture["scene_id"]),
            "segment_index": int(segment["segment_index"]),
            "source_text": str(segment["source_text"]),
            "canonical_text": str(canonical["canonical_text"]),
            "risk_flags": list(canonical.get("risk_flags", [])),
            "needs_shortening": bool(canonical.get("needs_shortening", False)),
            "unsafe_to_guess": bool(canonical.get("unsafe_to_guess", False)),
            "entities": [],
            "slot_pressure": 0.0,
            "review_reason_codes": [],
            "review_question": "",
        }
        for segment, canonical in zip(render_rows, conflicting_payloads, strict=True)
    }

    changed_segment_ids = _apply_scientific_notation_autofix(
        render_rows=render_rows,
        canonical_by_segment=canonical_by_segment,
    )

    segment_id = str(fixture["expected_segment_id"])
    assert changed_segment_ids == []
    assert canonical_by_segment[segment_id]["canonical_text"] == "Đó là một đại lượng vật lý ở cấp 10^."
    assert canonical_by_segment[segment_id]["unsafe_to_guess"] is True


def test_budget_governor_soft_stops_when_cost_crosses_threshold() -> None:
    metrics = ContextualRunMetrics(estimated_cost_usd=0.26)
    policy = NarrationBudgetPolicy(max_llm_cost_usd=0.30, soft_stop_ratio=0.85)

    allowed = _budget_allows_soft_escalation(metrics, policy)

    assert not allowed
    assert metrics.budget_soft_stop_hit is True


def test_run_contextual_translation_dispatches_narration_fast_v2_without_default_adaptation(tmp_path: Path) -> None:
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Fast V2 Runtime",
            root_dir=tmp_path / "narration-fast-v2-runtime",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-v2-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id="seg-001",
                project_id=workspace.project_id,
                segment_index=0,
                start_ms=0,
                end_ms=4000,
                source_lang="zh",
                target_lang="vi",
                source_text="现代医学显著提高了平均寿命。",
                source_text_norm="现代医学显著提高了平均寿命。",
            ),
            SegmentRecord(
                segment_id="seg-002",
                project_id=workspace.project_id,
                segment_index=1,
                start_ms=4200,
                end_ms=8400,
                source_lang="zh",
                target_lang="vi",
                source_text="但人类真的能突破一百二十岁的生理极限吗。",
                source_text_norm="但人类真的能突破一百二十岁的生理极限吗。",
            ),
        ],
    )
    segments = database.list_segments(workspace.project_id)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_slot_rewrite")
    engine = _NarrationFastV2Engine()

    result = run_contextual_translation(
        _DummyContext(),
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-4.1-mini",
    )

    assert engine.base_calls == 1
    assert engine.slot_rewrite_calls == 0
    assert engine.entity_calls == 0
    assert engine.ambiguity_calls == 0
    assert result["fast_path"]["mode"] == "narration_fast_v2"
    assert result["metrics"].base_semantic_call_count == 1
    assert all(
        analysis.approved_subtitle_text == analysis.approved_tts_text
        for analysis in result["segment_analyses"]
    )
    assert result["semantic_qc"]["error_count"] == 0


def test_run_contextual_translation_auto_resolves_scientific_notation_without_review(tmp_path: Path) -> None:
    fixture = _load_regression_fixture("zh-vi-narration-scientific-notation-autofix.json")
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Narration Scientific Notation Auto Fix",
            root_dir=tmp_path / "narration-scientific-notation-auto-fix",
            source_language="zh",
            target_language="vi",
            project_profile_id="zh-vi-narration-fast-v2-vieneu",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id=str(item["segment_id"]),
                project_id=workspace.project_id,
                segment_index=int(item["segment_index"]),
                start_ms=int(item["start_ms"]),
                end_ms=int(item["end_ms"]),
                source_lang="zh",
                target_lang="vi",
                source_text=str(item["source_text"]),
                source_text_norm=str(item["source_text"]),
            )
            for item in fixture["segments"]
        ],
    )
    segments = database.list_segments(workspace.project_id)
    selected_template = load_prompt_template(workspace.root_dir, "contextual_narration_slot_rewrite")
    engine = _ScientificNotationEngine(fixture)

    result = run_contextual_translation(
        _DummyContext(),
        workspace=workspace,
        database=database,
        engine=engine,
        segments=segments,
        selected_template=selected_template,
        source_language="zh",
        target_language="vi",
        model="gpt-4.1-mini",
    )

    target_segment_id = str(fixture["expected_segment_id"])
    analyses_by_segment = {item.segment_id: item for item in result["segment_analyses"]}

    assert engine.base_calls == 1
    assert engine.entity_calls == 0
    assert engine.ambiguity_calls == 0
    assert target_segment_id in analyses_by_segment
    assert analyses_by_segment[target_segment_id].approved_subtitle_text == str(fixture["expected_canonical_text"])
    assert analyses_by_segment[target_segment_id].approved_tts_text == str(fixture["expected_canonical_text"])
    assert analyses_by_segment[target_segment_id].needs_human_review is False
    assert "ambiguous_term" not in analyses_by_segment[target_segment_id].review_reason_codes_json
