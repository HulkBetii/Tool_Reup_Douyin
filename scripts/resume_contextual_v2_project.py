from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from app.core.jobs import CancellationToken, JobContext
from app.core.settings import load_settings
from app.project.bootstrap import open_project
from app.project.database import ProjectDatabase
from app.project.profiles import load_project_profile_state
from app.translate.contextual_checkpoint import (
    load_contextual_translation_checkpoint,
    persist_contextual_translation_checkpoint,
)
from app.translate.contextual_pipeline import build_contextual_translation_stage_hash, persist_contextual_translation_result
from app.translate.contextual_runtime import run_contextual_translation
from app.translate.openai_engine import OpenAITranslationEngine
from app.translate.presets import load_prompt_template

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _print(message: str) -> None:
    print(message, flush=True)


def _context(stage: str) -> JobContext:
    return JobContext(
        job_id=f"resume-contextual-v2-{stage}",
        logger_name=f"scripts.resume_contextual_v2.{stage}",
        cancellation_token=CancellationToken(),
        progress_callback=lambda value, message: _print(f"[{stage}] {value:>3}% {message}"),
    )


def _review_samples(database: ProjectDatabase, project_id: str, limit: int = 10) -> tuple[dict[str, int], list[dict[str, object]]]:
    rows = database.list_review_queue_items(project_id)
    counter: Counter[str] = Counter()
    samples: list[dict[str, object]] = []
    for row in rows[:limit]:
        reason_codes = json.loads(row["review_reason_codes_json"] or "[]")
        counter.update(str(code) for code in reason_codes)
        samples.append(
            {
                "segment_index": int(row["segment_index"]),
                "scene_index": int(row["scene_index"]),
                "source_text": row["source_text"],
                "subtitle_text": row["approved_subtitle_text"],
                "tts_text": row["approved_tts_text"],
                "review_reason_codes": reason_codes,
                "review_question": row["review_question"],
                "scene_summary": row["short_scene_summary"],
            }
        )
    return dict(counter), samples


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resume contextual translation on an existing project.")
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--prompt-template-id")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = load_settings()
    if not settings.openai_api_key:
        raise RuntimeError("Chua co OpenAI API key trong settings")

    project_root = args.project_root.expanduser().resolve()
    workspace = open_project(project_root)
    database = ProjectDatabase(workspace.database_path)
    project_row = database.get_project()
    if project_row is None:
        raise RuntimeError("Khong tim thay project row")
    segments = database.list_segments(workspace.project_id)
    if not segments:
        raise RuntimeError("Project chua co ASR segments de resume contextual translation")

    profile_state = load_project_profile_state(project_root)
    prompt_template_id = (
        args.prompt_template_id
        or (profile_state.recommended_prompt_template_id if profile_state is not None else None)
        or "contextual_cartoon_fun_adaptation"
    )
    selected_template = load_prompt_template(project_root, prompt_template_id)
    source_language = segments[0]["source_lang"] or str(project_row["source_language"] or "zh")
    target_language = str(project_row["target_language"] or "vi")
    model = settings.default_translation_model

    stage_hash = build_contextual_translation_stage_hash(
        segments=segments,
        template=selected_template,
        project_root=project_root,
        model=model,
        source_language=source_language,
        target_language=target_language,
    )
    checkpoint_state = load_contextual_translation_checkpoint(workspace, stage_hash=stage_hash)
    if checkpoint_state is not None:
        _print(
            "Resume tu checkpoint: "
            f"{checkpoint_state.completed_scene_count}/{checkpoint_state.total_scene_count} scene da hoan thanh"
        )
    else:
        _print("Khong co checkpoint partial, bat dau contextual translation moi tren project hien co")

    contextual_result = run_contextual_translation(
        _context("contextual"),
        workspace=workspace,
        database=database,
        engine=OpenAITranslationEngine(settings),
        segments=segments,
        selected_template=selected_template,
        source_language=source_language,
        target_language=target_language,
        model=model,
        checkpoint_state=checkpoint_state,
        checkpoint_writer=lambda scenes, character_profiles, relationship_profiles, analyses, route_decisions, term_entity_sheets, metrics, completed_scene_ids, total_scene_count: persist_contextual_translation_checkpoint(
            workspace,
            stage_hash=stage_hash,
            selected_template=selected_template,
            scenes=scenes,
            character_profiles=character_profiles,
            relationship_profiles=relationship_profiles,
            analyses=analyses,
            route_decisions=route_decisions,
            term_entity_sheets=term_entity_sheets,
            metrics=metrics,
            completed_scene_ids=completed_scene_ids,
            total_scene_count=total_scene_count,
        ),
    )

    cache_path = persist_contextual_translation_result(
        workspace,
        database=database,
        stage_hash=stage_hash,
        selected_template=selected_template,
        target_language=target_language,
        scenes=contextual_result["scenes"],
        character_profiles=contextual_result["character_profiles"],
        relationship_profiles=contextual_result["relationship_profiles"],
        analyses=contextual_result["segment_analyses"],
        route_decisions=contextual_result.get("route_decisions"),
        metrics=contextual_result.get("metrics"),
        term_entity_sheets=contextual_result.get("term_entity_sheets"),
    )

    review_reason_counts, review_samples = _review_samples(database, workspace.project_id)
    summary = {
        "project_root": str(project_root),
        "translation_mode": "contextual_v2",
        "project_profile_id": profile_state.project_profile_id if profile_state else None,
        "project_profile_prompt_template_id": profile_state.recommended_prompt_template_id if profile_state else None,
        "selected_template": selected_template.template_id,
        "fast_path": contextual_result.get("fast_path"),
        "route_decisions": [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in contextual_result.get("route_decisions", [])
        ],
        "term_entity_sheets": [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in contextual_result.get("term_entity_sheets", [])
        ],
        "metrics": (
            contextual_result["metrics"].model_dump(mode="json")
            if hasattr(contextual_result.get("metrics"), "model_dump")
            else contextual_result.get("metrics")
        ),
        "stage_hash": stage_hash,
        "asr_segment_count": len(segments),
        "scene_count": len(contextual_result["scenes"]),
        "character_profile_count": len(contextual_result["character_profiles"]),
        "relationship_profile_count": len(contextual_result["relationship_profiles"]),
        "pending_review_count": database.count_pending_segment_reviews(workspace.project_id),
        "semantic_qc": contextual_result["semantic_qc"],
        "review_reason_counts": review_reason_counts,
        "cache_path": str(cache_path),
        "review_samples": review_samples,
    }
    summary_path = project_root / "contextual_run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _print(f"Project: {workspace.root_dir}")
    _print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
