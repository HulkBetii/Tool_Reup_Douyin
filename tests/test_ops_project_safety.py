from __future__ import annotations

import json
from pathlib import Path

from app.ops.project_safety import create_workspace_backup, inspect_workspace, repair_workspace_metadata
from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import JobRunRecord, MediaAssetRecord, ProjectInitRequest


def test_create_workspace_backup_copies_core_project_files(tmp_path: Path) -> None:
    workspace = bootstrap_project(ProjectInitRequest(name="Demo", root_dir=tmp_path / "demo"))

    manifest = create_workspace_backup(
        workspace,
        reason="Before rerun",
        stage="translate",
    )

    assert manifest.backup_dir.exists()
    assert (manifest.backup_dir / "project.db").exists()
    assert (manifest.backup_dir / "project.json").exists()
    stored_manifest = json.loads((manifest.backup_dir / "manifest.json").read_text(encoding="utf-8"))
    assert stored_manifest["stage"] == "translate"
    assert stored_manifest["reason"] == "Before rerun"


def test_inspect_workspace_reports_missing_media_assets_and_stale_job_outputs(tmp_path: Path) -> None:
    workspace = bootstrap_project(ProjectInitRequest(name="Demo", root_dir=tmp_path / "demo"))
    database = ProjectDatabase(workspace.database_path)
    missing_media_path = tmp_path / "missing-video.mp4"
    missing_output_path = workspace.cache_dir / "tts" / "missing.wav"
    database.insert_media_asset(
        MediaAssetRecord(
            asset_id="asset-missing",
            project_id=workspace.project_id,
            asset_type="video",
            path=str(missing_media_path),
            created_at="2026-03-20T00:00:00+00:00",
        )
    )
    database.insert_job_run(
        JobRunRecord(
            job_id="job-1",
            project_id=workspace.project_id,
            stage="tts",
            description="tts",
            status="success",
            started_at="2026-03-20T00:00:00+00:00",
            ended_at="2026-03-20T00:01:00+00:00",
            output_paths=[str(missing_output_path)],
            message="done",
        )
    )

    report = inspect_workspace(workspace)

    issue_codes = {issue.code for issue in report.issues}
    assert "missing_media_asset" in issue_codes
    assert "stale_job_outputs" in issue_codes


def test_repair_workspace_metadata_clears_stale_job_output_references(tmp_path: Path) -> None:
    workspace = bootstrap_project(ProjectInitRequest(name="Demo", root_dir=tmp_path / "demo"))
    database = ProjectDatabase(workspace.database_path)
    missing_output_path = workspace.cache_dir / "mix" / "missing.wav"
    existing_output_path = workspace.cache_dir / "mix" / "kept.wav"
    existing_output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_output_path.write_bytes(b"data")
    database.insert_job_run(
        JobRunRecord(
            job_id="job-2",
            project_id=workspace.project_id,
            stage="mixdown",
            description="mix",
            status="success",
            started_at="2026-03-20T00:00:00+00:00",
            ended_at="2026-03-20T00:01:00+00:00",
            output_paths=[str(existing_output_path), str(missing_output_path)],
            message="done",
        )
    )

    report = repair_workspace_metadata(workspace)
    repaired_row = next(row for row in database.list_job_runs() if row["job_id"] == "job-2")
    repaired_paths = json.loads(str(repaired_row["output_paths_json"]))

    assert any("Cleared stale job output refs" in item for item in report.fixed_items)
    assert repaired_paths == [str(existing_output_path)]
