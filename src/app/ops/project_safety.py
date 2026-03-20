from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.ops.models import BackupManifest, WorkspaceRepairIssue, WorkspaceRepairReport, utc_now_iso
from app.project.database import ProjectDatabase
from app.project.runtime_state import restore_pipeline_state

OPS_DIRNAME = ".ops"
BACKUPS_DIRNAME = "backups"
REPORTS_DIRNAME = "reports"

_REQUIRED_TABLES = {
    "metadata",
    "projects",
    "media_assets",
    "segments",
    "subtitle_tracks",
    "subtitle_events",
    "job_runs",
}


def get_ops_root(workspace_root: Path) -> Path:
    return workspace_root / OPS_DIRNAME


def get_backups_root(workspace_root: Path) -> Path:
    return get_ops_root(workspace_root) / BACKUPS_DIRNAME


def ensure_ops_layout(workspace_root: Path) -> Path:
    ops_root = get_ops_root(workspace_root)
    (ops_root / BACKUPS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (ops_root / REPORTS_DIRNAME).mkdir(parents=True, exist_ok=True)
    return ops_root


def _sanitize_label(value: str) -> str:
    raw = "".join(char if char.isalnum() else "-" for char in value.lower())
    while "--" in raw:
        raw = raw.replace("--", "-")
    return raw.strip("-") or "manual"


def create_workspace_backup(
    workspace,
    *,
    reason: str,
    stage: str,
) -> BackupManifest:
    ensure_ops_layout(workspace.root_dir)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = get_backups_root(workspace.root_dir) / f"{timestamp}-{_sanitize_label(stage)}"
    backup_dir.mkdir(parents=True, exist_ok=False)

    copied_files: list[Path] = []
    for source_path in (workspace.database_path, workspace.project_json_path):
        if source_path.exists():
            target_path = backup_dir / source_path.name
            shutil.copy2(source_path, target_path)
            copied_files.append(target_path)

    manifest = BackupManifest(
        created_at=utc_now_iso(),
        workspace_root=workspace.root_dir,
        backup_dir=backup_dir,
        reason=reason,
        stage=stage,
        project_id=getattr(workspace, "project_id", None),
        copied_files=copied_files,
    )
    (backup_dir / "manifest.json").write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return manifest


def _parse_output_paths(raw_value: object) -> list[Path]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        values = raw_value
    else:
        try:
            values = json.loads(str(raw_value))
        except json.JSONDecodeError:
            return []
    if not isinstance(values, list):
        return []
    return [Path(str(value)) for value in values]


def _existing_tables(database_path: Path) -> set[str]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def inspect_workspace(workspace) -> WorkspaceRepairReport:
    ensure_ops_layout(workspace.root_dir)
    report = WorkspaceRepairReport(
        generated_at=utc_now_iso(),
        workspace_root=workspace.root_dir,
        checked_file_count=0,
        checked_job_count=0,
    )

    if not workspace.project_json_path.exists():
        report.issues.append(
            WorkspaceRepairIssue(
                severity="error",
                code="missing_project_json",
                message="Khong tim thay project.json",
                path=workspace.project_json_path,
            )
        )
    else:
        report.checked_file_count += 1

    if not workspace.database_path.exists():
        report.issues.append(
            WorkspaceRepairIssue(
                severity="error",
                code="missing_project_db",
                message="Khong tim thay project.db",
                path=workspace.database_path,
            )
        )
        report.schema_ok = False
        return report

    report.checked_file_count += 1
    database = ProjectDatabase(workspace.database_path)
    database.initialize()

    missing_tables = sorted(_REQUIRED_TABLES - _existing_tables(workspace.database_path))
    if missing_tables:
        report.schema_ok = False
        report.issues.append(
            WorkspaceRepairIssue(
                severity="error",
                code="missing_tables",
                message="CSDL thieu bang can thiet",
                path=workspace.database_path,
                detail_json={"missing_tables": missing_tables},
            )
        )

    project_row = database.get_project()
    if project_row is None:
        report.issues.append(
            WorkspaceRepairIssue(
                severity="error",
                code="missing_project_row",
                message="Khong tim thay project row trong CSDL",
                path=workspace.database_path,
            )
        )

    with database.connect() as connection:
        media_rows = connection.execute(
            "SELECT asset_id, type, path FROM media_assets WHERE project_id = ? ORDER BY asset_id",
            (workspace.project_id,),
        ).fetchall()

    for row in media_rows:
        candidate = Path(str(row["path"]))
        report.checked_file_count += 1
        if not candidate.exists():
            report.issues.append(
                WorkspaceRepairIssue(
                    severity="warning",
                    code="missing_media_asset",
                    message=f"Media asset khong con ton tai: {candidate}",
                    path=candidate,
                    detail_json={"asset_id": row["asset_id"], "asset_type": row["type"]},
                )
            )

    job_rows = database.list_job_runs()
    report.checked_job_count = len(job_rows)
    for row in job_rows:
        output_paths = _parse_output_paths(row["output_paths_json"])
        missing_paths = [path for path in output_paths if not path.exists()]
        report.checked_file_count += len(output_paths)
        if not missing_paths:
            continue
        report.issues.append(
            WorkspaceRepairIssue(
                severity="warning",
                code="stale_job_outputs",
                message=f"Job {row['job_id']} dang tham chieu artifact da mat",
                detail_json={
                    "job_id": row["job_id"],
                    "stage": row["stage"],
                    "missing_paths": [str(path) for path in missing_paths],
                },
            )
        )

    restored_state = restore_pipeline_state(job_rows)
    downstream_stage = None
    if restored_state.tts_manifest_path is None:
        downstream_stage = "tts"
    elif restored_state.voice_track_path is None:
        downstream_stage = "voice_track"
    elif restored_state.mixed_audio_path is None:
        downstream_stage = "mixdown"
    elif restored_state.export_output_path is None:
        downstream_stage = "export_video"
    if downstream_stage:
        report.issues.append(
            WorkspaceRepairIssue(
                severity="warning",
                code="safe_rerun_hint",
                message=f"Neu can rerun downstream, stage som nhat can chay lai: {downstream_stage}",
                detail_json={"suggested_stage": downstream_stage},
            )
        )

    return report


def repair_workspace_metadata(workspace) -> WorkspaceRepairReport:
    report = inspect_workspace(workspace)
    database = ProjectDatabase(workspace.database_path)
    for issue in report.issues:
        if issue.code != "stale_job_outputs":
            continue
        job_id = str(issue.detail_json.get("job_id", "")).strip()
        if not job_id:
            continue
        with database.connect() as connection:
            row = connection.execute("SELECT * FROM job_runs WHERE job_id = ? LIMIT 1", (job_id,)).fetchone()
        if row is None:
            continue
        filtered_paths = [str(path) for path in _parse_output_paths(row["output_paths_json"]) if path.exists()]
        error_json = None
        if row["error_json"]:
            try:
                error_json = json.loads(str(row["error_json"]))
            except json.JSONDecodeError:
                error_json = None
        database.update_job_run(
            job_id,
            status=str(row["status"]),
            progress=int(row["progress"] or 0),
            message=str(row["message"] or "Workspace repair da cap nhat artifact refs"),
            log_path=str(row["log_path"]) if row["log_path"] else None,
            ended_at=str(row["ended_at"]) if row["ended_at"] else None,
            output_paths=filtered_paths,
            error_json=error_json,
        )
        report.fixed_items.append(f"Cleared stale job output refs for {job_id}")
    return report
