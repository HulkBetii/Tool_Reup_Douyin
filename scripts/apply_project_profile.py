from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.project.bootstrap import open_project, sync_project_snapshot
from app.project.database import ProjectDatabase
from app.project.profiles import apply_project_profile

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply a reusable project profile to an existing project.")
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--project-profile-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    workspace = open_project(args.project_root.expanduser().resolve())
    database = ProjectDatabase(workspace.database_path)
    state = apply_project_profile(
        workspace.root_dir,
        project_id=workspace.project_id,
        database=database,
        project_profile_id=args.project_profile_id,
        applied_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    sync_project_snapshot(workspace)
    summary = {
        "project_root": str(workspace.root_dir),
        "project_profile_id": state.project_profile_id,
        "recommended_prompt_template_id": state.recommended_prompt_template_id,
        "active_voice_preset_id": state.active_voice_preset_id,
        "active_export_preset_id": state.active_export_preset_id,
        "active_watermark_profile_id": state.active_watermark_profile_id,
        "recommended_original_volume": state.recommended_original_volume,
        "recommended_voice_volume": state.recommended_voice_volume,
    }
    summary_path = workspace.root_dir / "apply_project_profile_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Summary: {summary_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
