from __future__ import annotations
# ruff: noqa: E402

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from app.ops.models import ValidationKitManifest, ValidationProjectEntry
from app.ops.release_validation import build_clean_machine_validation_report

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize a clean-machine release validation report.")
    parser.add_argument("--kit-root", required=True, type=Path)
    parser.add_argument("--machine-label", required=True)
    parser.add_argument("--windows-version", required=True)
    parser.add_argument("--bundle-smoke-passed", action="store_true")
    parser.add_argument("--preview-passed", action="store_true")
    parser.add_argument("--short-project-summary", type=Path, default=None)
    parser.add_argument("--long-project-summary", type=Path, default=None)
    parser.add_argument("--short-output-video", type=Path, default=None)
    parser.add_argument("--long-output-video", type=Path, default=None)
    parser.add_argument("--bundle-doctor-report", type=Path, default=None)
    parser.add_argument("--bundle-smoke-log", type=Path, default=None)
    parser.add_argument("--blocker", action="append", default=[])
    parser.add_argument("--note", action="append", default=[])
    return parser.parse_args()


def _load_manifest(kit_root: Path) -> ValidationKitManifest:
    payload = json.loads((kit_root / "validation_kit_manifest.json").read_text(encoding="utf-8"))
    return ValidationKitManifest(
        created_at=str(payload["created_at"]),
        kit_root=Path(str(payload["kit_root"])),
        bundle_dir=Path(str(payload["bundle_dir"])),
        smoke_script_path=Path(str(payload["smoke_script_path"])),
        instructions_path=Path(str(payload["instructions_path"])),
        report_template_path=Path(str(payload["report_template_path"])),
        local_smoke_log_path=Path(str(payload["local_smoke_log_path"])),
        local_doctor_report_path=Path(str(payload["local_doctor_report_path"])),
        projects=[
            ValidationProjectEntry(
                label=str(item["label"]),
                source_path=Path(str(item["source_path"])),
                copied_path=Path(str(item["copied_path"])),
            )
            for item in payload["projects"]
        ],
    )


def _write_markdown_report(report_path: Path, report_payload: dict[str, object]) -> None:
    project_lines = []
    for item in report_payload["project_results"]:
        project_lines.append(
            f"- `{item['label']}`: rerun_passed=`{item['rerun_passed']}` | "
            f"summary=`{item['rerun_summary_path'] or ''}` | output=`{item['output_video_path'] or ''}`"
        )
    blockers = "\n".join(f"- {value}" for value in report_payload["blockers"]) or "- none"
    notes = "\n".join(f"- {value}" for value in report_payload["notes"]) or "- none"
    markdown = f"""# Clean-Machine Validation Report

- machine: `{report_payload['machine_label']}`
- windows_version: `{report_payload['windows_version']}`
- bundle_dir: `{report_payload['bundle_dir']}`
- bundle_smoke_passed: `{report_payload['bundle_smoke_passed']}`
- preview_passed: `{report_payload['preview_passed']}`
- bundle_doctor_report: `{report_payload['bundle_doctor_report_path'] or ''}`
- bundle_smoke_log: `{report_payload['bundle_smoke_log_path'] or ''}`

## Projects

{chr(10).join(project_lines)}

## Blockers

{blockers}

## Notes

{notes}
"""
    report_path.write_text(markdown, encoding="utf-8")


def main() -> int:
    args = _parse_args()
    kit_root = args.kit_root.resolve()
    manifest = _load_manifest(kit_root)
    report = build_clean_machine_validation_report(
        manifest=manifest,
        machine_label=args.machine_label,
        windows_version=args.windows_version,
        bundle_smoke_passed=bool(args.bundle_smoke_passed),
        preview_passed=bool(args.preview_passed),
        short_project_summary_path=args.short_project_summary.resolve() if args.short_project_summary else None,
        long_project_summary_path=args.long_project_summary.resolve() if args.long_project_summary else None,
        short_output_video_path=args.short_output_video.resolve() if args.short_output_video else None,
        long_output_video_path=args.long_output_video.resolve() if args.long_output_video else None,
        bundle_doctor_report_path=args.bundle_doctor_report.resolve() if args.bundle_doctor_report else None,
        bundle_smoke_log_path=args.bundle_smoke_log.resolve() if args.bundle_smoke_log else None,
        blockers=list(args.blocker),
        notes=list(args.note),
    )
    json_path = kit_root / "reports" / "clean_machine_validation_report.json"
    markdown_path = kit_root / "reports" / "clean_machine_validation_report.md"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    _write_markdown_report(markdown_path, report.to_dict())
    print(f"Validation report JSON: {json_path}")
    print(f"Validation report Markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
