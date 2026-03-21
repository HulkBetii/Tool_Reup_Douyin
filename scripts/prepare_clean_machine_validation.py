from __future__ import annotations
# ruff: noqa: E402

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from app.ops.release_validation import prepare_validation_kit
from app.version import APP_SLUG

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _run_powershell(script_path: Path, arguments: list[str], *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    command = [
        "powershell.exe",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
        *arguments,
    ]
    environment = dict(os.environ)
    if extra_env:
        environment.update(extra_env)
    return subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _default_kit_root() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return REPO_ROOT / "workspace" / f"clean-machine-validation-kit-{timestamp}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a clean-machine validation kit for the Windows bundle.")
    parser.add_argument("--short-project-root", required=True, type=Path)
    parser.add_argument("--long-project-root", required=True, type=Path)
    parser.add_argument("--kit-root", type=Path, default=None)
    parser.add_argument("--bundle-dir", type=Path, default=None)
    parser.add_argument("--python-exe", type=Path, default=Path(sys.executable))
    parser.add_argument("--clean-build", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-local-smoke", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    kit_root = args.kit_root.resolve() if args.kit_root else _default_kit_root()

    bundle_dir = args.bundle_dir.resolve() if args.bundle_dir else REPO_ROOT / "dist" / APP_SLUG
    if not args.skip_build:
        build_args = [
            "-PythonExe",
            str(args.python_exe.resolve()),
        ]
        if args.clean_build:
            build_args.append("-Clean")
        build_result = _run_powershell(REPO_ROOT / "scripts" / "build_pyinstaller.ps1", build_args)
        if build_result.returncode != 0:
            sys.stderr.write(build_result.stdout)
            sys.stderr.write(build_result.stderr)
            raise RuntimeError("Build bundle that bai")

    manifest = prepare_validation_kit(
        bundle_dir=bundle_dir,
        short_project_root=args.short_project_root,
        long_project_root=args.long_project_root,
        kit_root=kit_root,
    )

    smoke_payload = {
        "bundle_dir": str(manifest.bundle_dir),
        "doctor_report": str(manifest.local_doctor_report_path),
        "status": "skipped",
    }
    if not args.skip_local_smoke:
        smoke_env = {
            "APPDATA": str((manifest.kit_root / "sandbox_appdata").resolve()),
            "LOCALAPPDATA": str((manifest.kit_root / "sandbox_localappdata").resolve()),
        }
        smoke_result = _run_powershell(
            REPO_ROOT / "scripts" / "smoke_release_bundle.ps1",
            [
                "-BundleDir",
                str(bundle_dir),
                "-DoctorReportPath",
                str(manifest.local_doctor_report_path),
                "-ProjectRoot",
                str(args.short_project_root.resolve()),
                "-DoctorStages",
                "preview,tts,voice_track,mixdown,export_video",
            ],
            extra_env=smoke_env,
        )
        manifest.local_smoke_log_path.write_text(
            smoke_result.stdout + ("\n" if smoke_result.stdout and smoke_result.stderr else "") + smoke_result.stderr,
            encoding="utf-8",
        )
        smoke_payload["status"] = "passed" if smoke_result.returncode == 0 else "failed"
        smoke_payload["log_path"] = str(manifest.local_smoke_log_path)
        smoke_payload["appdata_root"] = smoke_env["APPDATA"]
        smoke_payload["localappdata_root"] = smoke_env["LOCALAPPDATA"]
        if smoke_result.returncode != 0:
            raise RuntimeError("Smoke bundle cuc bo that bai")
    else:
        manifest.local_smoke_log_path.write_text("Local smoke skipped.\n", encoding="utf-8")

    summary = {
        "kit_root": str(manifest.kit_root),
        "bundle_dir": str(manifest.bundle_dir),
        "manifest_path": str(manifest.kit_root / "validation_kit_manifest.json"),
        "instructions_path": str(manifest.instructions_path),
        "report_template_path": str(manifest.report_template_path),
        "local_smoke": smoke_payload,
        "projects": [entry.to_dict() for entry in manifest.projects],
    }
    summary_path = manifest.kit_root / "prepare_clean_machine_validation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Validation kit: {manifest.kit_root}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
