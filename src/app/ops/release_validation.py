from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from app.ops.models import (
    CleanMachineProjectResult,
    CleanMachineValidationReport,
    ValidationKitManifest,
    ValidationProjectEntry,
    utc_now_iso,
)


def _slugify(value: str) -> str:
    raw = "".join(char.lower() if char.isalnum() else "-" for char in value.strip())
    while "--" in raw:
        raw = raw.replace("--", "-")
    return raw.strip("-") or "project"


def wait_for_path(path: Path, *, timeout_seconds: float = 10.0, poll_interval_seconds: float = 0.1) -> bool:
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    while time.monotonic() <= deadline:
        if path.exists():
            return True
        time.sleep(max(poll_interval_seconds, 0.01))
    return path.exists()


def _ensure_missing(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"Duong dan da ton tai: {path}")


def _write_clean_machine_smoke_script(*, script_path: Path, bundle_relative_path: Path) -> None:
    bundle_relative = str(bundle_relative_path).replace("/", "\\")
    bundle_name = bundle_relative_path.name
    script_content = f"""param(
    [string]$DoctorReportPath = "",
    [string]$ProjectRoot = "",
    [int]$WaitSeconds = 10,
    [string[]]$DoctorStages = @("preview", "tts", "voice_track", "mixdown", "export_video")
)

$ErrorActionPreference = "Stop"

$kitRoot = Split-Path -Parent $PSCommandPath
$bundleDir = Join-Path $kitRoot "{bundle_relative}"
$exePath = Join-Path $bundleDir "{bundle_name}.exe"
$resolvedDoctorReport = if ($DoctorReportPath) {{ [System.IO.Path]::GetFullPath($DoctorReportPath) }} else {{ Join-Path $kitRoot "reports\\bundle_doctor_report.json" }}

if (-not (Test-Path $bundleDir)) {{
    throw "Khong tim thay bundle tai $bundleDir"
}}
if (-not (Test-Path $exePath)) {{
    throw "Khong tim thay app binary tai $exePath"
}}

$dependencyChecks = @(
    @{{ Name = "ffmpeg"; Paths = @((Join-Path $bundleDir "dependencies\\ffmpeg\\ffmpeg.exe")) }},
    @{{ Name = "ffprobe"; Paths = @((Join-Path $bundleDir "dependencies\\ffmpeg\\ffprobe.exe")) }},
    @{{ Name = "mpv"; Paths = @((Join-Path $bundleDir "dependencies\\mpv\\mpv-2.dll"), (Join-Path $bundleDir "dependencies\\mpv\\libmpv-2.dll")) }},
    @{{ Name = "espeak"; Paths = @((Join-Path $bundleDir "dependencies\\espeak-ng")) }}
)

Write-Host "Bundle: $bundleDir"
Write-Host "Binary: $exePath"
foreach ($item in $dependencyChecks) {{
    $resolvedPath = $null
    foreach ($candidate in $item.Paths) {{
        if (Test-Path $candidate) {{
            $resolvedPath = $candidate
            break
        }}
    }}
    $status = if ($resolvedPath) {{ "OK" }} else {{ "Missing" }}
    $displayPath = if ($resolvedPath) {{ $resolvedPath }} else {{ ($item.Paths -join " | ") }}
    Write-Host ("- {{0}}: {{1}} ({{2}})" -f $item.Name, $status, $displayPath)
}}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedDoctorReport) | Out-Null
$doctorArgs = @("--doctor-report", $resolvedDoctorReport)
if ($ProjectRoot) {{
    $doctorArgs += @("--project-root", ([System.IO.Path]::GetFullPath($ProjectRoot)))
}}
if ($DoctorStages.Count -eq 1 -and $DoctorStages[0] -like "*,*") {{
    $DoctorStages = $DoctorStages[0].Split(",") | ForEach-Object {{ $_.Trim() }} | Where-Object {{ $_ }}
}}
if ($DoctorStages -and $DoctorStages.Count -gt 0) {{
    $doctorArgs += @("--doctor-stages") + $DoctorStages
}}

& $exePath @doctorArgs

for ($attempt = 0; $attempt -lt [Math]::Max($WaitSeconds * 10, 1); $attempt++) {{
    if (Test-Path $resolvedDoctorReport) {{
        break
    }}
    Start-Sleep -Milliseconds 100
}}

if (-not (Test-Path $resolvedDoctorReport)) {{
    throw "Bundle khong tao duoc doctor report tai $resolvedDoctorReport"
}}

$report = Get-Content -Path $resolvedDoctorReport -Raw | ConvertFrom-Json
Write-Host "Doctor report: $resolvedDoctorReport"
Write-Host ("- errors: {{0}}" -f $report.error_count)
Write-Host ("- warnings: {{0}}" -f $report.warning_count)

$requestedStageSet = @{{}}
foreach ($stage in $DoctorStages) {{
    if ($stage) {{
        $requestedStageSet[$stage.ToLowerInvariant()] = $true
    }}
}}
$blockingChecks = @()
foreach ($item in $report.checks) {{
    if ($item.status -ne "error") {{
        continue
    }}
    foreach ($stage in $item.blocking_stages) {{
        if ($requestedStageSet.ContainsKey($stage.ToLowerInvariant())) {{
            $blockingChecks += $item
            break
        }}
    }}
}}
if ($blockingChecks.Count -gt 0) {{
    $messages = $blockingChecks | ForEach-Object {{ "- $($_.name): $($_.message)" }}
    throw ("Bundle smoke bi block boi doctor:`n" + ($messages -join "`n"))
}}
"""
    script_path.write_text(script_content, encoding="utf-8")


def _write_instructions(
    *,
    instructions_path: Path,
    bundle_relative_path: Path,
    project_entries: list[ValidationProjectEntry],
    smoke_script_path: Path,
    report_template_path: Path,
) -> None:
    project_lines = "\n".join(
        f"- `{entry.label}`: `{entry.copied_path.relative_to(instructions_path.parent)}`" for entry in project_entries
    )
    instructions = f"""# Clean-Machine Validation Kit

Kit nay duoc tao de validate bundle PyInstaller tren Windows VM/may sach.

## Thanh phan

- bundle: `{bundle_relative_path}`
- smoke script: `{smoke_script_path.name}`
- report template: `{report_template_path.relative_to(instructions_path.parent)}`

Projects duoc dong kem:
{project_lines}

## Cac buoc tren may sach

1. Khong cai them Python hay copy DLL thu cong ngoai layout bundle.
2. Chay smoke bundle:
   - `powershell -ExecutionPolicy Bypass -File .\\{smoke_script_path.name}`
3. Mo app binary trong bundle.
4. Voi project ngan:
   - chay `Doctor`
   - kiem `Workspace safety`
   - preview subtitle mot lan
   - rerun `TTS -> Track giong -> Tron am thanh -> Xuat video`
5. Voi project dai:
   - chay `Doctor`
   - rerun `TTS -> Track giong -> Tron am thanh -> Xuat video`
6. Xac nhan:
   - backup duoc tao duoi `workspace\\.ops\\backups`
   - output video duoc tao
   - cache cleanup khong xoa artifact dang duoc tham chieu

## Artifact can giu lai

- `reports\\bundle_doctor_report.json`
- log/screenshot smoke bundle
- summary JSON cua rerun downstream trong moi project
- duong dan video dau ra
- report cuoi cung cap nhat tu template
"""
    instructions_path.write_text(instructions, encoding="utf-8")


def prepare_validation_kit(
    *,
    bundle_dir: Path,
    short_project_root: Path,
    long_project_root: Path,
    kit_root: Path,
) -> ValidationKitManifest:
    bundle_dir = bundle_dir.resolve()
    short_project_root = short_project_root.resolve()
    long_project_root = long_project_root.resolve()
    kit_root = kit_root.resolve()

    if not bundle_dir.exists():
        raise FileNotFoundError(f"Khong tim thay bundle: {bundle_dir}")
    if not short_project_root.exists():
        raise FileNotFoundError(f"Khong tim thay short project: {short_project_root}")
    if not long_project_root.exists():
        raise FileNotFoundError(f"Khong tim thay long project: {long_project_root}")

    _ensure_missing(kit_root)

    bundle_target = kit_root / "bundle" / bundle_dir.name
    short_target = kit_root / "projects" / f"short-{_slugify(short_project_root.name)}"
    long_target = kit_root / "projects" / f"long-{_slugify(long_project_root.name)}"
    reports_dir = kit_root / "reports"
    logs_dir = kit_root / "logs"

    bundle_target.parent.mkdir(parents=True, exist_ok=True)
    short_target.parent.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    shutil.copytree(bundle_dir, bundle_target)
    shutil.copytree(short_project_root, short_target)
    shutil.copytree(long_project_root, long_target)

    project_entries = [
        ValidationProjectEntry(label="short", source_path=short_project_root, copied_path=short_target),
        ValidationProjectEntry(label="long", source_path=long_project_root, copied_path=long_target),
    ]
    smoke_script_path = kit_root / "run_bundle_smoke.ps1"
    report_template_path = reports_dir / "clean_machine_validation_report.template.json"
    instructions_path = kit_root / "CLEAN_MACHINE_README.md"
    local_smoke_log_path = logs_dir / "local_smoke_bundle.log"
    local_doctor_report_path = reports_dir / "local_bundle_doctor_report.json"

    _write_clean_machine_smoke_script(
        script_path=smoke_script_path,
        bundle_relative_path=bundle_target.relative_to(kit_root),
    )
    _write_instructions(
        instructions_path=instructions_path,
        bundle_relative_path=bundle_target.relative_to(kit_root),
        project_entries=project_entries,
        smoke_script_path=smoke_script_path,
        report_template_path=report_template_path,
    )

    template_report = CleanMachineValidationReport(
        generated_at=utc_now_iso(),
        kit_root=kit_root,
        machine_label="",
        windows_version="",
        bundle_dir=bundle_target,
        bundle_doctor_report_path=local_doctor_report_path,
        bundle_smoke_log_path=local_smoke_log_path,
        bundle_smoke_passed=None,
        preview_project_label="short",
        preview_passed=None,
        project_results=[
            CleanMachineProjectResult(label=entry.label, project_path=entry.copied_path)
            for entry in project_entries
        ],
        blockers=[],
        notes=[
            "Cap nhat machine_label va windows_version sau khi chay tren may sach.",
            "Dien duong dan rerun summary/output video thuc te cho tung project.",
        ],
    )
    report_template_path.write_text(
        json.dumps(template_report.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    manifest = ValidationKitManifest(
        created_at=utc_now_iso(),
        kit_root=kit_root,
        bundle_dir=bundle_target,
        smoke_script_path=smoke_script_path,
        instructions_path=instructions_path,
        report_template_path=report_template_path,
        local_smoke_log_path=local_smoke_log_path,
        local_doctor_report_path=local_doctor_report_path,
        projects=project_entries,
    )
    (kit_root / "validation_kit_manifest.json").write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return manifest


def build_clean_machine_validation_report(
    *,
    manifest: ValidationKitManifest,
    machine_label: str,
    windows_version: str,
    bundle_smoke_passed: bool,
    preview_passed: bool,
    short_project_summary_path: Path | None,
    long_project_summary_path: Path | None,
    short_output_video_path: Path | None = None,
    long_output_video_path: Path | None = None,
    bundle_doctor_report_path: Path | None = None,
    bundle_smoke_log_path: Path | None = None,
    blockers: list[str] | None = None,
    notes: list[str] | None = None,
) -> CleanMachineValidationReport:
    project_results: list[CleanMachineProjectResult] = []
    for entry in manifest.projects:
        if entry.label == "short":
            summary_path = short_project_summary_path
            output_video_path = short_output_video_path
        else:
            summary_path = long_project_summary_path
            output_video_path = long_output_video_path
        rerun_passed = summary_path is not None and Path(summary_path).exists()
        project_results.append(
            CleanMachineProjectResult(
                label=entry.label,
                project_path=entry.copied_path,
                rerun_summary_path=Path(summary_path) if summary_path else None,
                output_video_path=Path(output_video_path) if output_video_path else None,
                rerun_passed=rerun_passed,
            )
        )
    return CleanMachineValidationReport(
        generated_at=utc_now_iso(),
        kit_root=manifest.kit_root,
        machine_label=machine_label,
        windows_version=windows_version,
        bundle_dir=manifest.bundle_dir,
        bundle_doctor_report_path=Path(bundle_doctor_report_path) if bundle_doctor_report_path else None,
        bundle_smoke_log_path=Path(bundle_smoke_log_path) if bundle_smoke_log_path else None,
        bundle_smoke_passed=bundle_smoke_passed,
        preview_project_label="short",
        preview_passed=preview_passed,
        project_results=project_results,
        blockers=list(blockers or []),
        notes=list(notes or []),
    )
