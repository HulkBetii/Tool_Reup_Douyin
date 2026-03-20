param(
    [string]$BundleDir = "",
    [string]$DoctorReportPath = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$versionFile = Join-Path $repoRoot "src\app\version.py"

function Get-VersionValue {
    param([string]$Name)
    $match = Select-String -Path $versionFile -Pattern "^$Name = ""(?<value>.+)""$"
    if (-not $match) {
        throw "Khong doc duoc $Name tu $versionFile"
    }
    return $match.Matches[0].Groups["value"].Value
}

$appSlug = Get-VersionValue "APP_SLUG"
$resolvedBundleDir = if ($BundleDir) { [System.IO.Path]::GetFullPath($BundleDir) } else { Join-Path $repoRoot "dist\$appSlug" }
$resolvedDoctorReport = if ($DoctorReportPath) { [System.IO.Path]::GetFullPath($DoctorReportPath) } else { Join-Path $resolvedBundleDir "doctor-report.json" }
$exePath = Join-Path $resolvedBundleDir "$appSlug.exe"

if (-not (Test-Path $resolvedBundleDir)) {
    throw "Khong tim thay bundle tai $resolvedBundleDir"
}
if (-not (Test-Path $exePath)) {
    throw "Khong tim thay app binary tai $exePath"
}

$dependencyChecks = @(
    @{ Name = "ffmpeg"; Path = (Join-Path $resolvedBundleDir "dependencies\ffmpeg\ffmpeg.exe") },
    @{ Name = "ffprobe"; Path = (Join-Path $resolvedBundleDir "dependencies\ffmpeg\ffprobe.exe") },
    @{ Name = "mpv"; Path = (Join-Path $resolvedBundleDir "dependencies\mpv\mpv-2.dll") },
    @{ Name = "espeak"; Path = (Join-Path $resolvedBundleDir "dependencies\espeak-ng") }
)

Write-Host "Bundle: $resolvedBundleDir"
Write-Host "Binary: $exePath"
foreach ($item in $dependencyChecks) {
    $status = if (Test-Path $item.Path) { "OK" } else { "Missing" }
    Write-Host ("- {0}: {1} ({2})" -f $item.Name, $status, $item.Path)
}

$doctorArgs = @("--doctor-report", $resolvedDoctorReport)
if ($ProjectRoot) {
    $doctorArgs += @("--project-root", ([System.IO.Path]::GetFullPath($ProjectRoot)))
}

& $exePath @doctorArgs

if (-not (Test-Path $resolvedDoctorReport)) {
    throw "Bundle khong tao duoc doctor report tai $resolvedDoctorReport"
}

$report = Get-Content -Path $resolvedDoctorReport -Raw | ConvertFrom-Json
Write-Host "Doctor report: $resolvedDoctorReport"
Write-Host ("- errors: {0}" -f $report.error_count)
Write-Host ("- warnings: {0}" -f $report.warning_count)
