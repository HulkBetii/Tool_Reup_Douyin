param(
    [string]$BundleDir = "",
    [string]$DoctorReportPath = "",
    [string]$ProjectRoot = "",
    [int]$WaitSeconds = 10,
    [string[]]$DoctorStages = @("preview", "tts", "voice_track", "mixdown", "export_video")
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
    @{ Name = "ffmpeg"; Paths = @((Join-Path $resolvedBundleDir "dependencies\ffmpeg\ffmpeg.exe")) },
    @{ Name = "ffprobe"; Paths = @((Join-Path $resolvedBundleDir "dependencies\ffmpeg\ffprobe.exe")) },
    @{ Name = "mpv"; Paths = @((Join-Path $resolvedBundleDir "dependencies\mpv\mpv-2.dll"), (Join-Path $resolvedBundleDir "dependencies\mpv\libmpv-2.dll")) },
    @{ Name = "espeak"; Paths = @((Join-Path $resolvedBundleDir "dependencies\espeak-ng")) }
)

Write-Host "Bundle: $resolvedBundleDir"
Write-Host "Binary: $exePath"
foreach ($item in $dependencyChecks) {
    $resolvedPath = $null
    foreach ($candidate in $item.Paths) {
        if (Test-Path $candidate) {
            $resolvedPath = $candidate
            break
        }
    }
    $status = if ($resolvedPath) { "OK" } else { "Missing" }
    $displayPath = if ($resolvedPath) { $resolvedPath } else { ($item.Paths -join " | ") }
    Write-Host ("- {0}: {1} ({2})" -f $item.Name, $status, $displayPath)
}

$doctorArgs = @("--doctor-report", $resolvedDoctorReport)
if ($ProjectRoot) {
    $doctorArgs += @("--project-root", ([System.IO.Path]::GetFullPath($ProjectRoot)))
}
if ($DoctorStages.Count -eq 1 -and $DoctorStages[0] -like "*,*") {
    $DoctorStages = $DoctorStages[0].Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}
if ($DoctorStages -and $DoctorStages.Count -gt 0) {
    $doctorArgs += @("--doctor-stages") + $DoctorStages
}

& $exePath @doctorArgs

for ($attempt = 0; $attempt -lt [Math]::Max($WaitSeconds * 10, 1); $attempt++) {
    if (Test-Path $resolvedDoctorReport) {
        break
    }
    Start-Sleep -Milliseconds 100
}

if (-not (Test-Path $resolvedDoctorReport)) {
    throw "Bundle khong tao duoc doctor report tai $resolvedDoctorReport"
}

$report = Get-Content -Path $resolvedDoctorReport -Raw | ConvertFrom-Json
Write-Host "Doctor report: $resolvedDoctorReport"
Write-Host ("- errors: {0}" -f $report.error_count)
Write-Host ("- warnings: {0}" -f $report.warning_count)

$requestedStageSet = @{}
foreach ($stage in $DoctorStages) {
    if ($stage) {
        $requestedStageSet[$stage.ToLowerInvariant()] = $true
    }
}
$blockingChecks = @()
foreach ($item in $report.checks) {
    if ($item.status -ne "error") {
        continue
    }
    foreach ($stage in $item.blocking_stages) {
        if ($requestedStageSet.ContainsKey($stage.ToLowerInvariant())) {
            $blockingChecks += $item
            break
        }
    }
}
if ($blockingChecks.Count -gt 0) {
    $messages = $blockingChecks | ForEach-Object { "- $($_.name): $($_.message)" }
    throw ("Bundle smoke bi block boi doctor:`n" + ($messages -join "`n"))
}
