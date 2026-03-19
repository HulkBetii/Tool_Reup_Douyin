param(
    [string]$PythonExe = ".\.venv\Scripts\python.exe",
    [string]$BundleDir = "",
    [string]$OutputDir = "",
    [string]$IsccPath = "",
    [string]$FfmpegBinDir = "",
    [string]$MpvDllPath = "",
    [string]$EspeakRoot = "",
    [switch]$BuildBundle,
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$issPath = Join-Path $repoRoot "installer\app.iss"

function Get-VersionValue {
    param([string]$Name)
    $versionFile = Join-Path $repoRoot "src\app\version.py"
    $match = Select-String -Path $versionFile -Pattern "^$Name = ""(?<value>.+)""$"
    if (-not $match) {
        throw "Khong doc duoc $Name tu $versionFile"
    }
    return $match.Matches[0].Groups["value"].Value
}

function Resolve-IsccPath {
    param([string]$ConfiguredPath)
    if ($ConfiguredPath) {
        if (-not (Test-Path $ConfiguredPath)) {
            throw "Khong tim thay ISCC.exe tai $ConfiguredPath"
        }
        return $ConfiguredPath
    }
    $candidates = @(
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        "C:\Program Files\Inno Setup 6\ISCC.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    throw "Khong tim thay ISCC.exe. Hay cai Inno Setup 6 hoac truyen -IsccPath."
}

$appName = Get-VersionValue "APP_NAME"
$appSlug = Get-VersionValue "APP_SLUG"
$appVersion = Get-VersionValue "APP_VERSION"
$resolvedBundleDir = if ($BundleDir) { [System.IO.Path]::GetFullPath($BundleDir) } else { Join-Path $repoRoot "dist\$appSlug" }
$resolvedOutputDir = if ($OutputDir) { [System.IO.Path]::GetFullPath($OutputDir) } else { Join-Path $repoRoot "dist\installer" }

if ($BuildBundle -or -not (Test-Path $resolvedBundleDir)) {
    & (Join-Path $repoRoot "scripts\build_pyinstaller.ps1") `
        -PythonExe $PythonExe `
        -DistRoot (Join-Path $repoRoot "dist") `
        -FfmpegBinDir $FfmpegBinDir `
        -MpvDllPath $MpvDllPath `
        -EspeakRoot $EspeakRoot `
        -Clean:$Clean
}

New-Item -ItemType Directory -Force -Path $resolvedOutputDir | Out-Null
$resolvedIsccPath = Resolve-IsccPath -ConfiguredPath $IsccPath

& $resolvedIsccPath `
    "/DAppName=$appName" `
    "/DAppSlug=$appSlug" `
    "/DAppVersion=$appVersion" `
    "/DAppExeName=$appSlug.exe" `
    "/DSourceDir=$resolvedBundleDir" `
    "/DOutputDir=$resolvedOutputDir" `
    $issPath

Write-Host "Installer san sang tai: $resolvedOutputDir"
