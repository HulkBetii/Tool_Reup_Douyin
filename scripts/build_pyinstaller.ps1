param(
    [string]$PythonExe = ".\.venv\Scripts\python.exe",
    [string]$DistRoot = "",
    [string]$WorkRoot = "",
    [string]$FfmpegBinDir = "",
    [string]$MpvDllPath = "",
    [string]$EspeakRoot = "",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$specPath = Join-Path $repoRoot "build\pyinstaller.spec"
$resolvedDistRoot = if ($DistRoot) { [System.IO.Path]::GetFullPath($DistRoot) } else { Join-Path $repoRoot "dist" }
$resolvedWorkRoot = if ($WorkRoot) { [System.IO.Path]::GetFullPath($WorkRoot) } else { Join-Path $repoRoot "build\pyinstaller-work" }
$settingsPath = Join-Path $env:APPDATA "ReupVideo\settings.json"

function Get-VersionValue {
    param([string]$Name)
    $versionFile = Join-Path $repoRoot "src\app\version.py"
    $match = Select-String -Path $versionFile -Pattern "^$Name = ""(?<value>.+)""$"
    if (-not $match) {
        throw "Khong doc duoc $Name tu $versionFile"
    }
    return $match.Matches[0].Groups["value"].Value
}

function Get-OptionalSettings {
    if (-not (Test-Path $settingsPath)) {
        return $null
    }
    try {
        return Get-Content -Path $settingsPath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Copy-DependencyDirectory {
    param(
        [string]$SourceDir,
        [string]$RelativeDestination
    )
    if (-not $SourceDir) {
        return
    }
    if (-not (Test-Path $SourceDir)) {
        throw "Khong tim thay dependency directory: $SourceDir"
    }
    $destination = Join-Path $bundleRoot $RelativeDestination
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    Copy-Item -Path (Join-Path $SourceDir "*") -Destination $destination -Recurse -Force
}

function Copy-DependencyFile {
    param(
        [string]$SourceFile,
        [string]$RelativeDestination
    )
    if (-not $SourceFile) {
        return
    }
    if (-not (Test-Path $SourceFile)) {
        throw "Khong tim thay dependency file: $SourceFile"
    }
    $destinationDir = Join-Path $bundleRoot $RelativeDestination
    New-Item -ItemType Directory -Force -Path $destinationDir | Out-Null
    Copy-Item -Path $SourceFile -Destination (Join-Path $destinationDir (Split-Path $SourceFile -Leaf)) -Force
}

$appSlug = Get-VersionValue "APP_SLUG"
$pythonPath = if ([System.IO.Path]::IsPathRooted($PythonExe)) { $PythonExe } else { Join-Path $repoRoot $PythonExe }
$settings = Get-OptionalSettings

if (-not (Test-Path $pythonPath)) {
    throw "Khong tim thay Python executable tai $pythonPath"
}

if (-not $FfmpegBinDir -and $settings -and $settings.dependency_paths.ffmpeg_path) {
    $candidateDir = Split-Path $settings.dependency_paths.ffmpeg_path -Parent
    if (Test-Path (Join-Path $candidateDir "ffmpeg.exe") -and Test-Path (Join-Path $candidateDir "ffprobe.exe")) {
        $FfmpegBinDir = $candidateDir
    }
}
if (-not $MpvDllPath -and $settings -and $settings.dependency_paths.mpv_dll_path -and (Test-Path $settings.dependency_paths.mpv_dll_path)) {
    $MpvDllPath = $settings.dependency_paths.mpv_dll_path
}
if (-not $EspeakRoot) {
    $defaultEspeakRoot = Join-Path $env:LOCALAPPDATA "Programs\eSpeak NG"
    if (Test-Path $defaultEspeakRoot) {
        $EspeakRoot = $defaultEspeakRoot
    }
}

if ($Clean) {
    Remove-Item -Path $resolvedDistRoot -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $resolvedWorkRoot -Recurse -Force -ErrorAction SilentlyContinue
}

New-Item -ItemType Directory -Force -Path $resolvedDistRoot | Out-Null
New-Item -ItemType Directory -Force -Path $resolvedWorkRoot | Out-Null

$pyinstallerArgs = @(
    "-m",
    "PyInstaller",
    "--noconfirm",
    "--distpath", $resolvedDistRoot,
    "--workpath", $resolvedWorkRoot,
    $specPath
)
if ($Clean) {
    $pyinstallerArgs = @("-m", "PyInstaller", "--noconfirm", "--clean", "--distpath", $resolvedDistRoot, "--workpath", $resolvedWorkRoot, $specPath)
}

$env:REUP_VIDEO_ROOT = $repoRoot
& $pythonPath @pyinstallerArgs

$bundleRoot = Join-Path $resolvedDistRoot $appSlug
if (-not (Test-Path $bundleRoot)) {
    throw "PyInstaller khong tao ra bundle tai $bundleRoot"
}

Copy-DependencyDirectory -SourceDir $FfmpegBinDir -RelativeDestination "dependencies\ffmpeg"
Copy-DependencyFile -SourceFile $MpvDllPath -RelativeDestination "dependencies\mpv"
Copy-DependencyDirectory -SourceDir $EspeakRoot -RelativeDestination "dependencies\espeak-ng"

Write-Host "PyInstaller bundle san sang tai: $bundleRoot"
