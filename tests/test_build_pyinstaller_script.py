from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_build_pyinstaller_dry_run_uses_settings_ffmpeg_paths(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "build_pyinstaller.ps1"
    appdata_dir = tmp_path / "appdata" / "ReupVideo"
    ffmpeg_dir = tmp_path / "deps" / "ffmpeg"
    appdata_dir.mkdir(parents=True, exist_ok=True)
    ffmpeg_dir.mkdir(parents=True, exist_ok=True)
    (ffmpeg_dir / "ffmpeg.exe").write_text("", encoding="utf-8")
    (ffmpeg_dir / "ffprobe.exe").write_text("", encoding="utf-8")
    (appdata_dir / "settings.json").write_text(
        json.dumps(
            {
                "dependency_paths": {
                    "ffmpeg_path": str(ffmpeg_dir / "ffmpeg.exe"),
                }
            }
        ),
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["APPDATA"] = str(tmp_path / "appdata")
    env["LOCALAPPDATA"] = str(tmp_path / "localappdata")

    result = subprocess.run(
        [
            "powershell.exe",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
            "-PythonExe",
            sys.executable,
            "-DryRun",
        ],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "Dry run: build prerequisites resolved" in result.stdout
    assert str(ffmpeg_dir) in result.stdout
