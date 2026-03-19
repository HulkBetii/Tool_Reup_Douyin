from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass

from app.core.paths import get_bundled_dependency_path
from app.core.settings import AppSettings


@dataclass(slots=True)
class ToolProbe:
    name: str
    executable: str | None
    available: bool
    version_line: str | None = None
    error: str | None = None


@dataclass(slots=True)
class FFmpegInstallation:
    ffmpeg: ToolProbe
    ffprobe: ToolProbe

    @property
    def is_ready(self) -> bool:
        return self.ffmpeg.available and self.ffprobe.available


def _resolve_executable(tool_name: str, configured_path: str | None) -> str | None:
    if configured_path:
        return configured_path
    bundled = get_bundled_dependency_path("dependencies", "ffmpeg", f"{tool_name}.exe")
    if bundled:
        return str(bundled)
    return shutil.which(tool_name)


def probe_tool(tool_name: str, configured_path: str | None = None) -> ToolProbe:
    executable = _resolve_executable(tool_name, configured_path)
    if not executable:
        return ToolProbe(name=tool_name, executable=None, available=False, error="Not found")

    try:
        result = subprocess.run(
            [executable, "-version"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    except OSError as exc:
        return ToolProbe(
            name=tool_name,
            executable=executable,
            available=False,
            error=str(exc),
        )

    version_line = result.stdout.splitlines()[0] if result.stdout else None
    return ToolProbe(
        name=tool_name,
        executable=executable,
        available=result.returncode == 0,
        version_line=version_line,
        error=None if result.returncode == 0 else (result.stderr or "Unknown error"),
    )


def detect_ffmpeg_installation(settings: AppSettings) -> FFmpegInstallation:
    dependency_paths = settings.dependency_paths
    ffmpeg = probe_tool("ffmpeg", dependency_paths.ffmpeg_path)
    ffprobe = probe_tool("ffprobe", dependency_paths.ffprobe_path)
    return FFmpegInstallation(ffmpeg=ffmpeg, ffprobe=ffprobe)
