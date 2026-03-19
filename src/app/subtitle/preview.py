from __future__ import annotations

import importlib
import os
import time
from pathlib import Path
from types import ModuleType

from app.core.paths import get_bundled_dependency_path


class PreviewUnavailableError(RuntimeError):
    """Raised when mpv preview cannot be started on the current machine."""


def resolve_mpv_dll_path(mpv_dll_path: str | None) -> Path:
    if not mpv_dll_path:
        bundled = get_bundled_dependency_path("dependencies", "mpv", "mpv-2.dll")
        if bundled is None:
            bundled = get_bundled_dependency_path("dependencies", "mpv", "libmpv-2.dll")
        if bundled is None:
            raise PreviewUnavailableError("Chua cau hinh mpv_dll_path trong Cai dat")
        return bundled
    resolved = Path(mpv_dll_path).expanduser().resolve()
    if not resolved.exists():
        raise PreviewUnavailableError(f"Khong tim thay mpv dll: {resolved}")
    return resolved


def prepare_mpv_environment(mpv_dll_path: str | None) -> Path:
    resolved = resolve_mpv_dll_path(mpv_dll_path)
    directory = str(resolved.parent)
    path_entries = os.environ.get("PATH", "").split(os.pathsep) if os.environ.get("PATH") else []
    if directory not in path_entries:
        os.environ["PATH"] = directory + os.pathsep + os.environ.get("PATH", "")
    return resolved


def load_mpv_module(mpv_dll_path: str | None) -> ModuleType:
    prepare_mpv_environment(mpv_dll_path)
    return importlib.import_module("mpv")


class MpvPreviewController:
    def __init__(self) -> None:
        self._player = None
        self._source_video_path: Path | None = None
        self._subtitle_path: Path | None = None

    @property
    def is_active(self) -> bool:
        return self._player is not None

    def close(self) -> None:
        if self._player is None:
            return
        try:
            self._player.terminate()
        except Exception:
            pass
        finally:
            self._player = None
            self._source_video_path = None
            self._subtitle_path = None

    def preview(
        self,
        *,
        source_video_path: Path,
        subtitle_path: Path,
        mpv_dll_path: str | None,
        start_ms: int = 0,
    ) -> None:
        if not source_video_path.exists():
            raise FileNotFoundError(f"Khong tim thay source video: {source_video_path}")
        if not subtitle_path.exists():
            raise FileNotFoundError(f"Khong tim thay subtitle file: {subtitle_path}")

        self.close()
        mpv = load_mpv_module(mpv_dll_path)
        player = mpv.MPV(
            force_window="yes",
            keep_open="yes",
            osc="yes",
            input_default_bindings="yes",
            sub_auto="no",
            audio_display="no",
            title="Reup Video Preview",
        )
        player.play(str(source_video_path))
        time.sleep(0.1)
        player.sub_add(str(subtitle_path), "select", "Preview")
        if start_ms > 0:
            player.seek(round(start_ms / 1000.0, 3), reference="absolute", precision="exact")
        player.pause = False
        self._player = player
        self._source_video_path = source_video_path.resolve()
        self._subtitle_path = subtitle_path.resolve()

    def reload_subtitles(self, subtitle_path: Path) -> None:
        if self._player is None:
            raise PreviewUnavailableError("Preview mpv chua duoc mo")
        if not subtitle_path.exists():
            raise FileNotFoundError(f"Khong tim thay subtitle file: {subtitle_path}")

        resolved_subtitle_path = subtitle_path.resolve()
        try:
            if self._subtitle_path == resolved_subtitle_path:
                self._player.sub_reload()
            else:
                self._player.sub_add(str(resolved_subtitle_path), "select", "Preview")
        except Exception as exc:
            self.close()
            raise PreviewUnavailableError(f"Khong the reload subtitle trong mpv: {exc}") from exc

        self._subtitle_path = resolved_subtitle_path
