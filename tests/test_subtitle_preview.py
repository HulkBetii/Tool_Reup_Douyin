from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.subtitle.preview import MpvPreviewController, PreviewUnavailableError, prepare_mpv_environment, resolve_mpv_dll_path


def test_resolve_mpv_dll_path_validates_input(tmp_path: Path) -> None:
    dll_path = tmp_path / "mpv-2.dll"
    dll_path.write_bytes(b"dll")

    assert resolve_mpv_dll_path(str(dll_path)) == dll_path.resolve()

    with pytest.raises(PreviewUnavailableError):
        resolve_mpv_dll_path(None)

    with pytest.raises(PreviewUnavailableError):
        resolve_mpv_dll_path(str(tmp_path / "missing.dll"))


def test_prepare_mpv_environment_prepends_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dll_path = tmp_path / "mpv-2.dll"
    dll_path.write_bytes(b"dll")
    monkeypatch.setenv("PATH", "C:\\Windows\\System32")

    resolved = prepare_mpv_environment(str(dll_path))

    assert resolved == dll_path.resolve()
    assert os.environ["PATH"].split(os.pathsep)[0] == str(dll_path.parent.resolve())


def test_resolve_mpv_dll_path_uses_bundled_runtime_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dll_path = tmp_path / "mpv-2.dll"
    dll_path.write_bytes(b"dll")
    monkeypatch.setattr(
        "app.subtitle.preview.get_bundled_dependency_path",
        lambda *parts: dll_path if parts[-1] == "mpv-2.dll" else None,
    )

    assert resolve_mpv_dll_path(None) == dll_path


def test_mpv_preview_controller_reloads_existing_subtitle_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_video = tmp_path / "video.mp4"
    subtitle_path = tmp_path / "preview.ass"
    source_video.write_bytes(b"video")
    subtitle_path.write_text("[Script Info]\nTitle: Preview", encoding="utf-8")

    class FakePlayer:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []
            self.pause = True

        def play(self, path: str) -> None:
            self.calls.append(("play", path))

        def sub_add(self, path: str, flags: str = "select", title: str | None = None, lang: str | None = None) -> None:
            self.calls.append(("sub_add", path, flags, title, lang))

        def seek(self, amount: float, reference: str = "relative", precision: str = "keyframes") -> None:
            self.calls.append(("seek", amount, reference, precision))

        def sub_reload(self, sub_id=None) -> None:
            self.calls.append(("sub_reload", sub_id))

        def terminate(self) -> None:
            self.calls.append(("terminate",))

    player = FakePlayer()
    monkeypatch.setattr("app.subtitle.preview.load_mpv_module", lambda _path: SimpleNamespace(MPV=lambda **_kwargs: player))
    monkeypatch.setattr("app.subtitle.preview.time.sleep", lambda _seconds: None)

    controller = MpvPreviewController()
    controller.preview(
        source_video_path=source_video,
        subtitle_path=subtitle_path,
        mpv_dll_path="C:/fake/mpv-2.dll",
        start_ms=2500,
    )
    controller.reload_subtitles(subtitle_path)

    assert controller.is_active is True
    assert ("play", str(source_video)) in player.calls
    assert ("sub_add", str(subtitle_path), "select", "Preview", None) in player.calls
    assert ("seek", 2.5, "absolute", "exact") in player.calls
    assert ("sub_reload", None) in player.calls


def test_mpv_preview_controller_reload_requires_active_session(tmp_path: Path) -> None:
    subtitle_path = tmp_path / "preview.ass"
    subtitle_path.write_text("[Script Info]\nTitle: Preview", encoding="utf-8")
    controller = MpvPreviewController()

    with pytest.raises(PreviewUnavailableError):
        controller.reload_subtitles(subtitle_path)
