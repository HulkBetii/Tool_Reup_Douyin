from __future__ import annotations

from app.core.ffmpeg import _resolve_executable


def test_resolve_executable_prefers_bundled_ffmpeg_runtime(monkeypatch) -> None:
    monkeypatch.setattr("app.core.ffmpeg.get_bundled_dependency_path", lambda *parts: None)
    monkeypatch.setattr("app.core.ffmpeg.shutil.which", lambda name: f"C:/tools/{name}.exe")
    assert _resolve_executable("ffmpeg", None) == "C:/tools/ffmpeg.exe"

    monkeypatch.setattr(
        "app.core.ffmpeg.get_bundled_dependency_path",
        lambda *parts: "C:/bundle/dependencies/ffmpeg/ffmpeg.exe",
    )
    assert _resolve_executable("ffmpeg", None) == "C:/bundle/dependencies/ffmpeg/ffmpeg.exe"
