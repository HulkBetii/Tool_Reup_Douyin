from __future__ import annotations

from pathlib import Path


def test_pyinstaller_spec_keeps_explicit_mpv_hiddenimport(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = repo_root / "build" / "pyinstaller.spec"
    captured: dict[str, object] = {}

    def _analysis(_scripts, **kwargs):
        captured["hiddenimports"] = list(kwargs.get("hiddenimports", []))
        return type("AnalysisResult", (), {"pure": [], "scripts": [], "binaries": [], "datas": []})()

    globals_dict = {
        "Analysis": _analysis,
        "PYZ": lambda _pure: object(),
        "EXE": lambda *args, **kwargs: object(),
        "COLLECT": lambda *args, **kwargs: object(),
    }
    monkeypatch.setenv("REUP_VIDEO_ROOT", str(repo_root))
    monkeypatch.setattr("PyInstaller.utils.hooks.collect_all", lambda _name: ([], [], []))
    monkeypatch.setattr("PyInstaller.utils.hooks.collect_submodules", lambda _name: [])

    exec(spec_path.read_text(encoding="utf-8"), globals_dict)

    assert "mpv" in captured["hiddenimports"]
