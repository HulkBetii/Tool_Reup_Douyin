from __future__ import annotations

from pathlib import Path


def test_pyinstaller_spec_keeps_explicit_mpv_hiddenimport(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = repo_root / "build" / "pyinstaller.spec"
    captured: dict[str, object] = {}
    collect_calls: list[str] = []

    def _analysis(_scripts, **kwargs):
        captured["hiddenimports"] = list(kwargs.get("hiddenimports", []))
        captured["module_collection_mode"] = dict(kwargs.get("module_collection_mode", {}))
        captured["excludes"] = list(kwargs.get("excludes", []))
        return type("AnalysisResult", (), {"pure": [], "scripts": [], "binaries": [], "datas": []})()

    def _collect_all(name: str):
        collect_calls.append(name)
        return [], [], []

    globals_dict = {
        "Analysis": _analysis,
        "PYZ": lambda _pure: object(),
        "EXE": lambda *args, **kwargs: object(),
        "COLLECT": lambda *args, **kwargs: object(),
    }
    monkeypatch.setenv("REUP_VIDEO_ROOT", str(repo_root))
    monkeypatch.setattr("PyInstaller.utils.hooks.collect_all", _collect_all)
    monkeypatch.setattr("PyInstaller.utils.hooks.collect_submodules", lambda _name: [])

    exec(spec_path.read_text(encoding="utf-8"), globals_dict)

    assert "mpv" in captured["hiddenimports"]
    assert captured["module_collection_mode"]["neucodec"] == "pyz+py"
    assert captured["module_collection_mode"]["llama_cpp"] == "pyz+py"
    assert "tkinter" in captured["excludes"]
    assert "_tkinter" in captured["excludes"]
    assert "llama_cpp" in collect_calls
    assert "sea_g2p" in collect_calls
    assert captured["module_collection_mode"]["sea_g2p"] == "pyz+py"
