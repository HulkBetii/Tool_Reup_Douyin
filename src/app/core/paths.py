from __future__ import annotations

import os
import sys
from pathlib import Path

from app.version import APP_SLUG


def get_roaming_appdata_root() -> Path:
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata)
    return Path.home() / "AppData" / "Roaming"


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def iter_runtime_roots() -> tuple[Path, ...]:
    roots: list[Path] = []
    if getattr(sys, "frozen", False):
        roots.append(Path(sys.executable).resolve().parent)
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        roots.append(Path(str(meipass)).resolve())
    if not roots:
        roots.append(get_repo_root())

    unique_roots: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        unique_roots.append(root)
        seen.add(root)
    return tuple(unique_roots)


def get_runtime_root() -> Path:
    return iter_runtime_roots()[0]


def get_bundled_dependency_path(*relative_parts: str) -> Path | None:
    for root in iter_runtime_roots():
        candidate = root.joinpath(*relative_parts)
        if candidate.exists():
            return candidate
    return None


def get_appdata_dir(base_dir: Path | None = None) -> Path:
    root = base_dir or get_roaming_appdata_root()
    path = Path(root) / APP_SLUG
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logs_dir(base_dir: Path | None = None) -> Path:
    return ensure_directory(get_appdata_dir(base_dir) / "logs")


def get_models_dir(base_dir: Path | None = None) -> Path:
    return ensure_directory(get_appdata_dir(base_dir) / "models")


def get_settings_path(base_dir: Path | None = None) -> Path:
    return get_appdata_dir(base_dir) / "settings.json"
