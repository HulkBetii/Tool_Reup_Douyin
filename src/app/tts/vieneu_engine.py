from __future__ import annotations

import importlib
import importlib.util
import os
import shutil
import wave
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

from app.core.paths import get_bundled_dependency_path

from .base import TTSEngine
from .models import SynthesisResult, VoicePreset

_LOCALAPPDATA = Path(os.environ.get("LOCALAPPDATA", ""))
_WINDOWS_ESPEAK_CANDIDATES = (
    Path("C:/Program Files/eSpeak NG/espeak-ng.exe"),
    Path("C:/Program Files/eSpeak NG/libespeak-ng.dll"),
    Path("C:/Program Files/eSpeak NG/espeak.exe"),
    Path("C:/Program Files (x86)/eSpeak NG/espeak-ng.exe"),
    Path("C:/Program Files (x86)/eSpeak NG/libespeak-ng.dll"),
    Path("C:/Program Files (x86)/eSpeak NG/espeak.exe"),
    _LOCALAPPDATA / "Programs" / "eSpeak NG" / "espeak-ng.exe",
    _LOCALAPPDATA / "Programs" / "eSpeak NG" / "libespeak-ng.dll",
    _LOCALAPPDATA / "Programs" / "eSpeak NG" / "espeak.exe",
)


@dataclass(slots=True)
class VieneuEnvironment:
    package_installed: bool
    package_version: str | None
    espeak_path: Path | None
    detail: str

    @property
    def local_ready(self) -> bool:
        return self.package_installed and self.espeak_path is not None


def _get_vieneu_version() -> str | None:
    try:
        return metadata.version("vieneu")
    except metadata.PackageNotFoundError:
        return None


def _runtime_espeak_candidates() -> tuple[Path, ...]:
    bundled_candidates = tuple(
        candidate
        for candidate in (
            get_bundled_dependency_path("dependencies", "espeak-ng", "espeak-ng.exe"),
            get_bundled_dependency_path("dependencies", "espeak-ng", "libespeak-ng.dll"),
            get_bundled_dependency_path("dependencies", "espeak-ng", "espeak.exe"),
        )
        if candidate is not None
    )
    return bundled_candidates + _WINDOWS_ESPEAK_CANDIDATES


def _find_espeak_dependency(candidate_paths: tuple[Path, ...] | None = None) -> Path | None:
    resolved_candidate_paths = candidate_paths or _runtime_espeak_candidates()
    for executable_name in ("espeak-ng.exe", "espeak-ng", "espeak.exe", "espeak"):
        resolved = shutil.which(executable_name)
        if resolved:
            return Path(resolved)
    for path in resolved_candidate_paths:
        if path.exists():
            return path
    return None


def detect_vieneu_installation() -> VieneuEnvironment:
    package_installed = importlib.util.find_spec("vieneu") is not None
    package_version = _get_vieneu_version() if package_installed else None
    espeak_path = _find_espeak_dependency()

    if package_installed and espeak_path:
        version_suffix = f" v{package_version}" if package_version else ""
        detail = f"VieNeu{version_suffix} san sang cho local mode"
    elif package_installed:
        version_suffix = f" v{package_version}" if package_version else ""
        detail = f"Da cai VieNeu{version_suffix}, nhung chua tim thay eSpeak NG cho local mode"
    else:
        detail = "Chua cai package `vieneu`"
    return VieneuEnvironment(
        package_installed=package_installed,
        package_version=package_version,
        espeak_path=espeak_path,
        detail=detail,
    )


def get_vieneu_mode(preset: VoicePreset) -> str:
    raw_mode = str(preset.engine_options.get("mode", "local")).strip().lower()
    if raw_mode not in {"local", "remote"}:
        raise ValueError("VieNeu engine_options.mode chi ho tro `local` hoac `remote`.")
    return raw_mode


def _resolve_reference_audio_path(project_root: Path | None, preset: VoicePreset) -> Path | None:
    raw_path = preset.engine_options.get("ref_audio_path")
    if not raw_path:
        return None

    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute() and project_root:
        path = project_root / path
    return path.resolve()


class VieneuTTSEngine(TTSEngine):
    def __init__(self, *, project_root: Path | None = None) -> None:
        self._project_root = project_root
        self._client: Any | None = None
        self._client_config_key: tuple[tuple[str, str], ...] | None = None

    def _build_client_config_key(self, preset: VoicePreset) -> tuple[tuple[str, str], ...]:
        mode = get_vieneu_mode(preset)
        engine_options = preset.engine_options
        items = [("mode", mode)]
        for key in ("api_base", "model_name"):
            value = engine_options.get(key)
            if value:
                items.append((key, str(value)))
        return tuple(items)

    def _build_init_kwargs(self, preset: VoicePreset) -> dict[str, Any]:
        mode = get_vieneu_mode(preset)
        init_kwargs: dict[str, Any] = {}
        if mode == "remote":
            init_kwargs["mode"] = "remote"
            for key in ("api_base", "model_name"):
                value = preset.engine_options.get(key)
                if value:
                    init_kwargs[key] = str(value)
        return init_kwargs

    def _get_client(self, preset: VoicePreset) -> Any:
        config_key = self._build_client_config_key(preset)
        if self._client is not None and self._client_config_key == config_key:
            return self._client

        environment = detect_vieneu_installation()
        if not environment.package_installed:
            raise RuntimeError(
                "Chua cai package `vieneu`. Tren Windows, cai them theo docs: "
                "`pip install vieneu --extra-index-url https://pnnbao97.github.io/llama-cpp-python-v0.3.16/cpu/`."
            )
        if get_vieneu_mode(preset) == "local" and not environment.espeak_path:
            raise RuntimeError("VieNeu local can eSpeak NG. Hay cai eSpeak NG (.msi) truoc khi chay TTS.")

        module = importlib.import_module("vieneu")
        client_class = getattr(module, "Vieneu", None)
        if client_class is None:
            raise RuntimeError("Package `vieneu` da cai nhung khong export `Vieneu`.")

        self._client = client_class(**self._build_init_kwargs(preset))
        self._client_config_key = config_key
        return self._client

    def _build_infer_kwargs(self, client: Any, *, text: str, preset: VoicePreset) -> dict[str, Any]:
        infer_kwargs: dict[str, Any] = {"text": text}
        ref_audio_path = _resolve_reference_audio_path(self._project_root, preset)
        ref_text = str(preset.engine_options.get("ref_text", "")).strip()
        if ref_audio_path:
            if not ref_audio_path.exists():
                raise RuntimeError(f"Khong tim thay ref_audio_path cho VieNeu: {ref_audio_path}")
            infer_kwargs["ref_audio"] = str(ref_audio_path)
            if ref_text:
                infer_kwargs["ref_text"] = ref_text
            return infer_kwargs

        voice_id = preset.voice_id.strip()
        if voice_id and voice_id.lower() != "default":
            get_preset_voice = getattr(client, "get_preset_voice", None)
            if not callable(get_preset_voice):
                raise RuntimeError("Vieneu SDK hien tai khong ho tro `get_preset_voice`.")
            infer_kwargs["voice"] = get_preset_voice(voice_id)
        return infer_kwargs

    def synthesize(
        self,
        *,
        text: str,
        output_path: Path,
        preset: VoicePreset,
    ) -> SynthesisResult:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        client = self._get_client(preset)
        audio = client.infer(**self._build_infer_kwargs(client, text=text, preset=preset))
        client.save(audio, str(output_path))
        if not output_path.exists() or output_path.stat().st_size <= 44:
            raise RuntimeError("VieNeu synth that bai, khong tao duoc wav output.")

        with wave.open(str(output_path), "rb") as handle:
            frames = handle.getnframes()
            sample_rate = handle.getframerate()
            duration_ms = int((frames / sample_rate) * 1000) if sample_rate else 0
        return SynthesisResult(
            wav_path=output_path,
            duration_ms=duration_ms,
            sample_rate=sample_rate or preset.sample_rate,
            voice_id=None if preset.voice_id == "default" else preset.voice_id,
        )
