from __future__ import annotations

import base64
import ctypes
import json
from pathlib import Path

from pydantic import BaseModel, Field, PrivateAttr, ValidationError, model_validator

from app.core.paths import get_appdata_dir, get_models_dir, get_settings_path


class _DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.c_uint32),
        ("pbData", ctypes.POINTER(ctypes.c_ubyte)),
    ]


class _SecretCipherError(ValueError):
    pass


def _protect_bytes_windows(raw_bytes: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not raw_bytes:
        return b""
    raw_buffer = ctypes.create_string_buffer(raw_bytes)
    input_blob = _DataBlob(
        len(raw_bytes),
        ctypes.cast(raw_buffer, ctypes.POINTER(ctypes.c_ubyte)),
    )
    output_blob = _DataBlob()
    if not crypt32.CryptProtectData(
        ctypes.byref(input_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise _SecretCipherError("Khong ma hoa duoc secret bang Windows DPAPI")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        if output_blob.pbData:
            kernel32.LocalFree(output_blob.pbData)


def _unprotect_bytes_windows(protected_bytes: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not protected_bytes:
        return b""
    protected_buffer = ctypes.create_string_buffer(protected_bytes)
    input_blob = _DataBlob(
        len(protected_bytes),
        ctypes.cast(protected_buffer, ctypes.POINTER(ctypes.c_ubyte)),
    )
    output_blob = _DataBlob()
    if not crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise _SecretCipherError("Khong giai ma duoc secret bang Windows DPAPI")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        if output_blob.pbData:
            kernel32.LocalFree(output_blob.pbData)


def encrypt_secret(secret: str | None) -> str | None:
    if not secret:
        return None
    raw_bytes = secret.encode("utf-8")
    if ctypes.sizeof(ctypes.c_void_p) and hasattr(ctypes, "windll"):
        protected = _protect_bytes_windows(raw_bytes)
        return "dpapi:" + base64.b64encode(protected).decode("ascii")
    return "plain:" + base64.b64encode(raw_bytes).decode("ascii")


def decrypt_secret(payload: str | None) -> str | None:
    if not payload:
        return None
    scheme, separator, encoded = payload.partition(":")
    if not separator:
        raise _SecretCipherError("Secret payload khong dung dinh dang")
    raw_bytes = base64.b64decode(encoded.encode("ascii"))
    if scheme == "dpapi":
        return _unprotect_bytes_windows(raw_bytes).decode("utf-8")
    if scheme == "plain":
        return raw_bytes.decode("utf-8")
    raise _SecretCipherError(f"Khong ho tro secret scheme: {scheme}")


class DependencyPaths(BaseModel):
    ffmpeg_path: str | None = None
    ffprobe_path: str | None = None
    mpv_dll_path: str | None = None


VALID_UI_MODES = {"simple_v2", "advanced"}


def normalize_ui_mode(value: object) -> str:
    raw_value = str(value or "simple_v2").strip().lower()
    if raw_value not in VALID_UI_MODES:
        return "simple_v2"
    return raw_value


class AppSettings(BaseModel):
    ui_language: str = "vi"
    ui_mode: str = "simple_v2"
    dependency_paths: DependencyPaths = Field(default_factory=DependencyPaths)
    model_cache_dir: str | None = None
    openai_api_key_encrypted: str | None = None
    default_asr_model: str = "small"
    default_translation_model: str = "gpt-4.1-mini"
    gpu_enabled: bool = False
    telemetry_opt_out: bool = True
    recent_projects: list[str] = Field(default_factory=list)
    _openai_api_key: str | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _hydrate_private_fields(self) -> AppSettings:
        self._openai_api_key = None
        self.ui_mode = normalize_ui_mode(self.ui_mode)
        return self

    @property
    def openai_api_key(self) -> str | None:
        return self._openai_api_key

    @openai_api_key.setter
    def openai_api_key(self, value: str | None) -> None:
        self._openai_api_key = value.strip() or None if isinstance(value, str) else value


def build_default_settings(appdata_dir: Path | None = None) -> AppSettings:
    return AppSettings(model_cache_dir=str(get_models_dir(appdata_dir)))


def load_settings(
    settings_path: Path | None = None,
    appdata_dir: Path | None = None,
) -> AppSettings:
    resolved_path = settings_path or get_settings_path(appdata_dir)
    if not resolved_path.exists():
        settings = build_default_settings(appdata_dir)
        save_settings(settings, resolved_path, appdata_dir)
        return settings

    try:
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
        legacy_api_key = payload.pop("openai_api_key", None)
        settings = AppSettings.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError):
        settings = build_default_settings(appdata_dir)
        save_settings(settings, resolved_path, appdata_dir)
        return settings

    should_resave = False
    if legacy_api_key:
        settings.openai_api_key = str(legacy_api_key)
        should_resave = True
    elif settings.openai_api_key_encrypted:
        try:
            settings.openai_api_key = decrypt_secret(settings.openai_api_key_encrypted)
        except _SecretCipherError:
            settings.openai_api_key = None

    if not settings.model_cache_dir:
        settings.model_cache_dir = str(get_models_dir(appdata_dir))
        should_resave = True
    if should_resave:
        save_settings(settings, resolved_path, appdata_dir)
    return settings


def save_settings(
    settings: AppSettings,
    settings_path: Path | None = None,
    appdata_dir: Path | None = None,
) -> Path:
    get_appdata_dir(appdata_dir)
    resolved_path = settings_path or get_settings_path(appdata_dir)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    payload = settings.model_dump(mode="json")
    payload["openai_api_key_encrypted"] = encrypt_secret(settings.openai_api_key)
    settings.openai_api_key_encrypted = payload["openai_api_key_encrypted"]
    resolved_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return resolved_path
