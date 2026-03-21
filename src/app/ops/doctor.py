from __future__ import annotations

import importlib.util
import shutil
import tempfile
from pathlib import Path

from app.core.ffmpeg import detect_ffmpeg_installation
from app.core.paths import get_appdata_dir
from app.core.settings import AppSettings
from app.ops.models import DoctorCheckResult, DoctorReport, utc_now_iso
from app.project.models import ProjectWorkspace
from app.subtitle.preview import PreviewUnavailableError, load_mpv_module, resolve_mpv_dll_path
from app.tts.models import VoicePreset
from app.tts.vieneu_engine import detect_vieneu_installation, get_vieneu_mode

DEFAULT_MIN_FREE_BYTES = 2 * 1024 * 1024 * 1024


def _status_for_bool(value: bool) -> str:
    return "ok" if value else "error"


def _is_writable_directory(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, prefix=".doctor-", delete=True):
            return True
    except OSError:
        return False


def _disk_usage_check(path: Path, minimum_free_bytes: int) -> DoctorCheckResult:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return DoctorCheckResult(
            name="disk_space",
            status="warning",
            message=f"Khong doc duoc dung luong dia tai {path}: {exc}",
            fix_hint="Kiem tra quyen truy cap vao o dia chua workspace hoac appdata.",
        )
    free_gb = usage.free / (1024**3)
    if usage.free >= minimum_free_bytes:
        return DoctorCheckResult(
            name="disk_space",
            status="ok",
            message=f"Dung luong trong con lai: {free_gb:.2f} GB",
            detail_json={"free_bytes": usage.free, "path": str(path)},
        )
    return DoctorCheckResult(
        name="disk_space",
        status="warning",
        message=f"Dung luong trong thap: {free_gb:.2f} GB",
        fix_hint="Giai phong bo nho truoc khi rerun TTS/mix/export dai.",
        detail_json={"free_bytes": usage.free, "path": str(path), "minimum_free_bytes": minimum_free_bytes},
    )


def _check_ffmpeg(settings: AppSettings) -> list[DoctorCheckResult]:
    installation = detect_ffmpeg_installation(settings)
    return [
        DoctorCheckResult(
            name="ffmpeg",
            status=_status_for_bool(installation.ffmpeg.available),
            message=installation.ffmpeg.version_line or installation.ffmpeg.error or "ffmpeg san sang",
            fix_hint="Cau hinh duong dan ffmpeg.exe trong Cai dat hoac copy dependency vao bundle.",
            blocking_stages=("probe_media", "extract_audio", "asr", "voice_track", "mixdown", "export_video"),
            detail_json={"path": installation.ffmpeg.executable},
        ),
        DoctorCheckResult(
            name="ffprobe",
            status=_status_for_bool(installation.ffprobe.available),
            message=installation.ffprobe.version_line or installation.ffprobe.error or "ffprobe san sang",
            fix_hint="Cau hinh duong dan ffprobe.exe trong Cai dat hoac copy dependency vao bundle.",
            blocking_stages=("probe_media", "extract_audio", "asr", "export_video"),
            detail_json={"path": installation.ffprobe.executable},
        ),
    ]


def _check_mpv(settings: AppSettings) -> DoctorCheckResult:
    try:
        resolved_path = resolve_mpv_dll_path(settings.dependency_paths.mpv_dll_path)
    except PreviewUnavailableError as exc:
        return DoctorCheckResult(
            name="mpv",
            status="warning",
            message=str(exc),
            fix_hint="Cau hinh mpv_dll_path trong Cai dat hoac copy mpv-2.dll vao bundle.",
            blocking_stages=("preview",),
        )
    module_spec = importlib.util.find_spec("mpv")
    try:
        if module_spec is None:
            load_mpv_module(settings.dependency_paths.mpv_dll_path)
        mpv_module_ready = True
    except Exception:
        mpv_module_ready = False
    status = "ok" if mpv_module_ready else "error"
    message = "mpv preview san sang" if mpv_module_ready else "Tim thay mpv dll nhung khong tai duoc python-mpv"
    return DoctorCheckResult(
        name="mpv",
        status=status,
        message=message,
        fix_hint="Cai python-mpv neu muon preview subtitle trong app.",
        blocking_stages=("preview",),
        detail_json={"path": str(resolved_path)},
    )


def _check_openai_api_key(settings: AppSettings) -> DoctorCheckResult:
    ready = bool(settings.openai_api_key)
    return DoctorCheckResult(
        name="openai_api_key",
        status=_status_for_bool(ready),
        message="OpenAI API key san sang" if ready else "Chua cau hinh OpenAI API key",
        fix_hint="Nhap OpenAI API key trong tab Cai dat truoc khi chay dich.",
        blocking_stages=("translate",),
    )


def _check_model_cache_dir(settings: AppSettings) -> DoctorCheckResult:
    raw_path = str(settings.model_cache_dir or "").strip()
    if not raw_path:
        return DoctorCheckResult(
            name="model_cache_dir",
            status="warning",
            message="Chua cau hinh thu muc model cache",
            fix_hint="Luu lai Cai dat de app tao thu muc cache model mac dinh.",
        )
    path = Path(raw_path)
    path.mkdir(parents=True, exist_ok=True)
    writable = _is_writable_directory(path)
    return DoctorCheckResult(
        name="model_cache_dir",
        status="ok" if writable else "warning",
        message=f"Model cache dir: {path}",
        fix_hint="Kiem tra quyen ghi vao model cache dir.",
        detail_json={"path": str(path)},
    )


def _check_writable_path(name: str, path: Path, *, blocking_stages: tuple[str, ...]) -> DoctorCheckResult:
    writable = _is_writable_directory(path)
    return DoctorCheckResult(
        name=name,
        status=_status_for_bool(writable),
        message=f"Co the ghi vao {path}" if writable else f"Khong the ghi vao {path}",
        fix_hint="Kiem tra quyen ghi, antivirus, hoac duong dan khong hop le.",
        blocking_stages=blocking_stages,
        detail_json={"path": str(path)},
    )


def _check_vieneu_local(voice_preset: VoicePreset | None) -> DoctorCheckResult:
    environment = detect_vieneu_installation()
    requires_local_vieneu = False
    if voice_preset is not None and voice_preset.engine.strip().lower() == "vieneu":
        try:
            requires_local_vieneu = get_vieneu_mode(voice_preset) == "local"
        except ValueError:
            requires_local_vieneu = True

    if environment.local_ready:
        return DoctorCheckResult(
            name="vieneu_local",
            status="ok",
            message=environment.detail,
            blocking_stages=("tts",),
            detail_json={
                "package_version": environment.package_version,
                "espeak_path": str(environment.espeak_path) if environment.espeak_path else None,
            },
        )

    status = "error" if requires_local_vieneu else "warning"
    return DoctorCheckResult(
        name="vieneu_local",
        status=status,
        message=environment.detail,
        fix_hint="Can cai package vieneu va eSpeak NG neu muon chay VieNeu local.",
        blocking_stages=("tts",) if requires_local_vieneu else (),
        detail_json={
            "package_version": environment.package_version,
            "espeak_path": str(environment.espeak_path) if environment.espeak_path else None,
        },
    )


def run_doctor(
    *,
    settings: AppSettings,
    workspace: ProjectWorkspace | None = None,
    requested_stages: list[str] | tuple[str, ...] | None = None,
    voice_preset: VoicePreset | None = None,
    minimum_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
) -> DoctorReport:
    checks: list[DoctorCheckResult] = []
    checks.extend(_check_ffmpeg(settings))
    checks.append(_check_mpv(settings))
    checks.append(_check_vieneu_local(voice_preset))
    checks.append(_check_openai_api_key(settings))
    checks.append(_check_model_cache_dir(settings))

    appdata_dir = get_appdata_dir()
    checks.append(
        _check_writable_path(
            "appdata_write",
            appdata_dir,
            blocking_stages=("translate", "tts", "voice_track", "mixdown", "export_video"),
        )
    )
    checks.append(_disk_usage_check(appdata_dir, minimum_free_bytes))

    if workspace is not None:
        checks.append(
            _check_writable_path(
                "workspace_write",
                workspace.root_dir,
                blocking_stages=("probe_media", "extract_audio", "asr", "translate", "tts", "voice_track", "mixdown", "export_video"),
            )
        )
        checks.append(
            _check_writable_path(
                "cache_write",
                workspace.cache_dir,
                blocking_stages=("extract_audio", "asr", "translate", "tts", "voice_track", "mixdown"),
            )
        )
        checks.append(_disk_usage_check(workspace.root_dir, minimum_free_bytes))

    return DoctorReport(
        generated_at=utc_now_iso(),
        checks=checks,
        requested_stages=tuple(str(stage).strip().lower() for stage in (requested_stages or []) if str(stage).strip()),
    )


def format_blocked_message(report: DoctorReport, *, stages: list[str] | tuple[str, ...], action_label: str) -> str:
    blocking_checks = report.blocking_checks_for(stages)
    if not blocking_checks:
        return ""
    lines = [f"Blocked because {action_label} chua dat dieu kien moi truong:"]
    for item in blocking_checks:
        lines.append(f"- {item.name}: {item.message}")
        if item.fix_hint:
            lines.append(f"  Goi y: {item.fix_hint}")
    return "\n".join(lines)
