from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


@dataclass(slots=True)
class DoctorCheckResult:
    name: str
    status: str
    message: str
    fix_hint: str = ""
    blocking_stages: tuple[str, ...] = ()
    detail_json: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(slots=True)
class DoctorReport:
    generated_at: str
    checks: list[DoctorCheckResult]
    requested_stages: tuple[str, ...] = ()

    @property
    def error_count(self) -> int:
        return sum(1 for item in self.checks if item.status == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for item in self.checks if item.status == "warning")

    def blocking_checks_for(self, stages: list[str] | tuple[str, ...] | set[str]) -> list[DoctorCheckResult]:
        stage_set = {str(stage).strip().lower() for stage in stages if str(stage).strip()}
        return [
            item
            for item in self.checks
            if item.status == "error"
            and (not stage_set or stage_set.intersection({stage.lower() for stage in item.blocking_stages}))
        ]

    def is_blocking_for(self, stages: list[str] | tuple[str, ...] | set[str]) -> bool:
        return bool(self.blocking_checks_for(stages))

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "requested_stages": list(self.requested_stages),
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "checks": [item.to_dict() for item in self.checks],
        }


@dataclass(slots=True)
class CacheBucketStats:
    bucket_name: str
    root_path: Path
    file_count: int = 0
    total_bytes: int = 0
    referenced_file_count: int = 0
    referenced_bytes: int = 0
    orphan_file_count: int = 0
    orphan_bytes: int = 0
    orphan_paths: list[Path] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(slots=True)
class CacheInventoryReport:
    generated_at: str
    workspace_root: Path
    referenced_paths: list[Path]
    buckets: list[CacheBucketStats]

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(slots=True)
class CacheCleanupReport:
    generated_at: str
    workspace_root: Path
    bucket_names: tuple[str, ...]
    deleted_paths: list[Path] = field(default_factory=list)
    deleted_bytes: int = 0
    skipped_referenced_paths: int = 0

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(slots=True)
class WorkspaceRepairIssue:
    severity: str
    code: str
    message: str
    path: Path | None = None
    detail_json: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(slots=True)
class WorkspaceRepairReport:
    generated_at: str
    workspace_root: Path
    checked_file_count: int
    checked_job_count: int
    issues: list[WorkspaceRepairIssue] = field(default_factory=list)
    fixed_items: list[str] = field(default_factory=list)
    schema_ok: bool = True

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "workspace_root": str(self.workspace_root),
            "checked_file_count": self.checked_file_count,
            "checked_job_count": self.checked_job_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "schema_ok": self.schema_ok,
            "issues": [issue.to_dict() for issue in self.issues],
            "fixed_items": list(self.fixed_items),
        }


@dataclass(slots=True)
class BackupManifest:
    created_at: str
    workspace_root: Path
    backup_dir: Path
    reason: str
    stage: str
    project_id: str | None
    copied_files: list[Path]

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))
