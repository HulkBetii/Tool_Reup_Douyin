from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app.ops.models import CacheBucketStats, CacheCleanupReport, CacheInventoryReport, utc_now_iso
from app.project.runtime_state import restore_pipeline_state

_BUCKET_RELATIVE_DIRS: dict[str, tuple[str, ...]] = {
    "audio": ("cache", "extract_audio"),
    "asr": ("cache", "asr"),
    "translate": ("cache", "translate"),
    "translate_contextual": ("cache", "translate_contextual"),
    "tts": ("cache", "tts"),
    "mix": ("cache", "mix"),
    "subs": ("cache", "subs"),
    "exports": ("exports",),
}


def _bucket_root(workspace, bucket_name: str) -> Path:
    relative_parts = _BUCKET_RELATIVE_DIRS[bucket_name]
    return workspace.root_dir.joinpath(*relative_parts)


def _safe_resolve(path: Path) -> Path:
    try:
        return path.resolve()
    except OSError:
        return path


def _is_under_workspace(candidate: Path, workspace_root: Path) -> bool:
    resolved_candidate = _safe_resolve(candidate)
    resolved_root = _safe_resolve(workspace_root)
    try:
        resolved_candidate.relative_to(resolved_root)
        return True
    except ValueError:
        return False


def _maybe_collect_path(value: str, *, workspace_root: Path, base_dir: Path) -> Path | None:
    candidate = Path(value)
    if not candidate.is_absolute():
        if not any(sep in value for sep in (os.sep, "/", "\\")) and "." not in candidate.name:
            return None
        candidate = (base_dir / candidate).resolve()
    else:
        candidate = _safe_resolve(candidate)
    if not candidate.exists():
        return None
    if not _is_under_workspace(candidate, workspace_root):
        return None
    return candidate


def _collect_paths_from_json_value(value: Any, *, workspace_root: Path, base_dir: Path) -> set[Path]:
    collected: set[Path] = set()
    if isinstance(value, str):
        candidate = _maybe_collect_path(value, workspace_root=workspace_root, base_dir=base_dir)
        if candidate is not None:
            collected.add(candidate)
        return collected
    if isinstance(value, list):
        for item in value:
            collected.update(_collect_paths_from_json_value(item, workspace_root=workspace_root, base_dir=base_dir))
        return collected
    if isinstance(value, dict):
        for item in value.values():
            collected.update(_collect_paths_from_json_value(item, workspace_root=workspace_root, base_dir=base_dir))
    return collected


def _collect_manifest_references(path: Path, *, workspace_root: Path, visited: set[Path]) -> set[Path]:
    resolved_path = _safe_resolve(path)
    if resolved_path in visited or resolved_path.suffix.lower() != ".json":
        return set()
    visited.add(resolved_path)
    try:
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    nested = _collect_paths_from_json_value(payload, workspace_root=workspace_root, base_dir=resolved_path.parent)
    references = {resolved_path, *nested}
    for candidate in list(nested):
        references.update(_collect_manifest_references(candidate, workspace_root=workspace_root, visited=visited))
    return references


def collect_referenced_artifact_paths(workspace, database) -> set[Path]:
    references: set[Path] = set()
    visited_json: set[Path] = set()
    job_rows = database.list_job_runs()
    restored_state = restore_pipeline_state(job_rows)
    for path in (
        *restored_state.subtitle_outputs.values(),
        restored_state.tts_manifest_path,
        restored_state.voice_track_path,
        restored_state.mixed_audio_path,
        restored_state.export_output_path,
    ):
        if path is None:
            continue
        resolved_path = _safe_resolve(path)
        if resolved_path.exists() and _is_under_workspace(resolved_path, workspace.root_dir):
            references.add(resolved_path)
            references.update(
                _collect_manifest_references(
                    resolved_path,
                    workspace_root=workspace.root_dir,
                    visited=visited_json,
                )
            )
    for row in job_rows:
        raw_value = row["output_paths_json"]
        try:
            values = json.loads(str(raw_value))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(values, list):
            continue
        for value in values:
            candidate = _maybe_collect_path(str(value), workspace_root=workspace.root_dir, base_dir=workspace.root_dir)
            if candidate is None:
                continue
            references.add(candidate)
            references.update(
                _collect_manifest_references(candidate, workspace_root=workspace.root_dir, visited=visited_json)
            )
    return references


def build_cache_inventory(workspace, database) -> CacheInventoryReport:
    referenced_paths = collect_referenced_artifact_paths(workspace, database)
    buckets: list[CacheBucketStats] = []
    for bucket_name in _BUCKET_RELATIVE_DIRS:
        root_path = _bucket_root(workspace, bucket_name)
        stats = CacheBucketStats(bucket_name=bucket_name, root_path=root_path)
        if root_path.exists():
            for file_path in (path for path in root_path.rglob("*") if path.is_file()):
                resolved_path = _safe_resolve(file_path)
                file_size = resolved_path.stat().st_size
                stats.file_count += 1
                stats.total_bytes += file_size
                if resolved_path in referenced_paths:
                    stats.referenced_file_count += 1
                    stats.referenced_bytes += file_size
                else:
                    stats.orphan_file_count += 1
                    stats.orphan_bytes += file_size
                    stats.orphan_paths.append(resolved_path)
        buckets.append(stats)
    return CacheInventoryReport(
        generated_at=utc_now_iso(),
        workspace_root=workspace.root_dir,
        referenced_paths=sorted(referenced_paths),
        buckets=buckets,
    )


def cleanup_cache(
    workspace,
    database,
    *,
    bucket_names: list[str] | tuple[str, ...] | None = None,
) -> CacheCleanupReport:
    selected_buckets = tuple(bucket_names or tuple(_BUCKET_RELATIVE_DIRS))
    inventory = build_cache_inventory(workspace, database)
    deleted_paths: list[Path] = []
    deleted_bytes = 0
    skipped_referenced_paths = 0
    for bucket in inventory.buckets:
        if bucket.bucket_name not in selected_buckets:
            continue
        for path in bucket.orphan_paths:
            if not path.exists():
                continue
            try:
                deleted_bytes += path.stat().st_size
                path.unlink()
                deleted_paths.append(path)
            except OSError:
                skipped_referenced_paths += 1
        root_path = bucket.root_path
        if root_path.exists():
            for directory in sorted(
                (candidate for candidate in root_path.rglob("*") if candidate.is_dir()),
                key=lambda item: len(item.parts),
                reverse=True,
            ):
                try:
                    directory.rmdir()
                except OSError:
                    continue
    return CacheCleanupReport(
        generated_at=utc_now_iso(),
        workspace_root=workspace.root_dir,
        bucket_names=selected_buckets,
        deleted_paths=deleted_paths,
        deleted_bytes=deleted_bytes,
        skipped_referenced_paths=skipped_referenced_paths,
    )
