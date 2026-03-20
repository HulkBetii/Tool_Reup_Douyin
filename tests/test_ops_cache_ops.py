from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from app.ops.cache_ops import build_cache_inventory, cleanup_cache


class _FakeDatabase:
    def __init__(self, job_rows: list[dict[str, object]]) -> None:
        self._job_rows = job_rows

    def list_job_runs(self) -> list[dict[str, object]]:
        return list(self._job_rows)


def test_cache_inventory_marks_manifest_referenced_artifacts_and_orphans(tmp_path: Path) -> None:
    workspace = SimpleNamespace(root_dir=tmp_path, cache_dir=tmp_path / "cache", exports_dir=tmp_path / "exports")
    referenced_wav = workspace.cache_dir / "tts" / "clip_ref.wav"
    orphan_wav = workspace.cache_dir / "tts" / "clip_orphan.wav"
    manifest_path = workspace.cache_dir / "tts" / "manifest.json"
    referenced_wav.parent.mkdir(parents=True, exist_ok=True)
    referenced_wav.write_bytes(b"ref")
    orphan_wav.write_bytes(b"orph")
    manifest_path.write_text(
        json.dumps({"artifacts": [{"raw_wav_path": str(referenced_wav)}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    database = _FakeDatabase(
        [
            {
                "status": "success",
                "stage": "tts",
                "output_paths_json": json.dumps([str(manifest_path)]),
            }
        ]
    )

    report = build_cache_inventory(workspace, database)
    tts_bucket = next(bucket for bucket in report.buckets if bucket.bucket_name == "tts")

    assert tts_bucket.referenced_file_count == 2
    assert tts_bucket.orphan_file_count == 1
    assert orphan_wav.resolve() in tts_bucket.orphan_paths


def test_cleanup_cache_keeps_referenced_files_and_deletes_orphans(tmp_path: Path) -> None:
    workspace = SimpleNamespace(root_dir=tmp_path, cache_dir=tmp_path / "cache", exports_dir=tmp_path / "exports")
    referenced_wav = workspace.cache_dir / "tts" / "clip_ref.wav"
    orphan_wav = workspace.cache_dir / "tts" / "clip_orphan.wav"
    manifest_path = workspace.cache_dir / "tts" / "manifest.json"
    referenced_wav.parent.mkdir(parents=True, exist_ok=True)
    referenced_wav.write_bytes(b"ref")
    orphan_wav.write_bytes(b"orph")
    manifest_path.write_text(
        json.dumps({"artifacts": [{"raw_wav_path": str(referenced_wav)}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    database = _FakeDatabase(
        [
            {
                "status": "success",
                "stage": "tts",
                "output_paths_json": json.dumps([str(manifest_path)]),
            }
        ]
    )

    report = cleanup_cache(workspace, database, bucket_names=["tts"])

    assert orphan_wav.resolve() in report.deleted_paths
    assert referenced_wav.exists()
    assert manifest_path.exists()
    assert not orphan_wav.exists()
