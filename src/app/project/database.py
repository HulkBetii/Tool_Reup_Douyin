from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.project.models import (
    JobRunRecord,
    MediaAssetRecord,
    ProjectRecord,
    SegmentRecord,
    SubtitleEventRecord,
    SubtitleTrackRecord,
)

SCHEMA_VERSION = 3
CANONICAL_SUBTITLE_TRACK_KIND = "canonical"
USER_SUBTITLE_TRACK_KIND = "user"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_dir TEXT NOT NULL,
    source_language TEXT NOT NULL,
    target_language TEXT NOT NULL,
    video_asset_id TEXT,
    active_subtitle_track_id TEXT,
    active_voice_preset_id TEXT,
    active_export_preset_id TEXT,
    active_watermark_profile_id TEXT,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS media_assets (
    asset_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT,
    duration_ms INTEGER,
    fps REAL,
    width INTEGER,
    height INTEGER,
    audio_channels INTEGER,
    sample_rate INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS segments (
    segment_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    source_lang TEXT,
    target_lang TEXT,
    source_text TEXT NOT NULL DEFAULT '',
    source_text_norm TEXT NOT NULL DEFAULT '',
    translated_text TEXT NOT NULL DEFAULT '',
    translated_text_norm TEXT NOT NULL DEFAULT '',
    subtitle_text TEXT NOT NULL DEFAULT '',
    tts_text TEXT NOT NULL DEFAULT '',
    audio_path TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    meta_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subtitle_tracks (
    track_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'user',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subtitle_events (
    event_id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    source_segment_id TEXT,
    event_index INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    source_lang TEXT,
    target_lang TEXT,
    source_text TEXT NOT NULL DEFAULT '',
    source_text_norm TEXT NOT NULL DEFAULT '',
    translated_text TEXT NOT NULL DEFAULT '',
    translated_text_norm TEXT NOT NULL DEFAULT '',
    subtitle_text TEXT NOT NULL DEFAULT '',
    tts_text TEXT NOT NULL DEFAULT '',
    audio_path TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    meta_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(track_id) REFERENCES subtitle_tracks(track_id) ON DELETE CASCADE,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_runs (
    job_id TEXT PRIMARY KEY,
    project_id TEXT,
    stage TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    input_hash TEXT NOT NULL DEFAULT '',
    output_paths_json TEXT NOT NULL DEFAULT '[]',
    log_path TEXT,
    error_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    ended_at TEXT,
    retry_of_job_id TEXT,
    message TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE SET NULL,
    FOREIGN KEY(retry_of_job_id) REFERENCES job_runs(job_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_media_assets_project_id ON media_assets(project_id);
CREATE INDEX IF NOT EXISTS idx_segments_project_id ON segments(project_id);
CREATE INDEX IF NOT EXISTS idx_subtitle_tracks_project_id ON subtitle_tracks(project_id);
CREATE INDEX IF NOT EXISTS idx_subtitle_events_project_track ON subtitle_events(project_id, track_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_subtitle_events_track_index ON subtitle_events(track_id, event_index);
CREATE INDEX IF NOT EXISTS idx_job_runs_project_id ON job_runs(project_id);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ProjectDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path

    @staticmethod
    def build_canonical_subtitle_track_id(project_id: str) -> str:
        return f"{project_id}:canonical"

    @contextmanager
    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(SCHEMA_SQL)
            self._ensure_project_columns(connection)
            schema_version = self._read_schema_version(connection)
            if schema_version < SCHEMA_VERSION:
                self._backfill_subtitle_tracks(connection)
            self._write_schema_version(connection, SCHEMA_VERSION)

    def _ensure_project_columns(self, connection: sqlite3.Connection) -> None:
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(projects)").fetchall()
        }
        if "active_watermark_profile_id" not in columns:
            connection.execute("ALTER TABLE projects ADD COLUMN active_watermark_profile_id TEXT")

    def _read_schema_version(self, connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version' LIMIT 1"
        ).fetchone()
        if not row:
            return 0
        try:
            return int(row["value"])
        except (TypeError, ValueError):
            return 0

    def _write_schema_version(self, connection: sqlite3.Connection, version: int) -> None:
        connection.execute(
            """
            INSERT INTO metadata(key, value)
            VALUES ('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(version),),
        )

    def insert_project(self, project: ProjectRecord) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO projects(
                    project_id, name, root_dir, source_language, target_language,
                    video_asset_id, active_subtitle_track_id, active_voice_preset_id,
                    active_export_preset_id, active_watermark_profile_id, notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.project_id,
                    project.name,
                    project.root_dir,
                    project.source_language,
                    project.target_language,
                    project.video_asset_id,
                    project.active_subtitle_track_id,
                    project.active_voice_preset_id,
                    project.active_export_preset_id,
                    project.active_watermark_profile_id,
                    project.notes,
                    project.created_at,
                    project.updated_at,
                ),
            )

    def insert_media_asset(self, asset: MediaAssetRecord) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO media_assets(
                    asset_id, project_id, type, path, sha256, duration_ms, fps, width,
                    height, audio_channels, sample_rate, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset.asset_id,
                    asset.project_id,
                    asset.asset_type,
                    asset.path,
                    asset.sha256,
                    asset.duration_ms,
                    asset.fps,
                    asset.width,
                    asset.height,
                    asset.audio_channels,
                    asset.sample_rate,
                    asset.created_at,
                ),
            )

    def insert_job_run(self, job_run: JobRunRecord) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO job_runs(
                    job_id, project_id, stage, description, status, progress, input_hash,
                    output_paths_json, log_path, error_json, started_at, ended_at,
                    retry_of_job_id, message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_run.job_id,
                    job_run.project_id,
                    job_run.stage,
                    job_run.description,
                    job_run.status,
                    job_run.progress,
                    job_run.input_hash,
                    json.dumps(job_run.output_paths),
                    job_run.log_path,
                    json.dumps(job_run.error_json),
                    job_run.started_at,
                    job_run.ended_at,
                    job_run.retry_of_job_id,
                    job_run.message,
                ),
            )

    def update_job_run(
        self,
        job_id: str,
        *,
        status: str,
        progress: int,
        message: str,
        log_path: str | None = None,
        ended_at: str | None = None,
        output_paths: list[str] | None = None,
        error_json: dict[str, object] | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE job_runs
                SET status = ?,
                    progress = ?,
                    message = ?,
                    log_path = COALESCE(?, log_path),
                    ended_at = COALESCE(?, ended_at),
                    output_paths_json = COALESCE(?, output_paths_json),
                    error_json = COALESCE(?, error_json)
                WHERE job_id = ?
                """,
                (
                    status,
                    progress,
                    message,
                    log_path,
                    ended_at,
                    json.dumps(output_paths) if output_paths is not None else None,
                    json.dumps(error_json) if error_json is not None else None,
                    job_id,
                ),
            )

    def get_project(self) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute("SELECT * FROM projects LIMIT 1").fetchone()

    def get_active_voice_preset_id(self, project_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT active_voice_preset_id
                FROM projects
                WHERE project_id = ?
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if not row or not row["active_voice_preset_id"]:
                return None
            return str(row["active_voice_preset_id"])

    def set_active_voice_preset_id(
        self,
        project_id: str,
        preset_id: str | None,
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET active_voice_preset_id = ?, updated_at = ?
                WHERE project_id = ?
                """,
                (preset_id, updated_at or _utc_now_iso(), project_id),
            )

    def get_active_export_preset_id(self, project_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT active_export_preset_id
                FROM projects
                WHERE project_id = ?
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if not row or not row["active_export_preset_id"]:
                return None
            return str(row["active_export_preset_id"])

    def set_active_export_preset_id(
        self,
        project_id: str,
        preset_id: str | None,
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET active_export_preset_id = ?, updated_at = ?
                WHERE project_id = ?
                """,
                (preset_id, updated_at or _utc_now_iso(), project_id),
            )

    def get_active_watermark_profile_id(self, project_id: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT active_watermark_profile_id
                FROM projects
                WHERE project_id = ?
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if not row or not row["active_watermark_profile_id"]:
                return None
            return str(row["active_watermark_profile_id"])

    def set_active_watermark_profile_id(
        self,
        project_id: str,
        profile_id: str | None,
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET active_watermark_profile_id = ?, updated_at = ?
                WHERE project_id = ?
                """,
                (profile_id, updated_at or _utc_now_iso(), project_id),
            )

    def update_project_video_asset(self, project_id: str, asset_id: str, updated_at: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET video_asset_id = ?, updated_at = ?
                WHERE project_id = ?
                """,
                (asset_id, updated_at, project_id),
            )

    def find_media_asset_by_path(
        self,
        *,
        project_id: str,
        asset_type: str,
        path: str,
    ) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM media_assets
                WHERE project_id = ? AND type = ? AND path = ?
                LIMIT 1
                """,
                (project_id, asset_type, path),
            ).fetchone()

    def update_media_asset(self, asset: MediaAssetRecord) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE media_assets
                SET type = ?,
                    path = ?,
                    sha256 = ?,
                    duration_ms = ?,
                    fps = ?,
                    width = ?,
                    height = ?,
                    audio_channels = ?,
                    sample_rate = ?,
                    created_at = ?
                WHERE asset_id = ?
                """,
                (
                    asset.asset_type,
                    asset.path,
                    asset.sha256,
                    asset.duration_ms,
                    asset.fps,
                    asset.width,
                    asset.height,
                    asset.audio_channels,
                    asset.sample_rate,
                    asset.created_at,
                    asset.asset_id,
                ),
            )

    def get_media_asset(self, asset_id: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                "SELECT * FROM media_assets WHERE asset_id = ? LIMIT 1",
                (asset_id,),
            ).fetchone()

    def get_primary_video_asset(self, project_id: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT ma.*
                FROM projects p
                JOIN media_assets ma ON ma.asset_id = p.video_asset_id
                WHERE p.project_id = ?
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()

    def replace_segments(self, project_id: str, segments: list[SegmentRecord]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM segments WHERE project_id = ?", (project_id,))
            connection.executemany(
                """
                INSERT INTO segments(
                    segment_id, project_id, segment_index, start_ms, end_ms,
                    source_lang, target_lang, source_text, source_text_norm,
                    translated_text, translated_text_norm, subtitle_text, tts_text,
                    audio_path, status, meta_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        segment.segment_id,
                        segment.project_id,
                        segment.segment_index,
                        segment.start_ms,
                        segment.end_ms,
                        segment.source_lang,
                        segment.target_lang,
                        segment.source_text,
                        segment.source_text_norm,
                        segment.translated_text,
                        segment.translated_text_norm,
                        segment.subtitle_text,
                        segment.tts_text,
                        segment.audio_path,
                        segment.status,
                        json.dumps(segment.meta_json),
                    )
                    for segment in segments
                ],
            )

    def count_segments(self, project_id: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM segments WHERE project_id = ?",
                (project_id,),
            ).fetchone()
        return int(row[0]) if row else 0

    def list_segments(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return self._list_segments_in_connection(connection, project_id)

    def _list_segments_in_connection(
        self,
        connection: sqlite3.Connection,
        project_id: str,
    ) -> list[sqlite3.Row]:
        return connection.execute(
            """
            SELECT *
            FROM segments
            WHERE project_id = ?
            ORDER BY segment_index ASC, start_ms ASC
            """,
            (project_id,),
        ).fetchall()

    def apply_segment_translations(
        self,
        project_id: str,
        translations: list[dict[str, object]],
    ) -> None:
        if not translations:
            return

        with self.connect() as connection:
            for item in translations:
                connection.execute(
                    """
                    UPDATE segments
                    SET target_lang = ?,
                        translated_text = ?,
                        translated_text_norm = ?,
                        subtitle_text = ?,
                        tts_text = ?,
                        status = ?
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (
                        item.get("target_lang"),
                        item.get("translated_text", ""),
                        item.get("translated_text_norm", ""),
                        item.get("subtitle_text", ""),
                        item.get("tts_text", ""),
                        item.get("status", "translated"),
                        project_id,
                        item["segment_id"],
                    ),
                )

    def apply_segment_edits(
        self,
        project_id: str,
        edits: list[dict[str, object]],
    ) -> None:
        if not edits:
            return

        with self.connect() as connection:
            for item in edits:
                translated_text = str(item.get("translated_text", ""))
                subtitle_text = str(item.get("subtitle_text", ""))
                tts_text = str(item.get("tts_text", ""))
                connection.execute(
                    """
                    UPDATE segments
                    SET start_ms = ?,
                        end_ms = ?,
                        translated_text = ?,
                        translated_text_norm = ?,
                        subtitle_text = ?,
                        tts_text = ?,
                        status = ?
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (
                        int(item["start_ms"]),
                        int(item["end_ms"]),
                        translated_text,
                        " ".join(translated_text.split()),
                        subtitle_text,
                        tts_text,
                        str(item.get("status", "edited")),
                        project_id,
                        str(item["segment_id"]),
                    ),
                )

    def apply_segment_audio_paths(
        self,
        project_id: str,
        items: list[dict[str, object]],
    ) -> None:
        if not items:
            return

        with self.connect() as connection:
            for item in items:
                connection.execute(
                    """
                    UPDATE segments
                    SET audio_path = ?,
                        status = COALESCE(?, status)
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (
                        str(item["audio_path"]) if item.get("audio_path") else None,
                        item.get("status"),
                        project_id,
                        str(item["segment_id"]),
                    ),
                )

    def _backfill_subtitle_tracks(self, connection: sqlite3.Connection) -> None:
        projects = connection.execute(
            """
            SELECT project_id, created_at, updated_at, active_subtitle_track_id
            FROM projects
            """
        ).fetchall()
        for project in projects:
            project_id = str(project["project_id"])
            canonical_track_id = self.build_canonical_subtitle_track_id(project_id)
            self._ensure_subtitle_track_row(
                connection,
                SubtitleTrackRecord(
                    track_id=canonical_track_id,
                    project_id=project_id,
                    name="Canonical Subtitle Track",
                    kind=CANONICAL_SUBTITLE_TRACK_KIND,
                    notes="Mirror subtitle track generated from canonical ASR/translation segments.",
                    created_at=str(project["created_at"]),
                    updated_at=str(project["updated_at"]),
                ),
            )
            event_count = self._count_subtitle_events_for_track(
                connection,
                project_id,
                canonical_track_id,
            )
            if event_count == 0:
                events = self._build_events_from_segments(connection, project_id, canonical_track_id)
                self._replace_subtitle_events_for_track(
                    connection,
                    project_id,
                    canonical_track_id,
                    events,
                    updated_at=str(project["updated_at"]),
                )

            active_track_id = str(project["active_subtitle_track_id"] or "").strip()
            if not active_track_id or not self._subtitle_track_exists(connection, active_track_id):
                self._set_active_subtitle_track_in_connection(
                    connection,
                    project_id,
                    canonical_track_id,
                    updated_at=str(project["updated_at"]),
                )

    def create_subtitle_track(
        self,
        track: SubtitleTrackRecord,
        *,
        set_active: bool = False,
    ) -> sqlite3.Row:
        with self.connect() as connection:
            self._ensure_subtitle_track_row(connection, track)
            if set_active:
                self._set_active_subtitle_track_in_connection(
                    connection,
                    track.project_id,
                    track.track_id,
                    updated_at=track.updated_at,
                )
            return self._get_subtitle_track_in_connection(connection, track.track_id)

    def ensure_canonical_subtitle_track(
        self,
        project_id: str,
        *,
        updated_at: str | None = None,
    ) -> sqlite3.Row:
        with self.connect() as connection:
            return self._ensure_canonical_subtitle_track_in_connection(
                connection,
                project_id,
                updated_at=updated_at or _utc_now_iso(),
            )

    def sync_canonical_subtitle_track(
        self,
        project_id: str,
        *,
        updated_at: str | None = None,
    ) -> sqlite3.Row:
        effective_updated_at = updated_at or _utc_now_iso()
        with self.connect() as connection:
            track_row = self._ensure_canonical_subtitle_track_in_connection(
                connection,
                project_id,
                updated_at=effective_updated_at,
            )
            events = self._build_events_from_segments(
                connection,
                project_id,
                str(track_row["track_id"]),
            )
            self._replace_subtitle_events_for_track(
                connection,
                project_id,
                str(track_row["track_id"]),
                events,
                updated_at=effective_updated_at,
            )
            return self._get_subtitle_track_in_connection(connection, str(track_row["track_id"]))

    def get_subtitle_track(self, track_id: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return self._get_subtitle_track_in_connection(connection, track_id)

    def list_subtitle_tracks(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM subtitle_tracks
                WHERE project_id = ?
                ORDER BY CASE WHEN kind = ? THEN 0 ELSE 1 END, updated_at DESC, track_id ASC
                """,
                (project_id, CANONICAL_SUBTITLE_TRACK_KIND),
            ).fetchall()

    def get_active_subtitle_track(self, project_id: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return self._get_active_subtitle_track_in_connection(connection, project_id)

    def set_active_subtitle_track(
        self,
        project_id: str,
        track_id: str,
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            self._set_active_subtitle_track_in_connection(
                connection,
                project_id,
                track_id,
                updated_at=updated_at or _utc_now_iso(),
            )

    def list_subtitle_events(
        self,
        project_id: str,
        *,
        track_id: str | None = None,
    ) -> list[sqlite3.Row]:
        with self.connect() as connection:
            resolved_track_id = track_id or self._resolve_active_track_id(connection, project_id)
            if not resolved_track_id:
                return []
            return self._list_subtitle_events_for_track(connection, project_id, resolved_track_id)

    def count_subtitle_events(
        self,
        project_id: str,
        *,
        track_id: str | None = None,
    ) -> int:
        with self.connect() as connection:
            resolved_track_id = track_id or self._resolve_active_track_id(connection, project_id)
            if not resolved_track_id:
                return 0
            return self._count_subtitle_events_for_track(connection, project_id, resolved_track_id)

    def replace_subtitle_events(
        self,
        project_id: str,
        track_id: str,
        events: list[SubtitleEventRecord],
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            self._replace_subtitle_events_for_track(
                connection,
                project_id,
                track_id,
                events,
                updated_at=updated_at or _utc_now_iso(),
            )

    def apply_subtitle_event_audio_paths(
        self,
        project_id: str,
        track_id: str,
        items: list[dict[str, object]],
    ) -> None:
        if not items:
            return

        with self.connect() as connection:
            for item in items:
                connection.execute(
                    """
                    UPDATE subtitle_events
                    SET audio_path = ?,
                        status = COALESCE(?, status)
                    WHERE project_id = ? AND track_id = ? AND event_id = ?
                    """,
                    (
                        str(item["audio_path"]) if item.get("audio_path") else None,
                        item.get("status"),
                        project_id,
                        track_id,
                        str(item["segment_id"]),
                    ),
                )
            connection.execute(
                """
                UPDATE subtitle_tracks
                SET updated_at = ?
                WHERE track_id = ? AND project_id = ?
                """,
                (_utc_now_iso(), track_id, project_id),
            )

    def _ensure_subtitle_track_row(
        self,
        connection: sqlite3.Connection,
        track: SubtitleTrackRecord,
    ) -> None:
        connection.execute(
            """
            INSERT INTO subtitle_tracks(
                track_id, project_id, name, kind, notes, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(track_id) DO UPDATE SET
                name = excluded.name,
                kind = excluded.kind,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                track.track_id,
                track.project_id,
                track.name,
                track.kind,
                track.notes,
                track.created_at,
                track.updated_at,
            ),
        )

    def _subtitle_track_exists(self, connection: sqlite3.Connection, track_id: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM subtitle_tracks WHERE track_id = ? LIMIT 1",
            (track_id,),
        ).fetchone()
        return row is not None

    def _get_subtitle_track_in_connection(
        self,
        connection: sqlite3.Connection,
        track_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT *
            FROM subtitle_tracks
            WHERE track_id = ?
            LIMIT 1
            """,
            (track_id,),
        ).fetchone()

    def _ensure_canonical_subtitle_track_in_connection(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        *,
        updated_at: str,
    ) -> sqlite3.Row:
        project_row = connection.execute(
            """
            SELECT created_at, updated_at
            FROM projects
            WHERE project_id = ?
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if not project_row:
            raise ValueError(f"Khong tim thay project: {project_id}")

        track_id = self.build_canonical_subtitle_track_id(project_id)
        self._ensure_subtitle_track_row(
            connection,
            SubtitleTrackRecord(
                track_id=track_id,
                project_id=project_id,
                name="Canonical Subtitle Track",
                kind=CANONICAL_SUBTITLE_TRACK_KIND,
                notes="Mirror subtitle track generated from canonical ASR/translation segments.",
                created_at=str(project_row["created_at"]),
                updated_at=updated_at or str(project_row["updated_at"]),
            ),
        )
        active_track_id = self._resolve_active_track_id(connection, project_id)
        if not active_track_id:
            self._set_active_subtitle_track_in_connection(
                connection,
                project_id,
                track_id,
                updated_at=updated_at,
            )
        return self._get_subtitle_track_in_connection(connection, track_id)

    def _get_active_subtitle_track_in_connection(
        self,
        connection: sqlite3.Connection,
        project_id: str,
    ) -> sqlite3.Row | None:
        row = connection.execute(
            """
            SELECT st.*
            FROM projects p
            JOIN subtitle_tracks st ON st.track_id = p.active_subtitle_track_id
            WHERE p.project_id = ?
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if row is not None:
            return row
        canonical_track_id = self.build_canonical_subtitle_track_id(project_id)
        return self._get_subtitle_track_in_connection(connection, canonical_track_id)

    def _resolve_active_track_id(
        self,
        connection: sqlite3.Connection,
        project_id: str,
    ) -> str | None:
        row = connection.execute(
            """
            SELECT active_subtitle_track_id
            FROM projects
            WHERE project_id = ?
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if row and row["active_subtitle_track_id"]:
            track_id = str(row["active_subtitle_track_id"])
            if self._subtitle_track_exists(connection, track_id):
                return track_id
        canonical_track_id = self.build_canonical_subtitle_track_id(project_id)
        if self._subtitle_track_exists(connection, canonical_track_id):
            return canonical_track_id
        return None

    def _set_active_subtitle_track_in_connection(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        track_id: str,
        *,
        updated_at: str,
    ) -> None:
        if not self._subtitle_track_exists(connection, track_id):
            raise ValueError(f"Khong tim thay subtitle track: {track_id}")
        connection.execute(
            """
            UPDATE projects
            SET active_subtitle_track_id = ?, updated_at = ?
            WHERE project_id = ?
            """,
            (track_id, updated_at, project_id),
        )

    def _build_events_from_segments(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        track_id: str,
    ) -> list[SubtitleEventRecord]:
        segment_rows = self._list_segments_in_connection(connection, project_id)
        events: list[SubtitleEventRecord] = []
        for row in segment_rows:
            raw_meta = row["meta_json"] or "{}"
            events.append(
                SubtitleEventRecord(
                    event_id=str(row["segment_id"]),
                    track_id=track_id,
                    project_id=project_id,
                    source_segment_id=str(row["segment_id"]),
                    event_index=int(row["segment_index"]),
                    start_ms=int(row["start_ms"]),
                    end_ms=int(row["end_ms"]),
                    source_lang=row["source_lang"],
                    target_lang=row["target_lang"],
                    source_text=row["source_text"] or "",
                    source_text_norm=row["source_text_norm"] or "",
                    translated_text=row["translated_text"] or "",
                    translated_text_norm=row["translated_text_norm"] or "",
                    subtitle_text=row["subtitle_text"] or "",
                    tts_text=row["tts_text"] or "",
                    audio_path=row["audio_path"],
                    status=row["status"] or "draft",
                    meta_json={} if not raw_meta else json.loads(raw_meta),
                )
            )
        return events

    def _replace_subtitle_events_for_track(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        track_id: str,
        events: list[SubtitleEventRecord],
        *,
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            DELETE FROM subtitle_events
            WHERE project_id = ? AND track_id = ?
            """,
            (project_id, track_id),
        )
        if events:
            connection.executemany(
                """
                INSERT INTO subtitle_events(
                    event_id, track_id, project_id, source_segment_id, event_index,
                    start_ms, end_ms, source_lang, target_lang, source_text,
                    source_text_norm, translated_text, translated_text_norm,
                    subtitle_text, tts_text, audio_path, status, meta_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        event.event_id,
                        event.track_id,
                        event.project_id,
                        event.source_segment_id,
                        event.event_index,
                        event.start_ms,
                        event.end_ms,
                        event.source_lang,
                        event.target_lang,
                        event.source_text,
                        event.source_text_norm,
                        event.translated_text,
                        event.translated_text_norm,
                        event.subtitle_text,
                        event.tts_text,
                        event.audio_path,
                        event.status,
                        json.dumps(event.meta_json),
                    )
                    for event in events
                ],
            )
        connection.execute(
            """
            UPDATE subtitle_tracks
            SET updated_at = ?
            WHERE track_id = ? AND project_id = ?
            """,
            (updated_at, track_id, project_id),
        )

    def _list_subtitle_events_for_track(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        track_id: str,
    ) -> list[sqlite3.Row]:
        return connection.execute(
            """
            SELECT
                se.event_id,
                se.event_id AS segment_id,
                se.track_id,
                se.source_segment_id,
                se.event_index,
                se.event_index AS segment_index,
                se.start_ms,
                se.end_ms,
                se.source_lang,
                se.target_lang,
                se.source_text,
                se.source_text_norm,
                se.translated_text,
                se.translated_text_norm,
                se.subtitle_text,
                se.tts_text,
                se.audio_path,
                se.status,
                se.meta_json,
                st.name AS track_name,
                st.kind AS track_kind
            FROM subtitle_events se
            JOIN subtitle_tracks st ON st.track_id = se.track_id
            WHERE se.project_id = ? AND se.track_id = ?
            ORDER BY se.event_index ASC, se.start_ms ASC
            """,
            (project_id, track_id),
        ).fetchall()

    def _count_subtitle_events_for_track(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        track_id: str,
    ) -> int:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM subtitle_events
            WHERE project_id = ? AND track_id = ?
            """,
            (project_id, track_id),
        ).fetchone()
        return int(row[0]) if row else 0

    def list_job_runs(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                "SELECT * FROM job_runs ORDER BY started_at DESC, job_id DESC"
            ).fetchall()
