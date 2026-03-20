from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.project.models import (
    CharacterProfileRecord,
    JobRunRecord,
    MediaAssetRecord,
    ProjectRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentRecord,
    SegmentAnalysisRecord,
    SpeakerBindingRecord,
    SubtitleEventRecord,
    SubtitleTrackRecord,
    VoicePolicyRecord,
)

SCHEMA_VERSION = 7
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
    translation_mode TEXT NOT NULL DEFAULT 'legacy',
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

CREATE TABLE IF NOT EXISTS character_profiles (
    character_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    canonical_name_zh TEXT NOT NULL DEFAULT '',
    canonical_name_vi TEXT NOT NULL DEFAULT '',
    aliases_json TEXT NOT NULL DEFAULT '[]',
    gender_hint TEXT,
    age_role TEXT,
    social_role TEXT,
    speech_style TEXT,
    default_register_profile_json TEXT NOT NULL DEFAULT '{}',
    default_self_terms_json TEXT NOT NULL DEFAULT '[]',
    default_address_terms_json TEXT NOT NULL DEFAULT '[]',
    forbidden_terms_json TEXT NOT NULL DEFAULT '[]',
    evidence_segment_ids_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'hypothesized',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS relationship_profiles (
    relationship_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    from_character_id TEXT NOT NULL,
    to_character_id TEXT NOT NULL,
    relation_type TEXT NOT NULL DEFAULT 'unknown',
    power_delta TEXT,
    age_delta TEXT,
    intimacy_level TEXT,
    default_self_term TEXT,
    default_address_term TEXT,
    allowed_alternates_json TEXT NOT NULL DEFAULT '[]',
    scope TEXT NOT NULL DEFAULT 'scene',
    status TEXT NOT NULL DEFAULT 'hypothesized',
    evidence_segment_ids_json TEXT NOT NULL DEFAULT '[]',
    last_updated_scene_id TEXT,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS scene_memories (
    scene_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    scene_index INTEGER NOT NULL,
    start_segment_index INTEGER NOT NULL,
    end_segment_index INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    participants_json TEXT NOT NULL DEFAULT '[]',
    location TEXT,
    time_context TEXT,
    short_scene_summary TEXT NOT NULL DEFAULT '',
    recent_turn_digest TEXT NOT NULL DEFAULT '',
    active_topic TEXT,
    current_conflict TEXT,
    current_emotional_tone TEXT,
    temporary_addressing_mode TEXT,
    who_knows_what_json TEXT NOT NULL DEFAULT '{}',
    open_ambiguities_json TEXT NOT NULL DEFAULT '[]',
    unresolved_references_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'planned',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS segment_analyses (
    segment_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    scene_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    speaker_json TEXT NOT NULL DEFAULT '{}',
    listeners_json TEXT NOT NULL DEFAULT '[]',
    register_json TEXT NOT NULL DEFAULT '{}',
    turn_function TEXT,
    resolved_ellipsis_json TEXT NOT NULL DEFAULT '{}',
    honorific_policy_json TEXT NOT NULL DEFAULT '{}',
    semantic_translation TEXT NOT NULL DEFAULT '',
    glossary_hits_json TEXT NOT NULL DEFAULT '[]',
    risk_flags_json TEXT NOT NULL DEFAULT '[]',
    confidence_json TEXT NOT NULL DEFAULT '{}',
    needs_human_review INTEGER NOT NULL DEFAULT 0,
    review_status TEXT NOT NULL DEFAULT 'draft',
    review_scope TEXT,
    review_reason_codes_json TEXT NOT NULL DEFAULT '[]',
    review_question TEXT NOT NULL DEFAULT '',
    approved_subtitle_text TEXT NOT NULL DEFAULT '',
    approved_tts_text TEXT NOT NULL DEFAULT '',
    semantic_qc_passed INTEGER NOT NULL DEFAULT 0,
    semantic_qc_issues_json TEXT NOT NULL DEFAULT '[]',
    source_template_family_id TEXT,
    adaptation_template_family_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
    FOREIGN KEY(scene_id) REFERENCES scene_memories(scene_id) ON DELETE CASCADE,
    FOREIGN KEY(segment_id) REFERENCES segments(segment_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_character_profiles_project_id ON character_profiles(project_id);
CREATE INDEX IF NOT EXISTS idx_relationship_profiles_project_id ON relationship_profiles(project_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_relationship_profiles_direction ON relationship_profiles(project_id, from_character_id, to_character_id);
CREATE INDEX IF NOT EXISTS idx_scene_memories_project_id ON scene_memories(project_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_scene_memories_project_scene_index ON scene_memories(project_id, scene_index);
CREATE INDEX IF NOT EXISTS idx_segment_analyses_project_id ON segment_analyses(project_id);
CREATE INDEX IF NOT EXISTS idx_segment_analyses_scene_id ON segment_analyses(scene_id);

CREATE TABLE IF NOT EXISTS speaker_bindings (
    binding_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    speaker_type TEXT NOT NULL DEFAULT 'character',
    speaker_key TEXT NOT NULL,
    voice_preset_id TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_speaker_bindings_unique
ON speaker_bindings(project_id, speaker_type, speaker_key);
CREATE INDEX IF NOT EXISTS idx_speaker_bindings_project_id ON speaker_bindings(project_id);

CREATE TABLE IF NOT EXISTS voice_policies (
    policy_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    policy_scope TEXT NOT NULL DEFAULT 'character',
    speaker_character_id TEXT NOT NULL,
    listener_character_id TEXT NOT NULL DEFAULT '',
    voice_preset_id TEXT NOT NULL,
    speed_override REAL,
    volume_override REAL,
    pitch_override REAL,
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_voice_policies_unique
ON voice_policies(project_id, policy_scope, speaker_character_id, listener_character_id);
CREATE INDEX IF NOT EXISTS idx_voice_policies_project_id ON voice_policies(project_id);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(raw_value: object, default: object) -> object:
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


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
            self._ensure_voice_policy_columns(connection)
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
        if "translation_mode" not in columns:
            connection.execute(
                "ALTER TABLE projects ADD COLUMN translation_mode TEXT NOT NULL DEFAULT 'legacy'"
            )
        connection.execute(
            """
            UPDATE projects
            SET translation_mode = CASE
                WHEN lower(source_language) = 'zh' AND lower(target_language) = 'vi' THEN 'contextual_v2'
                ELSE 'legacy'
            END
            WHERE translation_mode IS NULL OR trim(translation_mode) = ''
            """
        )

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

    def _ensure_voice_policy_columns(self, connection: sqlite3.Connection) -> None:
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(voice_policies)").fetchall()
        }
        if "speed_override" not in columns:
            connection.execute("ALTER TABLE voice_policies ADD COLUMN speed_override REAL")
        if "volume_override" not in columns:
            connection.execute("ALTER TABLE voice_policies ADD COLUMN volume_override REAL")
        if "pitch_override" not in columns:
            connection.execute("ALTER TABLE voice_policies ADD COLUMN pitch_override REAL")

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
                    project_id, name, root_dir, source_language, target_language, translation_mode,
                    video_asset_id, active_subtitle_track_id, active_voice_preset_id,
                    active_export_preset_id, active_watermark_profile_id, notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.project_id,
                    project.name,
                    project.root_dir,
                    project.source_language,
                    project.target_language,
                    project.translation_mode,
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

    def get_translation_mode(self, project_id: str) -> str:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT translation_mode
                FROM projects
                WHERE project_id = ?
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            if not row or not row["translation_mode"]:
                return "legacy"
            return str(row["translation_mode"])

    def set_translation_mode(
        self,
        project_id: str,
        translation_mode: str,
        *,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET translation_mode = ?, updated_at = ?
                WHERE project_id = ?
                """,
                (translation_mode, updated_at or _utc_now_iso(), project_id),
            )

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

    def replace_speaker_bindings(self, project_id: str, bindings: list[SpeakerBindingRecord]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM speaker_bindings WHERE project_id = ?", (project_id,))
            if not bindings:
                return
            connection.executemany(
                """
                INSERT INTO speaker_bindings(
                    binding_id, project_id, speaker_type, speaker_key, voice_preset_id,
                    notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        binding.binding_id,
                        binding.project_id,
                        binding.speaker_type,
                        binding.speaker_key,
                        binding.voice_preset_id,
                        binding.notes,
                        binding.created_at,
                        binding.updated_at,
                    )
                    for binding in bindings
                ],
            )

    def list_speaker_bindings(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM speaker_bindings
                WHERE project_id = ?
                ORDER BY speaker_type ASC, speaker_key ASC
                """,
                (project_id,),
            ).fetchall()

    def replace_voice_policies(self, project_id: str, policies: list[VoicePolicyRecord]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM voice_policies WHERE project_id = ?", (project_id,))
            if policies:
                connection.executemany(
                    """
                    INSERT INTO voice_policies(
                        policy_id, project_id, policy_scope, speaker_character_id,
                        listener_character_id, voice_preset_id, speed_override,
                        volume_override, pitch_override, notes, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            policy.policy_id,
                            policy.project_id,
                            policy.policy_scope,
                            policy.speaker_character_id,
                            policy.listener_character_id or "",
                            policy.voice_preset_id,
                            policy.speed_override,
                            policy.volume_override,
                            policy.pitch_override,
                            policy.notes,
                            policy.created_at,
                            policy.updated_at,
                        )
                        for policy in policies
                    ],
                )

    def list_voice_policies(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM voice_policies
                WHERE project_id = ?
                ORDER BY policy_scope ASC, speaker_character_id ASC, listener_character_id ASC
                """,
                (project_id,),
            ).fetchall()

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

    def replace_scene_memories(self, project_id: str, scenes: list[SceneMemoryRecord]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM scene_memories WHERE project_id = ?", (project_id,))
            if scenes:
                connection.executemany(
                    """
                    INSERT INTO scene_memories(
                        scene_id, project_id, scene_index, start_segment_index, end_segment_index,
                        start_ms, end_ms, participants_json, location, time_context,
                        short_scene_summary, recent_turn_digest, active_topic, current_conflict,
                        current_emotional_tone, temporary_addressing_mode, who_knows_what_json,
                        open_ambiguities_json, unresolved_references_json, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            scene.scene_id,
                            scene.project_id,
                            scene.scene_index,
                            scene.start_segment_index,
                            scene.end_segment_index,
                            scene.start_ms,
                            scene.end_ms,
                            _json_dumps(scene.participants_json),
                            scene.location,
                            scene.time_context,
                            scene.short_scene_summary,
                            scene.recent_turn_digest,
                            scene.active_topic,
                            scene.current_conflict,
                            scene.current_emotional_tone,
                            scene.temporary_addressing_mode,
                            _json_dumps(scene.who_knows_what_json),
                            _json_dumps(scene.open_ambiguities_json),
                            _json_dumps(scene.unresolved_references_json),
                            scene.status,
                            scene.created_at,
                            scene.updated_at,
                        )
                        for scene in scenes
                    ],
                )

    def list_scene_memories(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM scene_memories
                WHERE project_id = ?
                ORDER BY scene_index ASC, start_segment_index ASC
                """,
                (project_id,),
            ).fetchall()

    def upsert_character_profiles(self, profiles: list[CharacterProfileRecord]) -> None:
        if not profiles:
            return
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO character_profiles(
                    character_id, project_id, canonical_name_zh, canonical_name_vi, aliases_json,
                    gender_hint, age_role, social_role, speech_style, default_register_profile_json,
                    default_self_terms_json, default_address_terms_json, forbidden_terms_json,
                    evidence_segment_ids_json, confidence, status, notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(character_id) DO UPDATE SET
                    canonical_name_zh = excluded.canonical_name_zh,
                    canonical_name_vi = excluded.canonical_name_vi,
                    aliases_json = excluded.aliases_json,
                    gender_hint = excluded.gender_hint,
                    age_role = excluded.age_role,
                    social_role = excluded.social_role,
                    speech_style = excluded.speech_style,
                    default_register_profile_json = excluded.default_register_profile_json,
                    default_self_terms_json = excluded.default_self_terms_json,
                    default_address_terms_json = excluded.default_address_terms_json,
                    forbidden_terms_json = excluded.forbidden_terms_json,
                    evidence_segment_ids_json = excluded.evidence_segment_ids_json,
                    confidence = excluded.confidence,
                    status = excluded.status,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        profile.character_id,
                        profile.project_id,
                        profile.canonical_name_zh,
                        profile.canonical_name_vi,
                        _json_dumps(profile.aliases_json),
                        profile.gender_hint,
                        profile.age_role,
                        profile.social_role,
                        profile.speech_style,
                        _json_dumps(profile.default_register_profile_json),
                        _json_dumps(profile.default_self_terms_json),
                        _json_dumps(profile.default_address_terms_json),
                        _json_dumps(profile.forbidden_terms_json),
                        _json_dumps(profile.evidence_segment_ids_json),
                        profile.confidence,
                        profile.status,
                        profile.notes,
                        profile.created_at,
                        profile.updated_at,
                    )
                    for profile in profiles
                ],
            )

    def list_character_profiles(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM character_profiles
                WHERE project_id = ?
                ORDER BY canonical_name_vi ASC, canonical_name_zh ASC, character_id ASC
                """,
                (project_id,),
            ).fetchall()

    def upsert_relationship_profiles(self, relationships: list[RelationshipProfileRecord]) -> None:
        if not relationships:
            return
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO relationship_profiles(
                    relationship_id, project_id, from_character_id, to_character_id, relation_type,
                    power_delta, age_delta, intimacy_level, default_self_term, default_address_term,
                    allowed_alternates_json, scope, status, evidence_segment_ids_json,
                    last_updated_scene_id, notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id, from_character_id, to_character_id) DO UPDATE SET
                    relationship_id = excluded.relationship_id,
                    from_character_id = excluded.from_character_id,
                    to_character_id = excluded.to_character_id,
                    relation_type = excluded.relation_type,
                    power_delta = excluded.power_delta,
                    age_delta = excluded.age_delta,
                    intimacy_level = excluded.intimacy_level,
                    default_self_term = excluded.default_self_term,
                    default_address_term = excluded.default_address_term,
                    allowed_alternates_json = excluded.allowed_alternates_json,
                    scope = excluded.scope,
                    status = excluded.status,
                    evidence_segment_ids_json = excluded.evidence_segment_ids_json,
                    last_updated_scene_id = excluded.last_updated_scene_id,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        relationship.relationship_id,
                        relationship.project_id,
                        relationship.from_character_id,
                        relationship.to_character_id,
                        relationship.relation_type,
                        relationship.power_delta,
                        relationship.age_delta,
                        relationship.intimacy_level,
                        relationship.default_self_term,
                        relationship.default_address_term,
                        _json_dumps(relationship.allowed_alternates_json),
                        relationship.scope,
                        relationship.status,
                        _json_dumps(relationship.evidence_segment_ids_json),
                        relationship.last_updated_scene_id,
                        relationship.notes,
                        relationship.created_at,
                        relationship.updated_at,
                    )
                    for relationship in relationships
                ],
            )

    def list_relationship_profiles(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM relationship_profiles
                WHERE project_id = ?
                ORDER BY from_character_id ASC, to_character_id ASC
                """,
                (project_id,),
            ).fetchall()

    def replace_segment_analyses(self, project_id: str, analyses: list[SegmentAnalysisRecord]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM segment_analyses WHERE project_id = ?", (project_id,))
            if analyses:
                connection.executemany(
                    """
                    INSERT INTO segment_analyses(
                        segment_id, project_id, scene_id, segment_index, speaker_json, listeners_json,
                        register_json, turn_function, resolved_ellipsis_json, honorific_policy_json,
                        semantic_translation, glossary_hits_json, risk_flags_json, confidence_json,
                        needs_human_review, review_status, review_scope, review_reason_codes_json,
                        review_question, approved_subtitle_text, approved_tts_text, semantic_qc_passed,
                        semantic_qc_issues_json, source_template_family_id, adaptation_template_family_id,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            analysis.segment_id,
                            analysis.project_id,
                            analysis.scene_id,
                            analysis.segment_index,
                            _json_dumps(analysis.speaker_json),
                            _json_dumps(analysis.listeners_json),
                            _json_dumps(analysis.register_json),
                            analysis.turn_function,
                            _json_dumps(analysis.resolved_ellipsis_json),
                            _json_dumps(analysis.honorific_policy_json),
                            analysis.semantic_translation,
                            _json_dumps(analysis.glossary_hits_json),
                            _json_dumps(analysis.risk_flags_json),
                            _json_dumps(analysis.confidence_json),
                            1 if analysis.needs_human_review else 0,
                            analysis.review_status,
                            analysis.review_scope,
                            _json_dumps(analysis.review_reason_codes_json),
                            analysis.review_question,
                            analysis.approved_subtitle_text,
                            analysis.approved_tts_text,
                            1 if analysis.semantic_qc_passed else 0,
                            _json_dumps(analysis.semantic_qc_issues_json),
                            analysis.source_template_family_id,
                            analysis.adaptation_template_family_id,
                            analysis.created_at,
                            analysis.updated_at,
                        )
                        for analysis in analyses
                    ],
                )

    def replace_contextual_translation_state(
        self,
        project_id: str,
        *,
        scenes: list[SceneMemoryRecord],
        analyses: list[SegmentAnalysisRecord],
        character_profiles: list[CharacterProfileRecord] | None = None,
        relationship_profiles: list[RelationshipProfileRecord] | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM segment_analyses WHERE project_id = ?", (project_id,))
            connection.execute("DELETE FROM scene_memories WHERE project_id = ?", (project_id,))
            if scenes:
                connection.executemany(
                    """
                    INSERT INTO scene_memories(
                        scene_id, project_id, scene_index, start_segment_index, end_segment_index,
                        start_ms, end_ms, participants_json, location, time_context,
                        short_scene_summary, recent_turn_digest, active_topic, current_conflict,
                        current_emotional_tone, temporary_addressing_mode, who_knows_what_json,
                        open_ambiguities_json, unresolved_references_json, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            scene.scene_id,
                            scene.project_id,
                            scene.scene_index,
                            scene.start_segment_index,
                            scene.end_segment_index,
                            scene.start_ms,
                            scene.end_ms,
                            _json_dumps(scene.participants_json),
                            scene.location,
                            scene.time_context,
                            scene.short_scene_summary,
                            scene.recent_turn_digest,
                            scene.active_topic,
                            scene.current_conflict,
                            scene.current_emotional_tone,
                            scene.temporary_addressing_mode,
                            _json_dumps(scene.who_knows_what_json),
                            _json_dumps(scene.open_ambiguities_json),
                            _json_dumps(scene.unresolved_references_json),
                            scene.status,
                            scene.created_at,
                            scene.updated_at,
                        )
                        for scene in scenes
                    ],
                )
            if analyses:
                connection.executemany(
                    """
                    INSERT INTO segment_analyses(
                        segment_id, project_id, scene_id, segment_index, speaker_json, listeners_json,
                        register_json, turn_function, resolved_ellipsis_json, honorific_policy_json,
                        semantic_translation, glossary_hits_json, risk_flags_json, confidence_json,
                        needs_human_review, review_status, review_scope, review_reason_codes_json,
                        review_question, approved_subtitle_text, approved_tts_text, semantic_qc_passed,
                        semantic_qc_issues_json, source_template_family_id, adaptation_template_family_id,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            analysis.segment_id,
                            analysis.project_id,
                            analysis.scene_id,
                            analysis.segment_index,
                            _json_dumps(analysis.speaker_json),
                            _json_dumps(analysis.listeners_json),
                            _json_dumps(analysis.register_json),
                            analysis.turn_function,
                            _json_dumps(analysis.resolved_ellipsis_json),
                            _json_dumps(analysis.honorific_policy_json),
                            analysis.semantic_translation,
                            _json_dumps(analysis.glossary_hits_json),
                            _json_dumps(analysis.risk_flags_json),
                            _json_dumps(analysis.confidence_json),
                            1 if analysis.needs_human_review else 0,
                            analysis.review_status,
                            analysis.review_scope,
                            _json_dumps(analysis.review_reason_codes_json),
                            analysis.review_question,
                            analysis.approved_subtitle_text,
                            analysis.approved_tts_text,
                            1 if analysis.semantic_qc_passed else 0,
                            _json_dumps(analysis.semantic_qc_issues_json),
                            analysis.source_template_family_id,
                            analysis.adaptation_template_family_id,
                            analysis.created_at,
                            analysis.updated_at,
                        )
                        for analysis in analyses
                    ],
                )
            if character_profiles:
                connection.executemany(
                    """
                    INSERT INTO character_profiles(
                        character_id, project_id, canonical_name_zh, canonical_name_vi, aliases_json,
                        gender_hint, age_role, social_role, speech_style, default_register_profile_json,
                        default_self_terms_json, default_address_terms_json, forbidden_terms_json,
                        evidence_segment_ids_json, confidence, status, notes, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(character_id) DO UPDATE SET
                        canonical_name_zh = excluded.canonical_name_zh,
                        canonical_name_vi = excluded.canonical_name_vi,
                        aliases_json = excluded.aliases_json,
                        gender_hint = excluded.gender_hint,
                        age_role = excluded.age_role,
                        social_role = excluded.social_role,
                        speech_style = excluded.speech_style,
                        default_register_profile_json = excluded.default_register_profile_json,
                        default_self_terms_json = excluded.default_self_terms_json,
                        default_address_terms_json = excluded.default_address_terms_json,
                        forbidden_terms_json = excluded.forbidden_terms_json,
                        evidence_segment_ids_json = excluded.evidence_segment_ids_json,
                        confidence = excluded.confidence,
                        status = excluded.status,
                        notes = excluded.notes,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            profile.character_id,
                            profile.project_id,
                            profile.canonical_name_zh,
                            profile.canonical_name_vi,
                            _json_dumps(profile.aliases_json),
                            profile.gender_hint,
                            profile.age_role,
                            profile.social_role,
                            profile.speech_style,
                            _json_dumps(profile.default_register_profile_json),
                            _json_dumps(profile.default_self_terms_json),
                            _json_dumps(profile.default_address_terms_json),
                            _json_dumps(profile.forbidden_terms_json),
                            _json_dumps(profile.evidence_segment_ids_json),
                            profile.confidence,
                            profile.status,
                            profile.notes,
                            profile.created_at,
                            profile.updated_at,
                        )
                        for profile in character_profiles
                    ],
                )
            if relationship_profiles:
                connection.executemany(
                    """
                    INSERT INTO relationship_profiles(
                        relationship_id, project_id, from_character_id, to_character_id, relation_type,
                        power_delta, age_delta, intimacy_level, default_self_term, default_address_term,
                        allowed_alternates_json, scope, status, evidence_segment_ids_json,
                        last_updated_scene_id, notes, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project_id, from_character_id, to_character_id) DO UPDATE SET
                        relationship_id = excluded.relationship_id,
                        from_character_id = excluded.from_character_id,
                        to_character_id = excluded.to_character_id,
                        relation_type = excluded.relation_type,
                        power_delta = excluded.power_delta,
                        age_delta = excluded.age_delta,
                        intimacy_level = excluded.intimacy_level,
                        default_self_term = excluded.default_self_term,
                        default_address_term = excluded.default_address_term,
                        allowed_alternates_json = excluded.allowed_alternates_json,
                        scope = excluded.scope,
                        status = excluded.status,
                        evidence_segment_ids_json = excluded.evidence_segment_ids_json,
                        last_updated_scene_id = excluded.last_updated_scene_id,
                        notes = excluded.notes,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            relationship.relationship_id,
                            relationship.project_id,
                            relationship.from_character_id,
                            relationship.to_character_id,
                            relationship.relation_type,
                            relationship.power_delta,
                            relationship.age_delta,
                            relationship.intimacy_level,
                            relationship.default_self_term,
                            relationship.default_address_term,
                            _json_dumps(relationship.allowed_alternates_json),
                            relationship.scope,
                            relationship.status,
                            _json_dumps(relationship.evidence_segment_ids_json),
                            relationship.last_updated_scene_id,
                            relationship.notes,
                            relationship.created_at,
                            relationship.updated_at,
                        )
                        for relationship in relationship_profiles
                    ],
                )

    def list_segment_analyses(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM segment_analyses
                WHERE project_id = ?
                ORDER BY segment_index ASC, segment_id ASC
                """,
                (project_id,),
            ).fetchall()

    def get_segment_analysis(self, project_id: str, segment_id: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM segment_analyses
                WHERE project_id = ? AND segment_id = ?
                LIMIT 1
                """,
                (project_id, segment_id),
            ).fetchone()

    def count_pending_segment_reviews(self, project_id: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM segment_analyses
                WHERE project_id = ? AND (needs_human_review = 1 OR review_status = 'needs_review')
                """,
                (project_id,),
            ).fetchone()
            return int(row[0]) if row else 0

    def list_review_queue_items(self, project_id: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT
                    sa.*,
                    s.source_text,
                    s.start_ms,
                    s.end_ms,
                    sm.scene_index,
                    sm.short_scene_summary,
                    sm.open_ambiguities_json
                FROM segment_analyses sa
                JOIN segments s ON s.segment_id = sa.segment_id
                JOIN scene_memories sm ON sm.scene_id = sa.scene_id
                WHERE sa.project_id = ? AND (sa.needs_human_review = 1 OR sa.review_status = 'needs_review')
                ORDER BY sa.segment_index ASC
                """,
                (project_id,),
            ).fetchall()

    def update_segment_analysis_review(
        self,
        project_id: str,
        segment_id: str,
        *,
        speaker_json: dict[str, object] | None = None,
        listeners_json: list[dict[str, object]] | None = None,
        honorific_policy_json: dict[str, object] | None = None,
        approved_subtitle_text: str | None = None,
        approved_tts_text: str | None = None,
        needs_human_review: bool | None = None,
        review_status: str | None = None,
        review_scope: str | None = None,
        review_reason_codes_json: list[str] | None = None,
        review_question: str | None = None,
        semantic_qc_passed: bool | None = None,
        semantic_qc_issues_json: list[dict[str, object]] | None = None,
        updated_at: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE segment_analyses
                SET speaker_json = COALESCE(?, speaker_json),
                    listeners_json = COALESCE(?, listeners_json),
                    honorific_policy_json = COALESCE(?, honorific_policy_json),
                    approved_subtitle_text = COALESCE(?, approved_subtitle_text),
                    approved_tts_text = COALESCE(?, approved_tts_text),
                    needs_human_review = COALESCE(?, needs_human_review),
                    review_status = COALESCE(?, review_status),
                    review_scope = COALESCE(?, review_scope),
                    review_reason_codes_json = COALESCE(?, review_reason_codes_json),
                    review_question = COALESCE(?, review_question),
                    semantic_qc_passed = COALESCE(?, semantic_qc_passed),
                    semantic_qc_issues_json = COALESCE(?, semantic_qc_issues_json),
                    updated_at = ?
                WHERE project_id = ? AND segment_id = ?
                """,
                (
                    _json_dumps(speaker_json) if speaker_json is not None else None,
                    _json_dumps(listeners_json) if listeners_json is not None else None,
                    _json_dumps(honorific_policy_json) if honorific_policy_json is not None else None,
                    approved_subtitle_text,
                    approved_tts_text,
                    (1 if needs_human_review else 0) if needs_human_review is not None else None,
                    review_status,
                    review_scope,
                    _json_dumps(review_reason_codes_json) if review_reason_codes_json is not None else None,
                    review_question,
                    (1 if semantic_qc_passed else 0) if semantic_qc_passed is not None else None,
                    _json_dumps(semantic_qc_issues_json) if semantic_qc_issues_json is not None else None,
                    updated_at or _utc_now_iso(),
                    project_id,
                    segment_id,
                ),
            )

    def apply_segment_analysis_outputs(
        self,
        project_id: str,
        *,
        target_language: str | None = None,
        sync_subtitle_track: bool = True,
    ) -> None:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM segment_analyses
                WHERE project_id = ?
                ORDER BY segment_index ASC
                """,
                (project_id,),
            ).fetchall()
            for row in rows:
                raw_segment_meta = connection.execute(
                    "SELECT meta_json FROM segments WHERE project_id = ? AND segment_id = ? LIMIT 1",
                    (project_id, str(row["segment_id"])),
                ).fetchone()
                segment_meta = _json_loads(raw_segment_meta["meta_json"] if raw_segment_meta else "{}", {})
                if not isinstance(segment_meta, dict):
                    segment_meta = {}
                segment_meta["contextual_translation"] = {
                    "scene_id": row["scene_id"],
                    "review_status": row["review_status"],
                    "needs_human_review": bool(row["needs_human_review"]),
                    "semantic_qc_passed": bool(row["semantic_qc_passed"]),
                    "speaker": _json_loads(row["speaker_json"], {}),
                    "listeners": _json_loads(row["listeners_json"], []),
                    "honorific_policy": _json_loads(row["honorific_policy_json"], {}),
                    "semantic_qc_issues": _json_loads(row["semantic_qc_issues_json"], []),
                }
                translated_text = str(row["semantic_translation"] or "")
                subtitle_text = str(row["approved_subtitle_text"] or "")
                tts_text = str(row["approved_tts_text"] or "")
                status = "review_pending" if row["needs_human_review"] else "translated"
                connection.execute(
                    """
                    UPDATE segments
                    SET target_lang = COALESCE(?, target_lang),
                        translated_text = ?,
                        translated_text_norm = ?,
                        subtitle_text = ?,
                        tts_text = ?,
                        status = ?,
                        meta_json = ?
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (
                        target_language,
                        translated_text,
                        " ".join(translated_text.split()),
                        subtitle_text,
                        tts_text,
                        status,
                        _json_dumps(segment_meta),
                        project_id,
                        str(row["segment_id"]),
                    ),
                )
            if sync_subtitle_track:
                track_row = self._ensure_canonical_subtitle_track_in_connection(
                    connection,
                    project_id,
                    updated_at=_utc_now_iso(),
                )
                events = self._build_events_from_segments(connection, project_id, str(track_row["track_id"]))
                self._replace_subtitle_events_for_track(
                    connection,
                    project_id,
                    str(track_row["track_id"]),
                    events,
                    updated_at=_utc_now_iso(),
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
