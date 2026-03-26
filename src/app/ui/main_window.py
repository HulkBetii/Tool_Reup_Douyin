from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QItemSelectionModel, QSignalBlocker, QTimer, Qt, QUrl
from PySide6.QtGui import QColor, QDesktopServices
from PySide6.QtWidgets import (
    QAbstractScrollArea,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLayout,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from app.asr.faster_whisper_engine import FasterWhisperEngine
from app.asr.models import TranscriptionOptions
from app.asr.persistence import persist_transcription_result
from app.audio.mixdown import build_mixdown_stage_hash, mix_audio_tracks
from app.audio.voiceover_track import build_voice_track, build_voice_track_stage_hash
from app.core.ffmpeg import detect_ffmpeg_installation
from app.core.jobs import JobContext, JobManager, JobResult, JobStatus
from app.core.paths import get_appdata_dir, get_default_workspace_dir, get_user_downloads_dir
from app.core.settings import AppSettings, normalize_ui_mode, save_settings
from app.exporting.models import WatermarkProfile
from app.exporting.presets import (
    list_export_presets,
    list_watermark_profiles,
    save_watermark_profile,
)
from app.media.extract_audio import extract_audio_artifacts, load_cached_audio_artifacts
from app.media.ffprobe_service import attach_source_video_to_project, probe_media
from app.media.models import ExtractedAudioArtifacts, MediaMetadata
from app.ops.cache_ops import build_cache_inventory, cleanup_cache
from app.ops.doctor import format_blocked_message, run_doctor
from app.ops.project_safety import create_workspace_backup, get_backups_root, inspect_workspace, repair_workspace_metadata
from app.project.bootstrap import bootstrap_project, open_project, sync_project_snapshot, utc_now_iso
from app.project.database import (
    CANONICAL_SUBTITLE_TRACK_KIND,
    ProjectDatabase,
    USER_SUBTITLE_TRACK_KIND,
)
from app.project.profiles import (
    default_project_profiles,
    load_project_profile_state,
    resolve_subtitle_subtext_mode,
    set_project_subtitle_subtext_mode,
)
from app.project.models import (
    ProjectInitRequest,
    ProjectWorkspace,
    RegisterVoiceStylePolicyRecord,
    SpeakerBindingRecord,
    SubtitleTrackRecord,
    VoicePolicyRecord,
)
from app.project.runtime_state import restore_pipeline_state
from app.subtitle.editing import (
    build_subtitle_event_records,
    format_timestamp_ms,
    merge_editor_rows,
    parse_timestamp_ms,
    split_editor_row,
    suggest_subtitle_text,
    suggest_tts_text,
)
from app.subtitle.export import export_preview_subtitles, export_subtitles
from app.subtitle.hardsub import export_hardsub_video
from app.subtitle.preview import MpvPreviewController, PreviewUnavailableError
from app.subtitle.qc import SubtitleQcConfig, SubtitleQcIssue, SubtitleQcReport, analyze_subtitle_rows
from app.tts.base import build_tts_stage_hash
from app.tts.factory import create_tts_engine
from app.tts.pipeline import load_synthesized_segments, synthesize_segments
from app.tts.speaker_binding import (
    RegisterVoiceStyleCandidate,
    build_speaker_binding_plan,
    discover_register_voice_style_candidates,
    discover_relationship_voice_policy_candidates,
    discover_speaker_candidates,
    resolve_segment_voice_presets,
)
from app.translate.relationship_memory import build_locked_relationship_record, relationship_record_from_row
from app.tts.presets import (
    batch_import_voice_clone_presets,
    delete_voice_preset,
    list_voice_presets,
    save_voice_preset,
)
from app.tts.sapi_engine import list_installed_sapi_voices
from app.tts.vieneu_engine import detect_vieneu_installation, get_vieneu_mode
from app.translate.openai_engine import OpenAITranslationEngine
from app.translate.contextual_pipeline import (
    build_contextual_translation_stage_hash,
    load_cached_contextual_translation,
    persist_contextual_translation_result,
    recompute_semantic_qc,
    restore_cached_contextual_translation,
)
from app.translate.contextual_runtime import run_contextual_translation
from app.translate.persistence import (
    build_translation_stage_hash,
    load_cached_translations,
    persist_translations,
)
from app.translate.presets import ensure_prompt_templates, list_prompt_templates
from app.translate.review_resolution import apply_review_resolution, resolve_review_target_segment_ids
from app.ui.status_panel import StatusPanel
from app.version import APP_NAME, APP_VERSION

VOICE_POLICY_LABEL_COLUMN = 0
VOICE_POLICY_COUNT_COLUMN = 1
VOICE_POLICY_PRESET_COLUMN = 2
VOICE_POLICY_SPEED_COLUMN = 3
VOICE_POLICY_VOLUME_COLUMN = 4
VOICE_POLICY_PITCH_COLUMN = 5
VOICE_POLICY_STATUS_COLUMN = 6

REGISTER_STYLE_LABEL_COLUMN = 0
REGISTER_STYLE_COUNT_COLUMN = 1
REGISTER_STYLE_POLITENESS_COLUMN = 2
REGISTER_STYLE_POWER_COLUMN = 3
REGISTER_STYLE_EMOTION_COLUMN = 4
REGISTER_STYLE_TURN_COLUMN = 5
REGISTER_STYLE_RELATION_COLUMN = 6
REGISTER_STYLE_SPEED_COLUMN = 7
REGISTER_STYLE_VOLUME_COLUMN = 8
REGISTER_STYLE_PITCH_COLUMN = 9
REGISTER_STYLE_STATUS_COLUMN = 10

UI_MODE_SIMPLE_V2 = "simple_v2"
UI_MODE_ADVANCED = "advanced"
UI_MODE_LABELS = {
    UI_MODE_SIMPLE_V2: "Đơn giản (V2)",
    UI_MODE_ADVANCED: "Nâng cao",
}


class MainWindow(QMainWindow):
    def __init__(self, settings: AppSettings, job_manager: JobManager) -> None:
        super().__init__()
        self._settings = settings
        self._job_manager = job_manager
        self._current_workspace: ProjectWorkspace | None = None
        self._media_metadata: MediaMetadata | None = None
        self._audio_artifacts: ExtractedAudioArtifacts | None = None
        self._prompt_templates = []
        self._pending_review_segment_id: str | None = None
        self._last_subtitle_outputs: dict[str, Path] = {}
        self._last_export_output: Path | None = None
        self._subtitle_editor_loading = False
        self._subtitle_editor_dirty = False
        self._subtitle_segment_snapshot: dict[str, dict[str, object]] = {}
        self._subtitle_subtext_toggle: QCheckBox | None = None
        self._subtitle_qc_report = SubtitleQcReport(total_segments=0, issues=[])
        self._preview_controller = MpvPreviewController()
        self._preview_reload_timer = QTimer(self)
        self._preview_reload_timer.setSingleShot(True)
        self._preview_reload_timer.setInterval(450)
        self._preview_reload_timer.timeout.connect(self._flush_preview_reload)
        self._live_preview_ass_path: Path | None = None
        self._voice_presets = []
        self._installed_sapi_voices: list[str] = []
        self._vieneu_environment = detect_vieneu_installation()
        self._last_tts_manifest: Path | None = None
        self._last_voice_track_output: Path | None = None
        self._last_mixed_audio_output: Path | None = None
        self._export_presets = []
        self._watermark_profiles = []
        self._workflow_queue: list[str] = []
        self._workflow_current_stage: str | None = None
        self._workflow_current_job_id: str | None = None
        self._workflow_name: str | None = None
        self._speaker_binding_loading = False
        self._speaker_binding_dirty = False
        self._voice_policy_loading = False
        self._voice_policy_dirty = False
        self._last_doctor_report = None
        self._last_cache_inventory = None
        self._last_repair_report = None
        self._available_project_profiles = []
        self._ui_mode_combo: QComboBox | None = None
        self._settings_ui_mode_combo: QComboBox | None = None
        self._project_profile_combo: QComboBox | None = None
        self._project_root_browse_button: QPushButton | None = None
        self._project_name_user_edited = False
        self._project_root_user_edited = False
        self._default_project_dir_seed = datetime.now().strftime("video-moi-%Y%m%d-%H%M%S")

        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(1360, 860)

        self._tabs = QTabWidget()
        self._status_panel = StatusPanel()
        self._status_panel.cancel_requested.connect(self._job_manager.cancel_job)
        self._status_panel.retry_requested.connect(self._handle_retry_requested)
        self._job_manager.job_updated.connect(self._handle_job_updated)

        self._project_tab = self._build_project_tab()
        self._translate_tab = self._build_translate_tab()
        self._subtitle_tab = self._build_subtitle_tab()
        self._voiceover_tab = self._build_voiceover_tab()
        self._export_tab = self._build_export_tab()
        self._settings_tab = self._build_settings_tab()
        self._logs_tab = self._build_logs_tab()

        self._tabs.addTab(self._wrap_scrollable_tab(self._project_tab), "Dự án")
        self._tabs.addTab(self._wrap_scrollable_tab(self._translate_tab), "ASR & Dịch")
        self._tabs.addTab(self._wrap_scrollable_tab(self._subtitle_tab), "Phụ đề")
        self._tabs.addTab(self._wrap_scrollable_tab(self._voiceover_tab), "Lồng tiếng")
        self._tabs.addTab(self._wrap_scrollable_tab(self._export_tab), "Xuất bản")
        self._tabs.addTab(self._wrap_scrollable_tab(self._settings_tab), "Cài đặt")
        self._tabs.addTab(self._wrap_scrollable_tab(self._logs_tab), "Nhật ký")

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._tabs)
        splitter.addWidget(self._status_panel)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)

        self._ui_mode_bar = self._build_ui_mode_bar()

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(self._ui_mode_bar)
        layout.addWidget(splitter)
        self.setCentralWidget(container)

        self._sync_settings_to_form()
        self._reload_project_profile_options()
        self._sync_ui_mode_controls()
        self._apply_ui_mode()
        self._append_log_line("Khởi tạo giao diện hoàn tất")

    def _build_ui_mode_bar(self) -> QWidget:
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addStretch(1)
        layout.addWidget(QLabel("Chế độ giao diện"))
        self._ui_mode_combo = QComboBox()
        self._ui_mode_combo.addItem(UI_MODE_LABELS[UI_MODE_SIMPLE_V2], UI_MODE_SIMPLE_V2)
        self._ui_mode_combo.addItem(UI_MODE_LABELS[UI_MODE_ADVANCED], UI_MODE_ADVANCED)
        self._ui_mode_combo.currentIndexChanged.connect(self._handle_ui_mode_combo_changed)
        layout.addWidget(self._ui_mode_combo)
        return container

    def _current_ui_mode(self) -> str:
        return normalize_ui_mode(getattr(self._settings, "ui_mode", UI_MODE_SIMPLE_V2))

    def _is_simple_ui_mode(self) -> bool:
        return self._current_ui_mode() == UI_MODE_SIMPLE_V2

    def _sync_ui_mode_controls(self) -> None:
        mode = self._current_ui_mode()
        for combo in (self._ui_mode_combo, self._settings_ui_mode_combo):
            if combo is None:
                continue
            with QSignalBlocker(combo):
                index = combo.findData(mode)
                combo.setCurrentIndex(index if index >= 0 else 0)

    def _handle_ui_mode_combo_changed(self, index: int) -> None:
        del index
        combo = self.sender()
        if not isinstance(combo, QComboBox):
            return
        self._set_ui_mode(str(combo.currentData() or UI_MODE_SIMPLE_V2))

    def _set_ui_mode(self, mode: str, *, persist: bool = True) -> None:
        normalized_mode = normalize_ui_mode(mode)
        if getattr(self._settings, "ui_mode", UI_MODE_SIMPLE_V2) == normalized_mode:
            self._sync_ui_mode_controls()
            self._apply_ui_mode()
            return
        self._settings.ui_mode = normalized_mode
        self._sync_ui_mode_controls()
        self._reload_project_profile_options()
        self._apply_ui_mode()
        if persist:
            save_settings(self._settings)

    @staticmethod
    def _set_form_row_visible(form: QFormLayout, field: QWidget, visible: bool) -> None:
        label = form.labelForField(field)
        if label is not None:
            label.setVisible(visible)
        field.setVisible(visible)

    def _set_tab_visible(self, title: str, visible: bool) -> None:
        for index in range(self._tabs.count()):
            if self._tabs.tabText(index) == title:
                self._tabs.setTabVisible(index, visible)
                break

    def _apply_ui_mode(self) -> None:
        simple_mode = self._is_simple_ui_mode()
        self._set_tab_visible("Nhật ký", not simple_mode)

        self._project_smoke_button.setVisible(not simple_mode)
        self._project_ops_group.setVisible(not simple_mode)
        self._prepare_media_button.setText("1. Chuẩn bị video" if simple_mode else "Chuẩn bị media")
        self._asr_translate_button.setText("2. Tạo phụ đề" if simple_mode else "ASR -> Dịch")
        self._project_review_button.setVisible(simple_mode)
        self._project_finish_button.setVisible(simple_mode)
        self._dub_button.setVisible(not simple_mode)
        self._full_pipeline_button.setVisible(not simple_mode)
        self._stop_workflow_button.setText("Dừng" if simple_mode else "Dừng quy trình")

        self._set_form_row_visible(self._translate_form, self._asr_engine_combo, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._asr_model_combo, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._asr_language_combo, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._vad_checkbox, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._word_timestamps_checkbox, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._translation_mode_info, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._prompt_combo, not simple_mode)
        self._set_form_row_visible(self._translate_form, self._translation_model_input, not simple_mode)
        self._reload_prompts_button.setVisible(not simple_mode)

        self._subtitle_tools_container.setVisible(not simple_mode)
        for widget in (
            self._translated_to_subtitle_button,
            self._subtitle_to_tts_button,
            self._polish_tts_button,
            self._split_subtitle_button,
            self._merge_subtitle_button,
        ):
            widget.setVisible(not simple_mode)

        for field in (
            self._voice_profile_name_input,
            self._voice_engine_combo,
            self._voice_id_input,
            self._voice_language_input,
            self._voice_profile_numeric_container,
            self._voice_info,
            self._voice_preset_notes,
            self._voice_profile_status,
            self._voice_notes_input,
            self._voice_clone_status,
            self._voice_ref_audio_container,
            self._vieneu_ref_text_input,
            self._voice_profile_actions_container,
            self._speaker_binding_status,
            self._speaker_binding_hint,
            self._speaker_binding_table,
            self._speaker_binding_actions_container,
            self._voice_policy_status,
            self._voice_policy_hint,
            self._character_voice_policy_table,
            self._relationship_voice_policy_table,
            self._register_voice_style_status,
            self._register_voice_style_hint,
            self._register_voice_style_table,
            self._voice_policy_actions_container,
            self._effective_voice_plan_preview,
        ):
            self._set_form_row_visible(self._voiceover_form, field, not simple_mode)

        self._rerun_downstream_only_button.setVisible(not simple_mode)

        for field in (
            self._watermark_profile_combo,
            self._watermark_profile_name_input,
            self._watermark_profile_status,
            self._watermark_enabled_checkbox,
            self._watermark_path_container,
            self._watermark_position_combo,
            self._watermark_numeric_container,
            self._watermark_actions_container,
        ):
            self._set_form_row_visible(self._export_form, field, not simple_mode)
        self._reload_export_presets_button.setVisible(not simple_mode)
        self._sync_ui_mode_controls()
        self._apply_default_form_values()

    @staticmethod
    def _normalize_project_name(raw_value: str, *, fallback: str) -> str:
        normalized = raw_value.replace("_", " ")
        normalized = re.sub(r"\s+", " ", normalized).strip(" .-_")
        return normalized or fallback

    @staticmethod
    def _sanitize_project_dir_name(raw_value: str, *, fallback: str) -> str:
        normalized = re.sub(r'[<>:"/\\|?*#]+', " ", raw_value.replace("_", " "))
        normalized = re.sub(r"\s+", "-", normalized).strip(" .-_")
        if len(normalized) > 80:
            normalized = normalized[:80].rstrip(" .-_")
        return normalized or fallback

    def _default_project_display_name(self) -> str:
        return "Video mới"

    def _default_project_directory_name(self) -> str:
        return self._default_project_dir_seed

    def _suggest_project_name(self, source_video_path: Path | None = None) -> str:
        if source_video_path is not None and source_video_path.stem.strip():
            return self._normalize_project_name(
                source_video_path.stem,
                fallback=self._default_project_display_name(),
            )
        return self._default_project_display_name()

    def _suggest_project_root(
        self,
        *,
        source_video_path: Path | None = None,
        project_name: str | None = None,
    ) -> Path:
        workspace_root = get_default_workspace_dir()
        seed = project_name or (source_video_path.stem if source_video_path is not None else "")
        folder_name = self._sanitize_project_dir_name(
            seed,
            fallback=self._default_project_directory_name(),
        )
        return workspace_root / folder_name

    def _project_root_dialog_start_dir(self) -> Path:
        raw_value = self._project_root_input.text().strip()
        if raw_value:
            candidate = Path(raw_value).expanduser()
            if candidate.exists():
                return candidate
            if candidate.parent.exists():
                return candidate.parent
        if self._current_workspace is not None:
            return self._current_workspace.root_dir
        return get_default_workspace_dir()

    def _video_dialog_start_dir(self) -> Path:
        raw_value = self._source_video_input.text().strip()
        if raw_value:
            candidate = Path(raw_value).expanduser()
            if candidate.exists():
                return candidate.parent if candidate.is_file() else candidate
        if self._current_workspace and self._current_workspace.source_video_path:
            return self._current_workspace.source_video_path.parent
        downloads_dir = get_user_downloads_dir()
        if downloads_dir.exists():
            return downloads_dir
        return get_default_workspace_dir()

    def _apply_default_form_values(self) -> None:
        if self._current_workspace is None:
            suggested_root = self._suggest_project_root(project_name=self._project_name_input.text().strip() or None)
            if not self._project_name_user_edited and not self._project_name_input.text().strip():
                self._project_name_input.clear()
            if not self._project_root_user_edited:
                self._project_root_input.setText(str(suggested_root))
        if self._is_simple_ui_mode() and self._current_workspace is None:
            self._set_combo_text_if_present(self._source_lang_combo, "zh")
            self._set_combo_text_if_present(self._target_lang_combo, "vi")
            self._set_combo_text_if_present(self._asr_language_combo, "zh")
            self._original_volume_input.setText("0.07")
            self._voice_volume_input.setText("1.0")
            self._bgm_volume_input.setText("0.0")

    @staticmethod
    def _set_combo_text_if_present(combo: QComboBox, value: str) -> None:
        index = combo.findText(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _apply_profile_defaults_to_form(self, profile: object | None) -> None:
        if profile is None:
            return
        source_language = getattr(profile, "source_language", None)
        if source_language:
            self._set_combo_text_if_present(self._source_lang_combo, str(source_language))
            self._set_combo_text_if_present(self._asr_language_combo, str(source_language))
        target_language = getattr(profile, "target_language", None)
        if target_language:
            self._set_combo_text_if_present(self._target_lang_combo, str(target_language))
        recommended_original_volume = getattr(profile, "recommended_original_volume", None)
        if recommended_original_volume is not None:
            self._original_volume_input.setText(f"{float(recommended_original_volume):g}")
        recommended_voice_volume = getattr(profile, "recommended_voice_volume", None)
        if recommended_voice_volume is not None:
            self._voice_volume_input.setText(f"{float(recommended_voice_volume):g}")
        if getattr(profile, "project_profile_id", "") == "zh-vi-narration-fast-v2-vieneu":
            self._bgm_volume_input.setText("0.0")

    def _handle_project_name_edited(self, text: str) -> None:
        self._project_name_user_edited = True
        if self._project_root_user_edited:
            return
        suggested_root = self._suggest_project_root(project_name=text.strip() or None)
        self._project_root_input.setText(str(suggested_root))

    def _handle_project_root_edited(self, _text: str) -> None:
        self._project_root_user_edited = True

    def _apply_project_suggestions_from_source(self, source_video_path: Path) -> None:
        suggested_name = self._suggest_project_name(source_video_path)
        if not self._project_name_user_edited or not self._project_name_input.text().strip():
            self._project_name_input.setText(suggested_name)
        if not self._project_root_user_edited or not self._project_root_input.text().strip():
            suggested_root = self._suggest_project_root(
                source_video_path=source_video_path,
                project_name=self._project_name_input.text().strip() or suggested_name,
            )
            self._project_root_input.setText(str(suggested_root))

    def _build_project_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self._project_summary = self._create_info_label("Chưa mở dự án")
        self._media_summary = self._create_info_label("Chưa có video nguồn")
        self._pipeline_summary = self._create_info_label("Checklist quy trình chưa có dữ liệu")
        self._workflow_status = self._create_info_label("Quy trình nhanh: sẵn sàng")
        self._doctor_summary = self._create_info_label("Doctor: chưa chạy")
        self._workspace_repair_summary = self._create_info_label("Workspace safety: chưa kiểm tra")
        self._cache_ops_summary = self._create_info_label("Cache ops: chưa có dữ liệu")

        group = QGroupBox("Khởi tạo / mở dự án")
        self._project_init_group = group
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._project_name_input = QLineEdit()
        self._project_name_input.setPlaceholderText("Tự lấy từ tên video")
        self._project_name_input.textEdited.connect(self._handle_project_name_edited)
        self._project_root_input = QLineEdit(str(self._suggest_project_root()))
        self._project_root_input.textEdited.connect(self._handle_project_root_edited)
        self._source_video_input = QLineEdit()
        self._source_lang_combo = QComboBox()
        self._source_lang_combo.addItems(["auto", "vi", "zh", "en"])
        self._target_lang_combo = QComboBox()
        self._target_lang_combo.addItems(["vi", "zh", "en"])
        self._project_profile_combo = QComboBox()
        self._project_profile_combo.currentIndexChanged.connect(self._handle_project_profile_changed)

        browse_button = QPushButton("Chọn")
        self._project_root_browse_button = browse_button
        browse_button.clicked.connect(self._choose_project_root)
        create_button = QPushButton("Tạo dự án")
        create_button.clicked.connect(self._create_project)
        open_button = QPushButton("Mở dự án")
        open_button.clicked.connect(self._open_project)
        smoke_button = QPushButton("Chạy tác vụ thử")
        self._project_smoke_button = smoke_button
        smoke_button.clicked.connect(self._run_smoke_job)
        choose_video_button = QPushButton("Chọn video")
        choose_video_button.clicked.connect(self._choose_source_video)
        probe_button = QPushButton("Đọc metadata")
        probe_button.clicked.connect(self._run_probe_media_job)
        extract_button = QPushButton("Tách âm thanh")
        extract_button.clicked.connect(self._run_extract_audio_job)
        prepare_media_button = QPushButton("Chuẩn bị media")
        self._prepare_media_button = prepare_media_button
        prepare_media_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio"],
                workflow_name="Chuẩn bị media",
            )
        )
        asr_translate_button = QPushButton("ASR -> Dịch")
        self._asr_translate_button = asr_translate_button
        asr_translate_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["asr", "translate"],
                workflow_name="ASR -> Dịch",
            )
        )
        review_button = QPushButton("3. Mở review")
        self._project_review_button = review_button
        review_button.clicked.connect(self._open_review_queue_tab)
        dub_button = QPushButton("Lồng tiếng nhanh")
        self._dub_button = dub_button
        dub_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["tts", "voice_track", "mixdown"],
                workflow_name="Lồng tiếng nhanh",
            )
        )
        finish_button = QPushButton("4. Hoàn thiện video")
        self._project_finish_button = finish_button
        finish_button.clicked.connect(self._start_simple_finish_workflow)
        open_export_button = QPushButton("Mở video xuất")
        self._open_export_video_button = open_export_button
        open_export_button.setEnabled(False)
        open_export_button.clicked.connect(self._open_last_export_video)
        full_pipeline_button = QPushButton("Chạy toàn bộ quy trình")
        self._full_pipeline_button = full_pipeline_button
        full_pipeline_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio", "asr", "translate", "tts", "voice_track", "mixdown", "export_video"],
                workflow_name="Toàn bộ quy trình",
            )
        )
        stop_workflow_button = QPushButton("Dừng quy trình")
        self._stop_workflow_button = stop_workflow_button
        stop_workflow_button.clicked.connect(self._stop_workflow)

        project_root_row = QHBoxLayout()
        project_root_row.setContentsMargins(0, 0, 0, 0)
        project_root_row.addWidget(self._project_root_input)
        project_root_row.addWidget(browse_button)
        project_root_container = QWidget()
        project_root_container.setLayout(project_root_row)

        button_row = QHBoxLayout()
        button_row.addWidget(create_button)
        button_row.addWidget(open_button)
        button_row.addWidget(smoke_button)
        button_row.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(button_row)

        source_row = QHBoxLayout()
        source_row.addWidget(choose_video_button)
        source_row.addWidget(probe_button)
        source_row.addWidget(extract_button)
        source_row.addStretch(1)
        source_container = QWidget()
        source_container.setLayout(source_row)

        workflow_row_top = QHBoxLayout()
        workflow_row_top.addWidget(prepare_media_button)
        workflow_row_top.addWidget(asr_translate_button)
        workflow_row_top.addWidget(review_button)
        workflow_row_top.addWidget(dub_button)
        workflow_row_top.addStretch(1)
        workflow_row_bottom = QHBoxLayout()
        workflow_row_bottom.addWidget(finish_button)
        workflow_row_bottom.addWidget(open_export_button)
        workflow_row_bottom.addWidget(full_pipeline_button)
        workflow_row_bottom.addWidget(stop_workflow_button)
        workflow_row_bottom.addStretch(1)
        workflow_row = QVBoxLayout()
        workflow_row.addLayout(workflow_row_top)
        workflow_row.addLayout(workflow_row_bottom)
        workflow_container = QWidget()
        workflow_container.setLayout(workflow_row)

        workflow_group = QGroupBox("Quy trình nhanh")
        self._project_workflow_group = workflow_group
        workflow_form = QFormLayout(workflow_group)
        self._configure_form_layout(workflow_form)
        workflow_form.addRow("Trạng thái", self._workflow_status)
        workflow_form.addRow("", workflow_container)

        ops_group = QGroupBox("Ops & Safety")
        self._project_ops_group = ops_group
        ops_form = QFormLayout(ops_group)
        self._configure_form_layout(ops_form)
        self._cache_bucket_combo = QComboBox()
        self._cache_bucket_combo.addItem("Tat ca bucket cache", "")
        self._cache_bucket_combo.addItem("Audio extract", "audio")
        self._cache_bucket_combo.addItem("ASR", "asr")
        self._cache_bucket_combo.addItem("Translate", "translate")
        self._cache_bucket_combo.addItem("Translate contextual", "translate_contextual")
        self._cache_bucket_combo.addItem("TTS", "tts")
        self._cache_bucket_combo.addItem("Mix", "mix")
        self._cache_bucket_combo.addItem("Subs", "subs")
        self._cache_bucket_combo.addItem("Exports", "exports")

        run_doctor_button = QPushButton("Chạy doctor")
        run_doctor_button.clicked.connect(self._run_project_doctor_check)
        backup_button = QPushButton("Tạo backup")
        backup_button.clicked.connect(self._create_manual_workspace_backup)
        inspect_button = QPushButton("Kiểm tra workspace")
        inspect_button.clicked.connect(self._inspect_workspace_safety)
        repair_button = QPushButton("Sua metadata stale")
        repair_button.clicked.connect(self._repair_workspace_metadata)
        prune_cache_button = QPushButton("Don cache mo coi")
        prune_cache_button.clicked.connect(self._prune_orphan_cache)
        clear_bucket_button = QPushButton("Xoa cache bucket")
        clear_bucket_button.clicked.connect(self._clear_selected_cache_bucket)

        ops_button_row_top = QHBoxLayout()
        ops_button_row_top.addWidget(run_doctor_button)
        ops_button_row_top.addWidget(backup_button)
        ops_button_row_top.addWidget(inspect_button)
        ops_button_row_top.addWidget(repair_button)
        ops_button_row_top.addStretch(1)
        ops_button_row_bottom = QHBoxLayout()
        ops_button_row_bottom.addWidget(prune_cache_button)
        ops_button_row_bottom.addWidget(self._cache_bucket_combo)
        ops_button_row_bottom.addWidget(clear_bucket_button)
        ops_button_row_bottom.addStretch(1)
        ops_buttons = QVBoxLayout()
        ops_buttons.addLayout(ops_button_row_top)
        ops_buttons.addLayout(ops_button_row_bottom)
        ops_buttons_container = QWidget()
        ops_buttons_container.setLayout(ops_buttons)

        ops_form.addRow("Doctor", self._doctor_summary)
        ops_form.addRow("Workspace safety", self._workspace_repair_summary)
        ops_form.addRow("Cache", self._cache_ops_summary)
        ops_form.addRow("", ops_buttons_container)

        form.addRow("Tên dự án", self._project_name_input)
        form.addRow("Thư mục dự án", project_root_container)
        form.addRow("Video nguồn", self._source_video_input)
        form.addRow("", source_container)
        form.addRow("Ngôn ngữ nguồn", self._source_lang_combo)
        form.addRow("Dịch sang", self._target_lang_combo)
        form.addRow("Hồ sơ dự án", self._project_profile_combo)
        form.addRow("", button_container)

        layout.addWidget(self._project_summary)
        layout.addWidget(self._media_summary)
        layout.addWidget(self._pipeline_summary)
        layout.addWidget(group)
        layout.addWidget(workflow_group)
        layout.addWidget(ops_group)
        layout.addWidget(
            self._build_placeholder_group(
                "Hướng dẫn nhanh",
                "1. Chọn video nguồn rồi tạo hoặc mở dự án.\n"
                "2. Dùng “Chuẩn bị media” để đọc metadata và tách âm thanh.\n"
                "3. Dùng “ASR -> Dịch” để nhận diện lời nói và tạo bản dịch.\n"
                "4. Nếu cần lồng tiếng hoặc xuất video ngay, dùng các quy trình nhanh còn lại.",
            )
        )
        layout.addStretch(1)
        return widget

    def _reload_project_profile_options(self, selected_profile_id: str | None = None) -> None:
        if self._project_profile_combo is None:
            return
        profiles = default_project_profiles()
        if self._is_simple_ui_mode():
            profiles = [
                profile
                for profile in profiles
                if profile.project_profile_id == "zh-vi-narration-fast-v2-vieneu"
            ]
        self._available_project_profiles = profiles
        with QSignalBlocker(self._project_profile_combo):
            self._project_profile_combo.clear()
            if not self._is_simple_ui_mode():
                self._project_profile_combo.addItem("Chọn sau / thủ công", "")
            for profile in profiles:
                self._project_profile_combo.addItem(
                    f"{profile.name} ({profile.project_profile_id})",
                    profile.project_profile_id,
                )
            desired_profile_id = selected_profile_id
            if desired_profile_id is None and self._is_simple_ui_mode():
                desired_profile_id = "zh-vi-narration-fast-v2-vieneu"
            index = self._project_profile_combo.findData(desired_profile_id or "")
            self._project_profile_combo.setCurrentIndex(index if index >= 0 else 0)
        self._handle_project_profile_changed(self._project_profile_combo.currentIndex())

    def _selected_project_profile_id(self) -> str | None:
        if self._project_profile_combo is None:
            return None
        value = str(self._project_profile_combo.currentData() or "").strip()
        return value or None

    def _handle_project_profile_changed(self, index: int) -> None:
        del index
        profile_id = self._selected_project_profile_id()
        if not profile_id:
            return
        profile = next(
            (
                item
                for item in self._available_project_profiles
                if item.project_profile_id == profile_id
            ),
            None,
        )
        if profile is None:
            return
        self._apply_profile_defaults_to_form(profile)

    def _build_translate_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("ASR và Dịch")
        self._translate_group = group
        form = QFormLayout(group)
        self._translate_form = form
        self._configure_form_layout(form)

        self._asr_summary = self._create_info_label("Chưa có kết quả ASR")
        self._asr_engine_combo = QComboBox()
        self._asr_engine_combo.addItems(["faster-whisper"])
        self._asr_model_combo = QComboBox()
        self._asr_model_combo.addItems(["tiny", "base", "small", "medium", "large-v3"])
        self._asr_model_combo.setCurrentText(self._settings.default_asr_model)
        self._asr_language_combo = QComboBox()
        self._asr_language_combo.addItems(["auto", "vi", "zh", "en"])
        self._vad_checkbox = QCheckBox("Bật lọc VAD")
        self._vad_checkbox.setChecked(True)
        self._word_timestamps_checkbox = QCheckBox("Lấy mốc thời gian theo từ")
        self._word_timestamps_checkbox.setChecked(True)
        self._prompt_combo = QComboBox()
        self._translation_mode_info = self._create_info_label("legacy")
        self._translation_model_input = QLineEdit()
        self._translation_model_input.setPlaceholderText("gpt-4.1-mini")
        self._translation_summary = self._create_info_label("Chưa có kết quả dịch")
        self._review_summary = self._create_info_label("Chưa có hàng review semantic")

        run_asr_button = QPushButton("Chạy ASR")
        self._run_asr_button = run_asr_button
        run_asr_button.clicked.connect(self._run_asr_job)
        reload_prompts_button = QPushButton("Nạp lại prompt")
        self._reload_prompts_button = reload_prompts_button
        reload_prompts_button.clicked.connect(self._reload_prompt_templates)
        run_translate_button = QPushButton("Chạy dịch")
        self._run_translate_button = run_translate_button
        run_translate_button.clicked.connect(self._run_translation_job)
        translate_buttons = QHBoxLayout()
        translate_buttons.addWidget(reload_prompts_button)
        translate_buttons.addWidget(run_translate_button)
        translate_buttons.addStretch(1)
        translate_container = QWidget()
        translate_container.setLayout(translate_buttons)
        review_group = QGroupBox("Review Ngữ Cảnh")
        review_layout = QVBoxLayout(review_group)
        review_layout.addWidget(self._review_summary)
        self._review_table = QTableWidget(0, 7)
        self._review_table.setHorizontalHeaderLabels(
            ["#", "Scene", "Nguồn", "Speaker", "Listener", "Xưng hô", "Lý do"]
        )
        self._review_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._review_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._review_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._review_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._review_table)
        self._review_table.verticalHeader().setVisible(False)
        self._review_table.setMinimumHeight(220)
        review_header = self._review_table.horizontalHeader()
        review_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        review_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        review_header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        review_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        review_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        review_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        review_header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        self._review_table.itemSelectionChanged.connect(self._handle_review_selection_changed)
        review_layout.addWidget(self._review_table)
        self._review_context_text = QPlainTextEdit()
        self._review_context_text.setReadOnly(True)
        self._review_context_text.setMinimumHeight(120)
        review_layout.addWidget(self._review_context_text)
        review_form = QFormLayout()
        self._review_speaker_input = QLineEdit()
        self._review_listener_input = QLineEdit()
        self._review_self_term_input = QLineEdit()
        self._review_address_term_input = QLineEdit()
        self._review_subtitle_input = QLineEdit()
        self._review_tts_input = QLineEdit()
        review_form.addRow("Speaker", self._review_speaker_input)
        review_form.addRow("Listener", self._review_listener_input)
        review_form.addRow("Tự xưng", self._review_self_term_input)
        review_form.addRow("Gọi người nghe", self._review_address_term_input)
        review_form.addRow("Phụ đề duyệt", self._review_subtitle_input)
        review_form.addRow("Lời TTS duyệt", self._review_tts_input)
        review_layout.addLayout(review_form)
        review_button_row = QHBoxLayout()
        reload_review_button = QPushButton("Nạp review")
        reload_review_button.clicked.connect(self._reload_review_queue)
        approve_line_button = QPushButton("Khóa dòng")
        approve_line_button.clicked.connect(lambda checked=False: self._apply_review_resolution("line"))
        approve_scene_button = QPushButton("Khóa scene")
        approve_scene_button.clicked.connect(lambda checked=False: self._apply_review_resolution("scene"))
        approve_relation_button = QPushButton("Khóa quan hệ")
        approve_relation_button.clicked.connect(
            lambda checked=False: self._apply_review_resolution("project-relationship")
        )
        select_scene_button = QPushButton("Chọn cùng scene")
        select_scene_button.clicked.connect(lambda checked=False: self._select_review_rows_by_scope("scene"))
        select_relation_button = QPushButton("Chọn cùng quan hệ")
        select_relation_button.clicked.connect(lambda checked=False: self._select_review_rows_by_scope("relation"))
        approve_selected_button = QPushButton("Áp cho dòng chọn")
        approve_selected_button.clicked.connect(self._apply_review_resolution_to_selected_rows)
        review_button_row.addWidget(reload_review_button)
        review_button_row.addWidget(approve_line_button)
        review_button_row.addWidget(approve_scene_button)
        review_button_row.addWidget(approve_relation_button)
        review_button_row.addWidget(select_scene_button)
        review_button_row.addWidget(select_relation_button)
        review_button_row.addWidget(approve_selected_button)
        review_button_row.addStretch(1)
        review_layout.addLayout(review_button_row)

        form.addRow("Engine ASR", self._asr_engine_combo)
        form.addRow("Mô hình", self._asr_model_combo)
        form.addRow("Ngôn ngữ ASR", self._asr_language_combo)
        form.addRow("", self._vad_checkbox)
        form.addRow("", self._word_timestamps_checkbox)
        form.addRow("", run_asr_button)
        form.addRow("Chế độ dịch", self._translation_mode_info)
        form.addRow("Mẫu prompt", self._prompt_combo)
        form.addRow("Mô hình dịch", self._translation_model_input)
        form.addRow("", translate_container)

        layout.addWidget(self._asr_summary)
        layout.addWidget(self._translation_summary)
        layout.addWidget(group)
        layout.addWidget(review_group)
        layout.addWidget(
            self._build_placeholder_group(
                "Hướng dẫn",
                "ASR sẽ đọc lời thoại từ âm thanh 16 kHz. Bước dịch dùng OpenAI Responses API "
                "và Structured Outputs để tạo bản dịch ổn định cho phụ đề.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_subtitle_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._subtitle_summary = self._create_info_label("Chưa có file phụ đề đầu ra")
        self._subtitle_editor_status = self._create_info_label("Chưa nạp dữ liệu vào trình biên tập")
        self._subtitle_qc_summary = self._create_info_label("QC phụ đề chưa được chạy")
        self._subtitle_table = QTableWidget(0, 8)
        self._subtitle_table.setHorizontalHeaderLabels(
            ["#", "Bắt đầu", "Kết thúc", "Nguồn", "Bản dịch", "Phụ đề", "Lời TTS", "Trạng thái"]
        )
        self._subtitle_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._subtitle_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._subtitle_table)
        self._subtitle_table.verticalHeader().setVisible(False)
        self._subtitle_table.setMinimumHeight(300)
        subtitle_header = self._subtitle_table.horizontalHeader()
        subtitle_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        subtitle_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        subtitle_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        subtitle_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        subtitle_header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        subtitle_header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        subtitle_header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        subtitle_header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        self._subtitle_table.itemChanged.connect(self._handle_subtitle_item_changed)
        self._subtitle_qc_table = QTableWidget(0, 4)
        self._subtitle_qc_table.setHorizontalHeaderLabels(["Dòng", "Mã lỗi", "Mức độ", "Chi tiết"])
        self._subtitle_qc_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._subtitle_qc_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._subtitle_qc_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._subtitle_qc_table)
        self._subtitle_qc_table.verticalHeader().setVisible(False)
        self._subtitle_qc_table.setMinimumHeight(180)
        qc_header = self._subtitle_qc_table.horizontalHeader()
        qc_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        qc_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        qc_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        qc_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)

        self._shift_input = QLineEdit("0")
        self._find_input = QLineEdit()
        self._replace_input = QLineEdit()
        self._replace_target_combo = QComboBox()
        self._replace_target_combo.addItem("Phụ đề", "subtitle")
        self._replace_target_combo.addItem("Bản dịch", "translated")
        self._replace_target_combo.addItem("TTS", "tts")
        self._replace_target_combo.addItem("Tất cả", "all")

        reload_button = QPushButton("Nạp lại từ CSDL")
        reload_button.clicked.connect(lambda: self._reload_subtitle_editor_from_db(force=True))
        translated_to_subtitle_button = QPushButton("Bản dịch -> Phụ đề")
        self._translated_to_subtitle_button = translated_to_subtitle_button
        translated_to_subtitle_button.clicked.connect(self._apply_translated_to_subtitle)
        subtitle_to_tts_button = QPushButton("Phụ đề -> Lời TTS")
        self._subtitle_to_tts_button = subtitle_to_tts_button
        subtitle_to_tts_button.clicked.connect(self._apply_subtitle_to_tts)
        polish_tts_button = QPushButton("Làm mượt Lời TTS")
        self._polish_tts_button = polish_tts_button
        polish_tts_button.clicked.connect(self._polish_tts_texts)
        split_button = QPushButton("Tách dòng chọn")
        self._split_subtitle_button = split_button
        split_button.clicked.connect(self._split_selected_subtitle_row)
        merge_button = QPushButton("Gộp với dòng sau")
        self._merge_subtitle_button = merge_button
        merge_button.clicked.connect(self._merge_selected_subtitle_row_with_next)
        save_button = QPushButton("Lưu chỉnh sửa")
        save_button.clicked.connect(self._save_subtitle_edits)
        shift_button = QPushButton("Dịch toàn bộ")
        shift_button.clicked.connect(self._apply_shift_to_subtitle_rows)
        replace_button = QPushButton("Tìm và thay thế")
        replace_button.clicked.connect(self._apply_find_replace)
        qc_button = QPushButton("Chạy QC")
        qc_button.clicked.connect(self._run_subtitle_qc)
        preview_from_start_button = QPushButton("Xem từ đầu")
        preview_from_start_button.clicked.connect(lambda: self._preview_subtitles(start_from_selected=False))
        preview_selected_button = QPushButton("Xem dòng chọn")
        preview_selected_button.clicked.connect(lambda: self._preview_subtitles(start_from_selected=True))
        self._subtitle_subtext_toggle = QCheckBox("Subtext gốc")
        self._subtitle_subtext_toggle.setChecked(False)
        self._subtitle_subtext_toggle.toggled.connect(self._handle_subtitle_subtext_toggle)
        export_srt_button = QPushButton("Xuất SRT")
        export_srt_button.clicked.connect(lambda: self._run_export_subtitles_job("srt"))
        export_ass_button = QPushButton("Xuất ASS")
        export_ass_button.clicked.connect(lambda: self._run_export_subtitles_job("ass"))

        action_row_top = QHBoxLayout()
        action_row_top.addWidget(reload_button)
        action_row_top.addWidget(translated_to_subtitle_button)
        action_row_top.addWidget(subtitle_to_tts_button)
        action_row_top.addWidget(polish_tts_button)
        action_row_top.addWidget(split_button)
        action_row_top.addWidget(merge_button)
        action_row_top.addWidget(save_button)
        action_row_top.addWidget(qc_button)
        action_row_top.addStretch(1)
        action_row_bottom = QHBoxLayout()
        action_row_bottom.addWidget(preview_from_start_button)
        action_row_bottom.addWidget(preview_selected_button)
        action_row_bottom.addWidget(self._subtitle_subtext_toggle)
        action_row_bottom.addWidget(export_srt_button)
        action_row_bottom.addWidget(export_ass_button)
        action_row_bottom.addStretch(1)
        action_row = QVBoxLayout()
        action_row.addLayout(action_row_top)
        action_row.addLayout(action_row_bottom)
        action_container = QWidget()
        action_container.setLayout(action_row)

        shift_row = QHBoxLayout()
        shift_row.addWidget(QLabel("Dịch thời gian (ms)"))
        shift_row.addWidget(self._shift_input)
        shift_row.addWidget(shift_button)
        shift_row.addStretch(1)
        replace_row = QHBoxLayout()
        replace_row.addWidget(QLabel("Tìm"))
        replace_row.addWidget(self._find_input)
        replace_row.addWidget(QLabel("Thay bằng"))
        replace_row.addWidget(self._replace_input)
        replace_row.addWidget(self._replace_target_combo)
        replace_row.addWidget(replace_button)
        replace_row.addStretch(1)
        tools_row = QVBoxLayout()
        tools_row.addLayout(shift_row)
        tools_row.addLayout(replace_row)
        tools_container = QWidget()
        tools_container.setLayout(tools_row)
        self._subtitle_tools_container = tools_container

        layout.addWidget(self._subtitle_summary)
        layout.addWidget(self._subtitle_editor_status)
        layout.addWidget(self._subtitle_table)
        layout.addWidget(tools_container)
        layout.addWidget(action_container)
        layout.addWidget(self._subtitle_qc_summary)
        layout.addWidget(self._subtitle_qc_table)
        layout.addWidget(
            self._build_placeholder_group(
                "Hướng dẫn",
                "Bạn có thể sửa thời gian, bản dịch, phụ đề và nội dung TTS ngay trong bảng. "
                "Nếu muốn giọng đọc tự nhiên hơn, hãy dùng `Phụ đề -> Lời TTS` rồi `Làm mượt Lời TTS`, "
                "sau đó tinh chỉnh riêng những câu quan trọng. Hãy chạy QC trước khi xuất để kiểm tra lỗi chồng dòng, "
                "tốc độ đọc và độ dài câu.",
            )
        )
        return widget

    def _build_voiceover_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._voice_summary = self._create_info_label("Chưa có preset giọng hoặc kết quả TTS")
        self._mix_summary = self._create_info_label("Chưa có âm thanh đã trộn")
        self._voice_combo = QComboBox()
        self._voice_combo.currentIndexChanged.connect(self._handle_voice_preset_changed)
        self._voice_profile_name_input = QLineEdit()
        self._voice_profile_name_input.setPlaceholderText("Tên preset giọng")
        self._voice_profile_name_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_engine_combo = QComboBox()
        self._voice_engine_combo.addItem("Windows SAPI", "sapi")
        self._voice_engine_combo.addItem("VieNeu", "vieneu")
        self._voice_engine_combo.currentIndexChanged.connect(self._handle_voice_profile_form_changed)
        self._voice_id_input = QLineEdit()
        self._voice_id_input.setPlaceholderText("default hoặc tên giọng")
        self._voice_id_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_language_input = QLineEdit()
        self._voice_language_input.setPlaceholderText("vi / en / auto")
        self._voice_language_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_sample_rate_input = QLineEdit("24000")
        self._voice_sample_rate_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_speed_profile_input = QLineEdit("1.0")
        self._voice_speed_profile_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_profile_volume_input = QLineEdit("1.0")
        self._voice_profile_volume_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_pitch_input = QLineEdit("0.0")
        self._voice_pitch_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_info = self._create_info_label("Giọng SAPI đã phát hiện: chưa nạp")
        self._voice_preset_notes = self._create_info_label("Chưa có ghi chú cho preset")
        self._voice_clone_status = self._create_info_label("Preset clone chưa được cấu hình")
        self._voice_profile_status = self._create_info_label("Quản lý preset giọng đã sẵn sàng")
        self._speaker_binding_status = self._create_info_label("Speaker binding: chưa có dữ liệu")
        self._speaker_binding_hint = self._create_info_label(
            "Mẹo: nếu đã lưu ít nhất 1 speaker binding, mọi speaker nhận diện rõ trong track hiện tại phải được gán preset. Speaker unknown vẫn dùng preset mặc định."
        )
        self._voice_policy_status = self._create_info_label("Voice policy: chưa có dữ liệu")
        self._voice_policy_hint = self._create_info_label(
            "Mẹo: voice policy là fallback mềm. Quan hệ speaker->listener sẽ ưu tiên hơn policy theo nhân vật, nhưng speaker binding vẫn là mức ưu tiên cao nhất."
        )
        self._register_voice_style_status = self._create_info_label("Register style: chưa có dữ liệu")
        self._register_voice_style_hint = self._create_info_label(
            "Mẹo: register-aware style chỉ tinh chỉnh speed/volume/pitch. Policy theo quan hệ và nhân vật vẫn ưu tiên hơn register style, và segment còn mơ hồ sẽ không được áp."
        )
        self._effective_voice_plan_preview = QPlainTextEdit()
        self._effective_voice_plan_preview.setReadOnly(True)
        self._effective_voice_plan_preview.setPlaceholderText("Effective voice plan của track hiện tại sẽ hiện ở đây.")
        self._effective_voice_plan_preview.setFixedHeight(160)
        self._voice_notes_input = QPlainTextEdit()
        self._voice_notes_input.setPlaceholderText("Ghi chú preset hoặc ghi chú về cấu hình clone")
        self._voice_notes_input.setFixedHeight(56)
        self._voice_notes_input.textChanged.connect(self._handle_voice_profile_form_changed)
        self._vieneu_ref_audio_input = QLineEdit()
        self._vieneu_ref_audio_input.setPlaceholderText("assets/voices/reference.wav hoặc đường dẫn tuyệt đối")
        self._vieneu_ref_audio_input.textChanged.connect(lambda _text: self._handle_voice_clone_form_changed())
        self._vieneu_ref_text_input = QPlainTextEdit()
        self._vieneu_ref_text_input.setPlaceholderText("Nhập câu đọc gốc khớp với file audio mẫu")
        self._vieneu_ref_text_input.setFixedHeight(72)
        self._vieneu_ref_text_input.textChanged.connect(self._handle_voice_clone_form_changed)
        self._bgm_path_input = QLineEdit()
        self._bgm_path_input.setPlaceholderText("Đường dẫn BGM tùy chọn")
        self._original_volume_input = QLineEdit("0.35")
        self._voice_volume_input = QLineEdit("1.0")
        self._bgm_volume_input = QLineEdit("0.15")
        self._speaker_binding_table = QTableWidget(0, 4)
        self._speaker_binding_table.setHorizontalHeaderLabels(["Speaker", "Số dòng", "Preset giọng", "Trạng thái"])
        self._speaker_binding_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._speaker_binding_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._speaker_binding_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._speaker_binding_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._speaker_binding_table)
        self._speaker_binding_table.verticalHeader().setVisible(False)
        self._speaker_binding_table.setMinimumHeight(180)
        speaker_binding_header = self._speaker_binding_table.horizontalHeader()
        speaker_binding_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        speaker_binding_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        speaker_binding_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        speaker_binding_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._character_voice_policy_table = QTableWidget(0, 7)
        self._character_voice_policy_table.setHorizontalHeaderLabels(
            ["Nhân vật", "Số dòng", "Preset mặc định", "Tốc độ", "Âm lượng", "Cao độ", "Trạng thái"]
        )
        self._character_voice_policy_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._character_voice_policy_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._character_voice_policy_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._character_voice_policy_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._character_voice_policy_table)
        self._character_voice_policy_table.verticalHeader().setVisible(False)
        self._character_voice_policy_table.setMinimumHeight(140)
        character_policy_header = self._character_voice_policy_table.horizontalHeader()
        character_policy_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        character_policy_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        character_policy_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        character_policy_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        character_policy_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        character_policy_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        character_policy_header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        self._relationship_voice_policy_table = QTableWidget(0, 7)
        self._relationship_voice_policy_table.setHorizontalHeaderLabels(
            ["Quan hệ", "Số dòng", "Preset override", "Tốc độ", "Âm lượng", "Cao độ", "Trạng thái"]
        )
        self._relationship_voice_policy_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._relationship_voice_policy_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._relationship_voice_policy_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._relationship_voice_policy_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._relationship_voice_policy_table)
        self._relationship_voice_policy_table.verticalHeader().setVisible(False)
        self._relationship_voice_policy_table.setMinimumHeight(140)
        relationship_policy_header = self._relationship_voice_policy_table.horizontalHeader()
        relationship_policy_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        relationship_policy_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        relationship_policy_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        relationship_policy_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        relationship_policy_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        relationship_policy_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        relationship_policy_header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        self._register_voice_style_table = QTableWidget(0, 11)
        self._register_voice_style_table.setHorizontalHeaderLabels(
            [
                "Tình huống register",
                "Số dòng",
                "Lịch sự",
                "Quyền lực",
                "Cảm xúc",
                "Chức năng",
                "Quan hệ",
                "Tốc độ",
                "Âm lượng",
                "Cao độ",
                "Trạng thái",
            ]
        )
        self._register_voice_style_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._register_voice_style_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._register_voice_style_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._register_voice_style_table.setAlternatingRowColors(True)
        self._configure_table_widget(self._register_voice_style_table)
        self._register_voice_style_table.verticalHeader().setVisible(False)
        self._register_voice_style_table.setMinimumHeight(160)
        register_policy_header = self._register_voice_style_table.horizontalHeader()
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_LABEL_COLUMN, QHeaderView.ResizeMode.Stretch)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_COUNT_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_POLITENESS_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_POWER_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_EMOTION_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_TURN_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_RELATION_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_SPEED_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_VOLUME_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_PITCH_COLUMN, QHeaderView.ResizeMode.ResizeToContents)
        register_policy_header.setSectionResizeMode(REGISTER_STYLE_STATUS_COLUMN, QHeaderView.ResizeMode.Stretch)

        reload_voices_button = QPushButton("Nạp lại preset")
        reload_voices_button.clicked.connect(self._reload_voice_presets)
        run_tts_button = QPushButton("Chạy TTS")
        run_tts_button.clicked.connect(self._run_tts_job)
        build_track_button = QPushButton("Tạo track giọng")
        build_track_button.clicked.connect(self._run_build_voice_track_job)
        self._choose_vieneu_ref_audio_button = QPushButton("Chọn audio mẫu")
        self._choose_vieneu_ref_audio_button.clicked.connect(self._choose_vieneu_ref_audio_file)
        self._save_voice_preset_button = QPushButton("Lưu preset")
        self._save_voice_preset_button.clicked.connect(self._save_current_voice_preset)
        self._save_voice_preset_as_new_button = QPushButton("Lưu thành bản mới")
        self._save_voice_preset_as_new_button.clicked.connect(
            lambda checked=False: self._save_current_voice_preset(save_as_new=True)
        )
        self._delete_voice_preset_button = QPushButton("Xóa preset")
        self._delete_voice_preset_button.clicked.connect(self._delete_selected_voice_preset)
        self._batch_import_voice_profiles_button = QPushButton("Nhập hàng loạt từ thư mục mẫu")
        self._batch_import_voice_profiles_button.clicked.connect(self._batch_import_voice_profiles)
        self._reload_speaker_bindings_button = QPushButton("Nạp speaker")
        self._reload_speaker_bindings_button.clicked.connect(self._reload_speaker_bindings)
        self._save_speaker_bindings_button = QPushButton("Lưu binding")
        self._save_speaker_bindings_button.clicked.connect(self._save_speaker_bindings)
        self._fill_speaker_bindings_button = QPushButton("Gán preset đang chọn cho ô trống")
        self._fill_speaker_bindings_button.clicked.connect(self._fill_unbound_speakers_with_selected_preset)
        self._clear_speaker_bindings_button = QPushButton("Xóa gán trên form")
        self._clear_speaker_bindings_button.clicked.connect(self._clear_speaker_binding_form)
        self._fill_selected_speaker_bindings_button = QPushButton("Gán preset cho dòng chọn")
        self._fill_selected_speaker_bindings_button.clicked.connect(
            self._fill_selected_speaker_bindings_with_selected_preset
        )
        self._clear_selected_speaker_bindings_button = QPushButton("Xóa dòng chọn")
        self._clear_selected_speaker_bindings_button.clicked.connect(self._clear_selected_speaker_bindings)
        self._reload_voice_policies_button = QPushButton("Nạp voice policy")
        self._reload_voice_policies_button.clicked.connect(self._reload_voice_policies)
        self._save_voice_policies_button = QPushButton("Lưu voice policy")
        self._save_voice_policies_button.clicked.connect(self._save_voice_policies)
        self._fill_voice_policies_button = QPushButton("Gán preset đang chọn cho policy trống")
        self._fill_voice_policies_button.clicked.connect(self._fill_unbound_voice_policies_with_selected_preset)
        self._clear_voice_policies_button = QPushButton("Xóa policy trên form")
        self._clear_voice_policies_button.clicked.connect(self._clear_voice_policy_form)
        self._fill_voice_policy_styles_button = QPushButton("Điền style trống")
        self._fill_voice_policy_styles_button.clicked.connect(self._fill_unstyled_voice_policies_with_current_style)
        self._clear_voice_policy_styles_button = QPushButton("Xóa style form")
        self._clear_voice_policy_styles_button.clicked.connect(self._clear_voice_policy_form_styles)
        self._fill_selected_voice_policies_button = QPushButton("Gán preset cho dòng chọn")
        self._fill_selected_voice_policies_button.clicked.connect(
            self._fill_selected_voice_policy_rows_with_selected_preset
        )
        self._fill_selected_voice_policy_styles_button = QPushButton("Điền style dòng chọn")
        self._fill_selected_voice_policy_styles_button.clicked.connect(
            self._fill_selected_voice_policy_rows_with_current_style
        )
        self._clear_selected_voice_policies_button = QPushButton("Xóa dòng chọn")
        self._clear_selected_voice_policies_button.clicked.connect(self._clear_selected_voice_policy_rows)
        self._clear_selected_voice_policy_styles_button = QPushButton("Xóa style dòng chọn")
        self._clear_selected_voice_policy_styles_button.clicked.connect(self._clear_selected_voice_policy_row_styles)
        self._fill_register_voice_styles_button = QPushButton("Điền style register trống")
        self._fill_register_voice_styles_button.clicked.connect(self._fill_unstyled_register_voice_rows_with_current_style)
        self._clear_register_voice_styles_button = QPushButton("Xóa style register")
        self._clear_register_voice_styles_button.clicked.connect(self._clear_register_voice_style_form_styles)
        self._fill_selected_register_voice_styles_button = QPushButton("Điền style register dòng chọn")
        self._fill_selected_register_voice_styles_button.clicked.connect(
            self._fill_selected_register_voice_rows_with_current_style
        )
        self._clear_selected_register_voice_styles_button = QPushButton("Xóa style register dòng chọn")
        self._clear_selected_register_voice_styles_button.clicked.connect(
            self._clear_selected_register_voice_row_styles
        )
        self._rerun_downstream_only_button = QPushButton("Chạy lại downstream")
        self._rerun_downstream_only_button.clicked.connect(self._rerun_downstream_only)
        choose_bgm_button = QPushButton("Chọn BGM")
        choose_bgm_button.clicked.connect(self._choose_bgm_file)
        mix_button = QPushButton("Trộn âm thanh")
        mix_button.clicked.connect(self._run_mixdown_job)

        group = QGroupBox("Preset giọng, TTS và trộn âm thanh")
        self._voiceover_group = group
        form = QFormLayout(group)
        self._voiceover_form = form
        self._configure_form_layout(form)
        action_row = QHBoxLayout()
        action_row.addWidget(reload_voices_button)
        action_row.addWidget(run_tts_button)
        action_row.addWidget(build_track_button)
        action_row.addStretch(1)
        action_container = QWidget()
        action_container.setLayout(action_row)
        self._voice_actions_container = action_container

        profile_action_row_top = QHBoxLayout()
        profile_action_row_top.addWidget(self._save_voice_preset_button)
        profile_action_row_top.addWidget(self._save_voice_preset_as_new_button)
        profile_action_row_top.addWidget(self._delete_voice_preset_button)
        profile_action_row_top.addStretch(1)
        profile_action_row_bottom = QHBoxLayout()
        profile_action_row_bottom.addWidget(self._batch_import_voice_profiles_button)
        profile_action_row_bottom.addStretch(1)
        profile_action_row = QVBoxLayout()
        profile_action_row.addLayout(profile_action_row_top)
        profile_action_row.addLayout(profile_action_row_bottom)
        profile_action_container = QWidget()
        profile_action_container.setLayout(profile_action_row)
        self._voice_profile_actions_container = profile_action_container

        speaker_binding_actions_top = QHBoxLayout()
        speaker_binding_actions_top.addWidget(self._reload_speaker_bindings_button)
        speaker_binding_actions_top.addWidget(self._save_speaker_bindings_button)
        speaker_binding_actions_top.addWidget(self._fill_speaker_bindings_button)
        speaker_binding_actions_top.addWidget(self._clear_speaker_bindings_button)
        speaker_binding_actions_top.addStretch(1)
        speaker_binding_actions_bottom = QHBoxLayout()
        speaker_binding_actions_bottom.addWidget(self._fill_selected_speaker_bindings_button)
        speaker_binding_actions_bottom.addWidget(self._clear_selected_speaker_bindings_button)
        speaker_binding_actions_bottom.addStretch(1)
        speaker_binding_actions = QVBoxLayout()
        speaker_binding_actions.addLayout(speaker_binding_actions_top)
        speaker_binding_actions.addLayout(speaker_binding_actions_bottom)
        speaker_binding_actions_container = QWidget()
        speaker_binding_actions_container.setLayout(speaker_binding_actions)
        self._speaker_binding_actions_container = speaker_binding_actions_container
        voice_policy_actions_top = QHBoxLayout()
        voice_policy_actions_top.addWidget(self._reload_voice_policies_button)
        voice_policy_actions_top.addWidget(self._save_voice_policies_button)
        voice_policy_actions_top.addWidget(self._fill_voice_policies_button)
        voice_policy_actions_top.addWidget(self._clear_voice_policies_button)
        voice_policy_actions_top.addWidget(self._fill_voice_policy_styles_button)
        voice_policy_actions_top.addWidget(self._clear_voice_policy_styles_button)
        voice_policy_actions_top.addStretch(1)
        voice_policy_actions_bottom = QHBoxLayout()
        voice_policy_actions_bottom.addWidget(self._fill_selected_voice_policies_button)
        voice_policy_actions_bottom.addWidget(self._clear_selected_voice_policies_button)
        voice_policy_actions_bottom.addWidget(self._fill_selected_voice_policy_styles_button)
        voice_policy_actions_bottom.addWidget(self._clear_selected_voice_policy_styles_button)
        voice_policy_actions_bottom.addStretch(1)
        register_style_actions = QHBoxLayout()
        register_style_actions.addWidget(self._fill_register_voice_styles_button)
        register_style_actions.addWidget(self._clear_register_voice_styles_button)
        register_style_actions.addWidget(self._fill_selected_register_voice_styles_button)
        register_style_actions.addWidget(self._clear_selected_register_voice_styles_button)
        register_style_actions.addWidget(self._rerun_downstream_only_button)
        register_style_actions.addStretch(1)
        voice_policy_actions = QVBoxLayout()
        voice_policy_actions.addLayout(voice_policy_actions_top)
        voice_policy_actions.addLayout(voice_policy_actions_bottom)
        voice_policy_actions.addLayout(register_style_actions)
        voice_policy_actions_container = QWidget()
        voice_policy_actions_container.setLayout(voice_policy_actions)
        self._voice_policy_actions_container = voice_policy_actions_container

        ref_audio_row = QHBoxLayout()
        ref_audio_row.addWidget(self._vieneu_ref_audio_input)
        ref_audio_row.addWidget(self._choose_vieneu_ref_audio_button)
        ref_audio_container = QWidget()
        ref_audio_container.setLayout(ref_audio_row)
        self._voice_ref_audio_container = ref_audio_container

        bgm_row = QHBoxLayout()
        bgm_row.addWidget(self._bgm_path_input)
        bgm_row.addWidget(choose_bgm_button)
        bgm_container = QWidget()
        bgm_container.setLayout(bgm_row)
        self._bgm_container = bgm_container

        profile_numeric_row_top = QHBoxLayout()
        profile_numeric_row_top.addWidget(QLabel("Tần số mẫu"))
        profile_numeric_row_top.addWidget(self._voice_sample_rate_input)
        profile_numeric_row_top.addWidget(QLabel("Tốc độ"))
        profile_numeric_row_top.addWidget(self._voice_speed_profile_input)
        profile_numeric_row_top.addStretch(1)
        profile_numeric_row_bottom = QHBoxLayout()
        profile_numeric_row_bottom.addWidget(QLabel("Âm lượng"))
        profile_numeric_row_bottom.addWidget(self._voice_profile_volume_input)
        profile_numeric_row_bottom.addWidget(QLabel("Cao độ"))
        profile_numeric_row_bottom.addWidget(self._voice_pitch_input)
        profile_numeric_row_bottom.addStretch(1)
        profile_numeric_row = QVBoxLayout()
        profile_numeric_row.addLayout(profile_numeric_row_top)
        profile_numeric_row.addLayout(profile_numeric_row_bottom)
        profile_numeric_container = QWidget()
        profile_numeric_container.setLayout(profile_numeric_row)
        self._voice_profile_numeric_container = profile_numeric_container

        mix_row = QHBoxLayout()
        mix_row.addWidget(QLabel("Audio gốc"))
        mix_row.addWidget(self._original_volume_input)
        mix_row.addWidget(QLabel("Giọng đọc"))
        mix_row.addWidget(self._voice_volume_input)
        mix_row.addWidget(QLabel("BGM"))
        mix_row.addWidget(self._bgm_volume_input)
        mix_row.addWidget(mix_button)
        mix_row.addStretch(1)
        mix_container = QWidget()
        mix_container.setLayout(mix_row)
        self._mix_container = mix_container

        form.addRow("Preset giọng", self._voice_combo)
        form.addRow("Tên preset", self._voice_profile_name_input)
        form.addRow("Bộ máy TTS", self._voice_engine_combo)
        form.addRow("ID giọng", self._voice_id_input)
        form.addRow("Ngôn ngữ", self._voice_language_input)
        form.addRow("Thông số giọng", profile_numeric_container)
        form.addRow("Giọng SAPI phát hiện", self._voice_info)
        form.addRow("Ghi chú preset", self._voice_preset_notes)
        form.addRow("Trạng thái chỉnh sửa", self._voice_profile_status)
        form.addRow("Ghi chú chi tiết", self._voice_notes_input)
        form.addRow("Trạng thái clone", self._voice_clone_status)
        form.addRow("Audio mẫu VieNeu", ref_audio_container)
        form.addRow("Văn bản mẫu VieNeu", self._vieneu_ref_text_input)
        form.addRow("", profile_action_container)
        form.addRow("", action_container)
        form.addRow("Speaker binding", self._speaker_binding_status)
        form.addRow("", self._speaker_binding_hint)
        form.addRow("Bảng gán speaker", self._speaker_binding_table)
        form.addRow("", speaker_binding_actions_container)
        form.addRow("Voice policy", self._voice_policy_status)
        form.addRow("", self._voice_policy_hint)
        form.addRow("Policy theo nhân vật", self._character_voice_policy_table)
        form.addRow("Policy theo quan hệ", self._relationship_voice_policy_table)
        form.addRow("Register style", self._register_voice_style_status)
        form.addRow("", self._register_voice_style_hint)
        form.addRow("Policy theo register", self._register_voice_style_table)
        form.addRow("", voice_policy_actions_container)
        form.addRow("Effective voice plan", self._effective_voice_plan_preview)
        form.addRow("BGM tùy chọn", bgm_container)
        form.addRow("Mức âm khi trộn", mix_container)

        layout.addWidget(self._voice_summary)
        layout.addWidget(self._mix_summary)
        layout.addWidget(group)
        layout.addWidget(
            self._build_placeholder_group(
                "Hướng dẫn",
                "VieNeu phù hợp nhất cho giọng tiếng Việt. Nếu bạn cần nhân bản giọng, hãy điền audio mẫu "
                "và văn bản mẫu đúng với audio đó trước khi chạy TTS hoặc nhập hàng loạt từ thư mục `assets/voices`. "
                "Để giọng đọc tự nhiên hơn, hãy viết cột `Lời TTS` theo văn nói ngắn gọn rồi mới chạy TTS.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_export_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._export_summary = self._create_info_label("Chưa có video đầu ra")
        self._export_preset_combo = QComboBox()
        self._export_preset_combo.currentIndexChanged.connect(self._handle_export_preset_changed)
        self._burn_subtitles_checkbox = QCheckBox("Ghi cứng phụ đề vào video")
        self._burn_subtitles_checkbox.setChecked(True)
        self._burn_subtitles_checkbox.stateChanged.connect(self._handle_export_mode_changed)
        self._watermark_profile_combo = QComboBox()
        self._watermark_profile_combo.currentIndexChanged.connect(self._handle_watermark_profile_changed)
        self._watermark_profile_name_input = QLineEdit()
        self._watermark_profile_name_input.setPlaceholderText("Tên profile watermark hoặc logo")
        self._watermark_profile_name_input.textChanged.connect(
            lambda _text: self._handle_watermark_form_changed()
        )
        self._watermark_profile_status = self._create_info_label("Chưa có profile watermark")
        self._watermark_enabled_checkbox = QCheckBox("Bật watermark hoặc logo")
        self._watermark_enabled_checkbox.stateChanged.connect(self._handle_watermark_form_changed)
        self._watermark_path_input = QLineEdit()
        self._watermark_path_input.setPlaceholderText("assets/logos/logo.png hoặc đường dẫn tuyệt đối")
        self._watermark_path_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_position_combo = QComboBox()
        self._watermark_position_combo.addItem("Trên phải", "top-right")
        self._watermark_position_combo.addItem("Trên trái", "top-left")
        self._watermark_position_combo.addItem("Dưới phải", "bottom-right")
        self._watermark_position_combo.addItem("Dưới trái", "bottom-left")
        self._watermark_position_combo.currentIndexChanged.connect(self._handle_watermark_form_changed)
        self._watermark_opacity_input = QLineEdit("0.85")
        self._watermark_opacity_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_scale_input = QLineEdit("0.16")
        self._watermark_scale_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_margin_input = QLineEdit("24")
        self._watermark_margin_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        choose_watermark_button = QPushButton("Chọn logo")
        choose_watermark_button.clicked.connect(self._choose_watermark_file)
        reload_presets_button = QPushButton("Nạp lại preset")
        self._reload_export_presets_button = reload_presets_button
        reload_presets_button.clicked.connect(self._reload_export_presets)
        reload_watermarks_button = QPushButton("Nạp lại profile watermark")
        reload_watermarks_button.clicked.connect(self._reload_watermark_profiles)
        save_watermark_button = QPushButton("Lưu profile")
        save_watermark_button.clicked.connect(self._save_current_watermark_profile)
        save_watermark_as_new_button = QPushButton("Lưu thành bản mới")
        save_watermark_as_new_button.clicked.connect(
            lambda checked=False: self._save_current_watermark_profile(save_as_new=True)
        )
        export_button = QPushButton("Xuất video")
        export_button.clicked.connect(self._run_video_export_job)
        form_group = QGroupBox("Preset xuất video")
        self._export_group = form_group
        form = QFormLayout(form_group)
        self._export_form = form
        self._configure_form_layout(form)
        watermark_row = QHBoxLayout()
        watermark_row.addWidget(self._watermark_path_input)
        watermark_row.addWidget(choose_watermark_button)
        watermark_container = QWidget()
        watermark_container.setLayout(watermark_row)
        self._watermark_path_container = watermark_container
        watermark_actions = QHBoxLayout()
        watermark_actions.addWidget(reload_watermarks_button)
        watermark_actions.addWidget(save_watermark_button)
        watermark_actions.addWidget(save_watermark_as_new_button)
        watermark_actions.addStretch(1)
        watermark_actions_container = QWidget()
        watermark_actions_container.setLayout(watermark_actions)
        self._watermark_actions_container = watermark_actions_container
        watermark_numeric_row = QHBoxLayout()
        watermark_numeric_row.addWidget(QLabel("Độ mờ"))
        watermark_numeric_row.addWidget(self._watermark_opacity_input)
        watermark_numeric_row.addWidget(QLabel("Tỷ lệ"))
        watermark_numeric_row.addWidget(self._watermark_scale_input)
        watermark_numeric_row.addWidget(QLabel("Lề"))
        watermark_numeric_row.addWidget(self._watermark_margin_input)
        watermark_numeric_row.addStretch(1)
        watermark_numeric_container = QWidget()
        watermark_numeric_container.setLayout(watermark_numeric_row)
        self._watermark_numeric_container = watermark_numeric_container
        buttons = QHBoxLayout()
        buttons.addWidget(reload_presets_button)
        buttons.addWidget(export_button)
        buttons.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(buttons)
        layout.addWidget(self._export_summary)
        form.addRow("Preset xuất", self._export_preset_combo)
        form.addRow("Chế độ phụ đề", self._burn_subtitles_checkbox)
        form.addRow("Profile watermark", self._watermark_profile_combo)
        form.addRow("Tên profile", self._watermark_profile_name_input)
        form.addRow("Trạng thái profile", self._watermark_profile_status)
        form.addRow("Bật watermark", self._watermark_enabled_checkbox)
        form.addRow("Đường dẫn logo", watermark_container)
        form.addRow("Vị trí", self._watermark_position_combo)
        form.addRow("Độ mờ / Tỷ lệ / Lề", watermark_numeric_container)
        form.addRow("", watermark_actions_container)
        layout.addWidget(form_group)
        layout.addWidget(button_container)
        layout.addWidget(
            self._build_placeholder_group(
                "Hướng dẫn",
                "Chọn preset xuất bản để quyết định tỷ lệ khung hình và kiểu chèn phụ đề. "
                "Nếu cần logo, bạn có thể chỉnh nhanh trong form rồi lưu lại thành profile để dùng cho các lần xuất sau.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_settings_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("Đường dẫn công cụ và cài đặt ứng dụng")
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._ui_language_input = QLineEdit()
        self._settings_ui_mode_combo = QComboBox()
        self._settings_ui_mode_combo.addItem(UI_MODE_LABELS[UI_MODE_SIMPLE_V2], UI_MODE_SIMPLE_V2)
        self._settings_ui_mode_combo.addItem(UI_MODE_LABELS[UI_MODE_ADVANCED], UI_MODE_ADVANCED)
        self._settings_ui_mode_combo.currentIndexChanged.connect(self._handle_ui_mode_combo_changed)
        self._ffmpeg_path_input = QLineEdit()
        self._ffprobe_path_input = QLineEdit()
        self._mpv_path_input = QLineEdit()
        self._model_cache_input = QLineEdit()
        self._openai_key_input = QLineEdit()
        self._openai_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._default_translation_model_input = QLineEdit()
        self._ffmpeg_status = self._create_info_label("Chưa kiểm tra")
        self._settings_doctor_status = self._create_info_label("Doctor: chưa chạy")

        save_button = QPushButton("Lưu cài đặt")
        save_button.clicked.connect(self._save_settings)
        check_button = QPushButton("Kiểm tra FFmpeg")
        check_button.clicked.connect(self._check_ffmpeg)
        doctor_button = QPushButton("Chạy doctor")
        doctor_button.clicked.connect(self._run_settings_doctor_check)

        buttons = QHBoxLayout()
        buttons.addWidget(save_button)
        buttons.addWidget(check_button)
        buttons.addWidget(doctor_button)
        buttons.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(buttons)

        form.addRow("Ngôn ngữ giao diện", self._ui_language_input)
        form.addRow("Chế độ giao diện", self._settings_ui_mode_combo)
        form.addRow("Đường dẫn ffmpeg", self._ffmpeg_path_input)
        form.addRow("Đường dẫn ffprobe", self._ffprobe_path_input)
        form.addRow("Đường dẫn mpv DLL", self._mpv_path_input)
        form.addRow("Thư mục cache model", self._model_cache_input)
        form.addRow("OpenAI API key", self._openai_key_input)
        form.addRow("Mô hình dịch mặc định", self._default_translation_model_input)
        form.addRow("", button_container)
        form.addRow("Trạng thái kiểm tra", self._ffmpeg_status)
        form.addRow("Doctor summary", self._settings_doctor_status)

        layout.addWidget(group)
        layout.addWidget(
            self._build_placeholder_group(
                "Gợi ý",
                "OpenAI API key chỉ cần cho bước dịch. mpv DLL chỉ cần khi bạn muốn xem trước phụ đề trong mpv.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_logs_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._logs_info = self._create_info_label(f"Dữ liệu ứng dụng: {get_appdata_dir()}")
        self._logs_console = QPlainTextEdit()
        self._logs_console.setReadOnly(True)
        layout.addWidget(self._logs_info)
        layout.addWidget(self._logs_console)
        return widget

    def _build_placeholder_tab(self, title: str, description: str) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.addWidget(self._build_placeholder_group(title, description))
        layout.addStretch(1)
        return widget

    @staticmethod
    def _wrap_scrollable_tab(widget: QWidget) -> QScrollArea:
        widget.setMinimumWidth(0)
        widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        if widget.layout() is not None:
            widget.layout().setSizeConstraint(QLayout.SizeConstraint.SetMinAndMaxSize)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setWidget(widget)
        return scroll

    @staticmethod
    def _configure_form_layout(form: QFormLayout) -> None:
        form.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

    @staticmethod
    def _create_info_label(text: str) -> QLabel:
        label = QLabel(text)
        label.setWordWrap(True)
        label.setMinimumWidth(0)
        label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        return label

    @staticmethod
    def _configure_table_widget(table: QTableWidget) -> None:
        table.setMinimumWidth(0)
        table.setWordWrap(True)
        table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)

    @staticmethod
    def _build_placeholder_group(title: str, description: str) -> QGroupBox:
        group = QGroupBox(title)
        layout = QVBoxLayout(group)
        label = QLabel(description)
        label.setWordWrap(True)
        label.setStyleSheet("color: #5f6368; font-size: 12px;")
        layout.addWidget(label)
        return group

    @staticmethod
    def _make_table_item(
        text: str,
        *,
        editable: bool,
        user_data: object | None = None,
    ) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        if not editable:
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        if user_data is not None:
            item.setData(Qt.ItemDataRole.UserRole, user_data)
        return item

    @staticmethod
    def _subtitle_issue_color(severity: str) -> QColor:
        if severity == "error":
            return QColor(255, 219, 219)
        if severity == "warning":
            return QColor(255, 241, 204)
        return QColor()

    @staticmethod
    def _segment_to_editor_row(segment: object) -> dict[str, object]:
        meta_json = getattr(segment, "__getitem__", None)
        raw_meta = segment["meta_json"] if meta_json else "{}"
        return {
            "segment_id": str(segment["segment_id"]),
            "track_id": str(segment["track_id"]) if segment["track_id"] is not None else None,
            "source_segment_id": (
                str(segment["source_segment_id"]) if segment["source_segment_id"] is not None else None
            ),
            "segment_index": int(segment["segment_index"]),
            "start_ms": int(segment["start_ms"]),
            "end_ms": int(segment["end_ms"]),
            "source_lang": segment["source_lang"],
            "target_lang": segment["target_lang"],
            "source_text": segment["source_text"] or "",
            "translated_text": segment["translated_text"] or "",
            "subtitle_text": segment["subtitle_text"] or "",
            "tts_text": segment["tts_text"] or "",
            "audio_path": segment["audio_path"],
            "status": segment["status"] or "draft",
            "track_name": segment["track_name"] if "track_name" in segment.keys() else None,
            "track_kind": segment["track_kind"] if "track_kind" in segment.keys() else None,
            "meta_json": {} if not raw_meta else json.loads(raw_meta),
        }

    def _set_subtitle_table_rows(self, rows: list[dict[str, object]]) -> None:
        self._subtitle_editor_loading = True
        self._subtitle_table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            payload = dict(row)
            payload["segment_index"] = row_index
            self._subtitle_table.setItem(
                row_index,
                0,
                self._make_table_item(str(row_index), editable=False, user_data=payload),
            )
            self._subtitle_table.setItem(
                row_index,
                1,
                self._make_table_item(format_timestamp_ms(int(payload["start_ms"])), editable=True),
            )
            self._subtitle_table.setItem(
                row_index,
                2,
                self._make_table_item(format_timestamp_ms(int(payload["end_ms"])), editable=True),
            )
            self._subtitle_table.setItem(
                row_index,
                3,
                self._make_table_item(str(payload.get("source_text", "") or ""), editable=False),
            )
            self._subtitle_table.setItem(
                row_index,
                4,
                self._make_table_item(str(payload.get("translated_text", "") or ""), editable=True),
            )
            self._subtitle_table.setItem(
                row_index,
                5,
                self._make_table_item(str(payload.get("subtitle_text", "") or ""), editable=True),
            )
            self._subtitle_table.setItem(
                row_index,
                6,
                self._make_table_item(str(payload.get("tts_text", "") or ""), editable=True),
            )
            self._subtitle_table.setItem(
                row_index,
                7,
                self._make_table_item(str(payload.get("status", "draft") or "draft"), editable=False),
            )
        self._subtitle_table.resizeRowsToContents()
        self._subtitle_editor_loading = False

    def _replace_subtitle_editor_rows(
        self,
        rows: list[dict[str, object]],
        *,
        status_message: str,
    ) -> None:
        self._set_subtitle_table_rows(rows)
        self._mark_subtitle_editor_dirty(status_message)

    def _mark_subtitle_editor_dirty(self, status_message: str) -> None:
        self._subtitle_editor_dirty = True
        self._clear_subtitle_qc_ui()
        self._subtitle_editor_status.setText(status_message)
        self._schedule_preview_reload()

    def _choose_project_root(self) -> None:
        directory = QFileDialog.getExistingDirectory(
            self,
            "Chọn thư mục dự án",
            str(self._project_root_dialog_start_dir()),
        )
        if directory:
            self._project_root_input.setText(directory)
            self._project_root_user_edited = True

    def _choose_source_video(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn video nguồn",
            str(self._video_dialog_start_dir()),
            "Tệp video (*.mp4 *.mkv *.mov *.avi *.webm);;Tất cả tệp (*.*)",
        )
        if file_path:
            self._source_video_input.setText(file_path)
            self._apply_project_suggestions_from_source(Path(file_path))

    def _resolve_source_video_path(self) -> Path | None:
        raw_value = self._source_video_input.text().strip()
        if raw_value:
            candidate = Path(raw_value).expanduser()
            return candidate if candidate.exists() else None
        if self._current_workspace and self._current_workspace.source_video_path:
            return self._current_workspace.source_video_path
        return None

    @staticmethod
    def _subtitle_track_label(track: object | None) -> str:
        if track is None:
            return "-"
        return f"{track['name']} ({track['kind']})"

    def _load_active_subtitle_track_rows(
        self,
    ) -> tuple[ProjectDatabase, object | None, list[object]]:
        if not self._current_workspace:
            raise ValueError("Chưa có dự án")
        database = ProjectDatabase(self._current_workspace.database_path)
        track = database.get_active_subtitle_track(self._current_workspace.project_id)
        if track is None:
            track = database.ensure_canonical_subtitle_track(self._current_workspace.project_id)
            sync_project_snapshot(self._current_workspace)
        rows = (
            database.list_subtitle_events(
                self._current_workspace.project_id,
                track_id=str(track["track_id"]),
            )
            if track is not None
            else []
        )
        return database, track, rows

    @staticmethod
    def _record_value(record: object, key: str) -> object | None:
        if isinstance(record, dict):
            return record.get(key)
        try:
            return record[key]
        except (KeyError, IndexError, TypeError):
            return None

    @staticmethod
    def _record_text(record: object, key: str) -> str:
        value = MainWindow._record_value(record, key)
        return str(value or "").strip()

    @staticmethod
    def _normalize_language_code(value: object | None) -> str | None:
        text = str(value or "").strip().lower()
        if not text or text in {"-", "auto", "none", "null", "und", "unknown"}:
            return None
        return text.replace("_", "-").split("-", 1)[0]

    def _project_language_codes(
        self,
        database: ProjectDatabase,
        rows: list[object],
    ) -> tuple[str | None, str | None]:
        project_row = database.get_project()
        source_language = None
        for row in rows:
            source_language = self._normalize_language_code(self._record_value(row, "source_lang"))
            if source_language:
                break
        if source_language is None and project_row:
            source_language = self._normalize_language_code(project_row["source_language"])

        target_language = self._normalize_language_code(project_row["target_language"] if project_row else None)
        if target_language is None:
            for row in rows:
                target_language = self._normalize_language_code(self._record_value(row, "target_lang"))
                if target_language:
                    break
        return source_language, target_language

    def _requires_localized_output(
        self,
        database: ProjectDatabase,
        rows: list[object],
    ) -> bool:
        source_language, target_language = self._project_language_codes(database, rows)
        return bool(source_language and target_language and source_language != target_language)

    def _subtitle_output_text(self, row: object, *, require_localized: bool) -> str:
        if require_localized:
            return self._record_text(row, "subtitle_text") or self._record_text(row, "translated_text")
        return (
            self._record_text(row, "subtitle_text")
            or self._record_text(row, "translated_text")
            or self._record_text(row, "source_text")
        )

    def _tts_output_text(self, row: object, *, require_localized: bool) -> str:
        if require_localized:
            return (
                self._record_text(row, "tts_text")
                or self._record_text(row, "subtitle_text")
                or self._record_text(row, "translated_text")
            )
        return (
            self._record_text(row, "tts_text")
            or self._record_text(row, "subtitle_text")
            or self._record_text(row, "translated_text")
            or self._record_text(row, "source_text")
        )

    @staticmethod
    def _format_row_number_list(row_indexes: list[int], *, limit: int = 8) -> str:
        visible = [str(index + 1) for index in row_indexes[:limit]]
        if len(row_indexes) > limit:
            visible.append("...")
        return ", ".join(visible)

    def _focus_subtitle_table_row(self, row_index: int) -> None:
        if row_index < 0 or row_index >= self._subtitle_table.rowCount():
            return
        self._subtitle_table.selectRow(row_index)
        item = self._subtitle_table.item(row_index, 0)
        if item is not None:
            self._subtitle_table.scrollToItem(item, QAbstractItemView.ScrollHint.PositionAtCenter)

    def _missing_localized_row_indexes(
        self,
        rows: list[object],
        *,
        purpose: str,
        require_localized: bool,
    ) -> list[int]:
        missing_indexes: list[int] = []
        for row_index, row in enumerate(rows):
            if purpose == "tts":
                text = self._tts_output_text(row, require_localized=require_localized)
            else:
                text = self._subtitle_output_text(row, require_localized=require_localized)
            if not text:
                missing_indexes.append(row_index)
        return missing_indexes

    def _ensure_localized_rows_ready(
        self,
        database: ProjectDatabase,
        rows: list[object],
        *,
        purpose: str,
        dialog_title: str,
    ) -> bool:
        require_localized = self._requires_localized_output(database, rows)
        if not require_localized:
            return True
        missing_indexes = self._missing_localized_row_indexes(
            rows,
            purpose=purpose,
            require_localized=True,
        )
        if not missing_indexes:
            return True

        self._focus_subtitle_table_row(missing_indexes[0])
        if purpose == "tts":
            action_label = "lồng tiếng"
            column_hint = "Lời TTS, Phụ đề hoặc Bản dịch"
        else:
            action_label = "xuất phụ đề/video"
            column_hint = "Phụ đề hoặc Bản dịch"
        QMessageBox.warning(
            self,
            dialog_title,
            (
                f"Không thể {action_label} vì còn {len(missing_indexes)} dòng chưa có nội dung tiếng đích.\n"
                f"- Dòng: {self._format_row_number_list(missing_indexes)}\n"
                f"- Hãy hoàn tất bước dịch hoặc điền trực tiếp vào cột {column_hint} trước khi tiếp tục."
            ),
        )
        return False

    def _normalize_rows_for_qc(self, rows: list[object]) -> list[dict[str, object]]:
        normalized_rows: list[dict[str, object]] = []
        for row_index, row in enumerate(rows):
            normalized_rows.append(
                {
                    "segment_id": str(
                        self._record_value(row, "segment_id")
                        or self._record_value(row, "event_id")
                        or row_index
                    ),
                    "segment_index": int(
                        self._record_value(row, "segment_index")
                        or self._record_value(row, "event_index")
                        or row_index
                    ),
                    "start_ms": int(self._record_value(row, "start_ms") or 0),
                    "end_ms": int(self._record_value(row, "end_ms") or 0),
                    "source_text": self._record_text(row, "source_text"),
                    "translated_text": self._record_text(row, "translated_text"),
                    "subtitle_text": self._record_text(row, "subtitle_text"),
                }
            )
        return normalized_rows

    def _ensure_qc_passed_for_export(self, rows: list[object], *, dialog_title: str) -> bool:
        normalized_rows = self._normalize_rows_for_qc(rows)
        report = analyze_subtitle_rows(normalized_rows, config=SubtitleQcConfig())
        self._apply_qc_report_to_ui(report)
        self._append_log_line(
            f"QC phụ đề trước khi xuất: {report.error_count} lỗi, {report.warning_count} cảnh báo, {report.total_segments} dòng"
        )
        if report.error_count == 0:
            return True

        first_error = next((issue for issue in report.issues if issue.severity == "error"), report.issues[0])
        self._focus_subtitle_table_row(first_error.segment_index)
        QMessageBox.warning(
            self,
            dialog_title,
            (
                f"Không thể xuất khi QC còn {report.error_count} lỗi.\n"
                f"- Dòng lỗi đầu tiên: {first_error.segment_index + 1}\n"
                f"- {first_error.message}\n"
                "- Hãy sửa trong tab Phụ đề rồi thử lại."
            ),
        )
        return False

    def _missing_tts_artifact_row_indexes(
        self,
        rows: list[object],
        artifacts: list[object],
        *,
        require_localized: bool,
    ) -> list[int]:
        available_segment_ids = {
            str(getattr(item, "segment_id", ""))
            for item in artifacts
            if getattr(item, "raw_wav_path", None) is not None and getattr(item, "raw_wav_path").exists()
        }
        missing_indexes: list[int] = []
        for row_index, row in enumerate(rows):
            text = self._tts_output_text(row, require_localized=require_localized)
            if not text:
                continue
            segment_id = str(self._record_value(row, "segment_id") or "")
            if segment_id not in available_segment_ids:
                missing_indexes.append(row_index)
        return missing_indexes

    def _current_mixdown_inputs(self) -> tuple[float, float, Path | None, float] | None:
        try:
            original_volume = self._parse_volume_value(self._original_volume_input.text(), field_name="Âm lượng audio gốc")
            voice_volume = self._parse_volume_value(self._voice_volume_input.text(), field_name="Âm lượng giọng")
            bgm_volume = self._parse_volume_value(self._bgm_volume_input.text(), field_name="Âm lượng BGM")
        except ValueError:
            return None
        return original_volume, voice_volume, self._resolve_bgm_path(), bgm_volume

    def _current_mixdown_inputs_or_warn(
        self,
        *,
        dialog_title: str,
    ) -> tuple[float, float, Path | None, float] | None:
        mixdown_inputs = self._current_mixdown_inputs()
        if mixdown_inputs is not None:
            return mixdown_inputs
        try:
            self._parse_volume_value(self._original_volume_input.text(), field_name="Âm lượng audio gốc")
            self._parse_volume_value(self._voice_volume_input.text(), field_name="Âm lượng giọng")
            self._parse_volume_value(self._bgm_volume_input.text(), field_name="Âm lượng BGM")
        except ValueError as exc:
            QMessageBox.warning(self, dialog_title, str(exc))
        return None

    @staticmethod
    def _expected_mixed_audio_path(
        workspace: ProjectWorkspace,
        *,
        original_audio_path: Path,
        voice_track_path: Path,
        original_volume: float,
        voice_volume: float,
        bgm_path: Path | None,
        bgm_volume: float,
    ) -> Path:
        stage_hash = build_mixdown_stage_hash(
            original_audio_path=original_audio_path,
            voice_track_path=voice_track_path,
            original_volume=original_volume,
            voice_volume=voice_volume,
            bgm_path=bgm_path,
            bgm_volume=bgm_volume,
        )
        return workspace.cache_dir / "mix" / stage_hash / "mixed_audio.wav"

    @staticmethod
    def _expected_voice_track_path(
        workspace: ProjectWorkspace,
        *,
        artifacts: list[object],
        total_duration_ms: int,
    ) -> Path:
        stage_hash = build_voice_track_stage_hash(
            artifacts,
            total_duration_ms=total_duration_ms,
        )
        return workspace.cache_dir / "mix" / stage_hash / "voice_track.wav"

    def _expected_voice_track_path_for_rows(
        self,
        *,
        workspace: ProjectWorkspace,
        subtitle_rows: list[object],
        preset: object,
        total_duration_ms: int,
        require_localized: bool,
        segment_voice_presets: dict[str, object] | None = None,
    ) -> tuple[Path | None, list[int]]:
        tts_stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
            segment_voice_presets=segment_voice_presets,
        )
        cached = load_synthesized_segments(workspace, tts_stage_hash)
        if not cached:
            return None, []
        missing_artifact_indexes = self._missing_tts_artifact_row_indexes(
            subtitle_rows,
            cached.artifacts,
            require_localized=require_localized,
        )
        if missing_artifact_indexes:
            return None, missing_artifact_indexes
        return (
            self._expected_voice_track_path(
                workspace,
                artifacts=cached.artifacts,
                total_duration_ms=total_duration_ms,
            ),
            [],
        )

    def _subtitle_rows_changed_since_snapshot(self, rows: list[dict[str, object]]) -> bool:
        if len(rows) != len(self._subtitle_segment_snapshot):
            return True
        for row in rows:
            original = self._subtitle_segment_snapshot.get(str(row.get("segment_id")))
            if original is None:
                return True
            current_values = {
                "start_ms": int(row["start_ms"]),
                "end_ms": int(row["end_ms"]),
                "translated_text": str(row.get("translated_text", "") or ""),
                "subtitle_text": str(row.get("subtitle_text", "") or ""),
                "tts_text": str(row.get("tts_text", "") or ""),
            }
            if any(current_values[key] != original[key] for key in current_values):
                return True
        return False

    def _invalidate_subtitle_pipeline_outputs(self, *, clear_tts_audio: bool) -> None:
        self._last_subtitle_outputs = {}
        self._last_export_output = None
        if clear_tts_audio:
            self._last_tts_manifest = None
            self._last_voice_track_output = None
            self._last_mixed_audio_output = None

    def _has_active_preview(self) -> bool:
        return self._preview_controller.is_active and self._live_preview_ass_path is not None

    def _cancel_preview_reload(self) -> None:
        if self._preview_reload_timer.isActive():
            self._preview_reload_timer.stop()

    def _schedule_preview_reload(self) -> None:
        if not self._has_active_preview():
            return
        self._preview_reload_timer.start()

    def _current_subtitle_subtext_mode(self) -> str:
        if not self._current_workspace:
            return "off"
        return resolve_subtitle_subtext_mode(self._current_workspace.root_dir)

    def _sync_subtitle_subtext_toggle(self) -> None:
        if self._subtitle_subtext_toggle is None:
            return
        with QSignalBlocker(self._subtitle_subtext_toggle):
            self._subtitle_subtext_toggle.setChecked(
                self._current_workspace is not None
                and self._current_subtitle_subtext_mode() == "source_text"
            )

    def _handle_subtitle_subtext_toggle(self, checked: bool) -> None:
        if self._subtitle_subtext_toggle is None:
            return
        if not self._current_workspace:
            with QSignalBlocker(self._subtitle_subtext_toggle):
                self._subtitle_subtext_toggle.setChecked(False)
            return
        mode = "source_text" if checked else "off"
        state = set_project_subtitle_subtext_mode(
            self._current_workspace.root_dir,
            mode,
            applied_at=utc_now_iso(),
        )
        self._last_subtitle_outputs = {}
        self._last_export_output = None
        self._subtitle_summary.setText(
            "Subtext gốc: bật"
            if state.subtitle_subtext_mode == "source_text"
            else "Subtext gốc: tắt"
        )
        self._schedule_preview_reload()

    def _export_live_preview_ass_from_editor(self) -> Path:
        if not self._current_workspace:
            raise ValueError("Chưa có dự án")
        rows = self._collect_subtitle_table_rows()
        ass_path = export_preview_subtitles(
            self._current_workspace,
            segments=rows,
            format_name="ass",
            subtitle_subtext_mode=self._current_subtitle_subtext_mode(),
        )
        self._live_preview_ass_path = ass_path
        self._last_subtitle_outputs["ass"] = ass_path
        return ass_path

    def _flush_preview_reload(self) -> None:
        if not self._has_active_preview():
            return
        try:
            ass_path = self._export_live_preview_ass_from_editor()
            self._preview_controller.reload_subtitles(ass_path)
        except ValueError:
            self._subtitle_editor_status.setText("mpv đang chờ phụ đề hợp lệ trước khi tự nạp lại")
            return
        except (PreviewUnavailableError, FileNotFoundError, RuntimeError) as exc:
            self._cancel_preview_reload()
            self._live_preview_ass_path = None
            self._subtitle_editor_status.setText(f"Đã dừng tự nạp lại preview mpv: {exc}")
            self._append_log_line(f"Tự nạp lại preview mpv thất bại: {exc}")
            return

        self._subtitle_editor_status.setText("Đã nạp lại preview mpv từ trình biên tập phụ đề")

    def _ensure_editable_subtitle_track(
        self,
        database: ProjectDatabase,
        active_track: object,
    ) -> tuple[object, bool]:
        if str(active_track["kind"]) != CANONICAL_SUBTITLE_TRACK_KIND:
            return active_track, False
        if not self._current_workspace:
            raise ValueError("Chưa có dự án")

        now = utc_now_iso()
        forked_track = database.create_subtitle_track(
            SubtitleTrackRecord(
                track_id=f"{self._current_workspace.project_id}:user:{uuid4()}",
                project_id=self._current_workspace.project_id,
                name="Bản phụ đề chỉnh sửa",
                kind=USER_SUBTITLE_TRACK_KIND,
                notes="Được tách từ track phụ đề chuẩn khi lưu chỉnh sửa từ trình biên tập.",
                created_at=now,
                updated_at=now,
            ),
            set_active=True,
        )
        sync_project_snapshot(self._current_workspace)
        return forked_track, True

    def _create_project(self) -> None:
        source_video_path = self._resolve_source_video_path()
        project_profile_id = self._selected_project_profile_id()
        if project_profile_id is None and self._is_simple_ui_mode():
            project_profile_id = "zh-vi-narration-fast-v2-vieneu"
        project_name = self._project_name_input.text().strip() or self._suggest_project_name(source_video_path)
        root_dir = Path(
            self._project_root_input.text().strip()
            or str(self._suggest_project_root(source_video_path=source_video_path, project_name=project_name))
        ).expanduser()
        request = ProjectInitRequest(
            name=project_name,
            root_dir=root_dir,
            source_language=self._source_lang_combo.currentText(),
            target_language=self._target_lang_combo.currentText(),
            source_video_path=source_video_path,
            project_profile_id=project_profile_id,
        )
        try:
            workspace = bootstrap_project(request)
        except FileExistsError as exc:
            QMessageBox.warning(self, "Không thể tạo dự án", str(exc))
            return
        self._set_current_workspace(workspace)
        QMessageBox.information(self, "Thành công", f"Đã tạo dự án tại:\n{workspace.root_dir}")

    def _open_project(self) -> None:
        directory = QFileDialog.getExistingDirectory(
            self,
            "Mở thư mục dự án",
            str(self._project_root_dialog_start_dir()),
        )
        if not directory:
            return
        try:
            workspace = open_project(Path(directory))
        except FileNotFoundError as exc:
            QMessageBox.warning(self, "Không thể mở dự án", str(exc))
            return
        self._set_current_workspace(workspace)

    def _set_current_workspace(self, workspace: ProjectWorkspace) -> None:
        self._cancel_preview_reload()
        self._preview_controller.close()
        self._live_preview_ass_path = None
        self._stop_workflow(update_status=False)
        self._current_workspace = workspace
        self._media_metadata = None
        self._audio_artifacts = load_cached_audio_artifacts(workspace)
        self._last_subtitle_outputs = {}
        self._last_export_output = None
        self._last_tts_manifest = None
        self._last_voice_track_output = None
        self._last_mixed_audio_output = None
        self._project_name_user_edited = False
        self._project_root_user_edited = False
        self._project_name_input.setText(workspace.name)
        self._project_root_input.setText(str(workspace.root_dir))
        self._restore_workspace_runtime_state(workspace)
        self._source_video_input.setText(str(workspace.source_video_path) if workspace.source_video_path else "")
        state = load_project_profile_state(workspace.root_dir)
        self._reload_project_profile_options(state.project_profile_id if state is not None else None)
        self._reload_prompt_templates()
        self._reload_voice_presets()
        self._reload_export_presets()
        self._reload_watermark_profiles()
        self._apply_ui_mode()
        if state is not None:
            self._original_volume_input.setText(
                f"{float(state.recommended_original_volume):g}"
                if state.recommended_original_volume is not None
                else self._original_volume_input.text()
            )
            self._voice_volume_input.setText(
                f"{float(state.recommended_voice_volume):g}"
                if state.recommended_voice_volume is not None
                else self._voice_volume_input.text()
            )
        if state is not None and state.project_profile_id == "zh-vi-narration-fast-v2-vieneu":
            self._bgm_volume_input.setText("0.0")
        self._project_summary.setText(
            "Dự án hiện tại:\n"
            f"- Tên: {workspace.name}\n"
            f"- Thư mục: {workspace.root_dir}\n"
            f"- CSDL: {workspace.database_path}\n"
            f"- Cache: {workspace.cache_dir}\n"
            f"- Xuất bản: {workspace.exports_dir}"
        )
        self._refresh_workspace_views()
        self._logs_info.setText(
            f"Dữ liệu ứng dụng: {get_appdata_dir()}\nNhật ký dự án: {workspace.logs_dir}"
        )
        self._append_log_line(f"Mở workspace: {workspace.root_dir}")
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION} - {workspace.name}")
        self._reload_subtitle_editor_from_db(force=True)
        self._reload_review_queue()

    def _restore_workspace_runtime_state(self, workspace: ProjectWorkspace) -> None:
        database = ProjectDatabase(workspace.database_path)
        project_job_runs = [
            row
            for row in database.list_job_runs()
            if row["project_id"] == workspace.project_id
        ]
        restored = restore_pipeline_state(project_job_runs)
        self._last_subtitle_outputs = dict(restored.subtitle_outputs)
        self._last_tts_manifest = restored.tts_manifest_path
        self._last_voice_track_output = restored.voice_track_path
        self._last_mixed_audio_output = restored.mixed_audio_path
        self._last_export_output = restored.export_output_path

    def _workflow_stage_label(self, stage: str) -> str:
        labels = {
            "probe_media": "Đọc metadata",
            "extract_audio": "Tách âm thanh",
            "asr": "ASR",
            "translate": "Dịch",
            "tts": "TTS",
            "voice_track": "Tạo track giọng",
            "mixdown": "Trộn âm thanh",
            "export_video": "Xuất video",
        }
        return labels.get(stage, stage)

    def _workflow_stage_runner(self, stage: str):
        runners = {
            "probe_media": self._run_probe_media_job,
            "extract_audio": self._run_extract_audio_job,
            "asr": self._run_asr_job,
            "translate": self._run_translation_job,
            "tts": self._run_tts_job,
            "voice_track": self._run_build_voice_track_job,
            "mixdown": self._run_mixdown_job,
            "export_video": self._run_video_export_job,
        }
        return runners.get(stage)

    def _update_workflow_status_label(self, message: str | None = None) -> None:
        if message:
            self._workflow_status.setText(message)
            return
        if self._workflow_current_stage:
            remaining = ", ".join(self._workflow_stage_label(stage) for stage in self._workflow_queue)
            tail = f"\n- Còn lại: {remaining}" if remaining else ""
            self._workflow_status.setText(
                f"Quy trình nhanh: đang chạy {self._workflow_name or '-'}\n"
                f"- Bước hiện tại: {self._workflow_stage_label(self._workflow_current_stage)}"
                f"{tail}"
            )
            return
        self._workflow_status.setText("Quy trình nhanh: sẵn sàng")

    def _start_workflow(self, stages: list[str], *, workflow_name: str) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        if not stages:
            return
        self._workflow_queue = list(stages)
        self._workflow_current_stage = None
        self._workflow_current_job_id = None
        self._workflow_name = workflow_name
        self._append_log_line(f"Khởi động quy trình nhanh: {workflow_name}")
        self._run_next_workflow_stage()

    def _activate_tab(self, title: str) -> None:
        for index in range(self._tabs.count()):
            if self._tabs.tabText(index) == title:
                self._tabs.setCurrentIndex(index)
                return

    def _pending_review_count(self) -> int:
        if not self._current_workspace:
            return 0
        database = ProjectDatabase(self._current_workspace.database_path)
        if self._current_translation_mode(database.get_project()) != "contextual_v2":
            return 0
        return database.count_pending_segment_reviews(self._current_workspace.project_id)

    def _open_review_queue_tab(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        self._reload_review_queue()
        self._activate_tab("ASR & Dịch")
        pending_review_count = self._pending_review_count()
        if pending_review_count <= 0:
            QMessageBox.information(self, "Review ngữ cảnh", "Hiện không còn dòng review nào cần xử lý.")
            return
        if self._review_table.rowCount() > 0:
            self._review_table.selectRow(0)
        self._review_table.setFocus()

    def _start_simple_finish_workflow(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        pending_review_count = self._pending_review_count()
        if pending_review_count > 0:
            self._open_review_queue_tab()
            QMessageBox.warning(
                self,
                "Hoàn thiện video",
                (
                    f"Còn {pending_review_count} dòng chưa qua semantic review/QC.\n"
                    "Hãy xử lý review trước rồi bấm lại `Hoàn thiện video`."
                ),
            )
            return
        self._start_workflow(
            ["tts", "voice_track", "mixdown", "export_video"],
            workflow_name="Hoàn thiện video",
        )

    def _refresh_export_access_actions(self) -> None:
        output_path = self._last_export_output
        export_ready = bool(output_path and Path(output_path).exists())
        if getattr(self, "_open_export_video_button", None) is None:
            return
        self._open_export_video_button.setEnabled(export_ready)
        self._open_export_video_button.setToolTip(
            f"Mở video export cuối cùng:\n{output_path}"
            if export_ready and output_path is not None
            else "Chưa có video export nào sẵn sàng."
        )

    def _open_last_export_video(self) -> None:
        output_path = self._last_export_output
        if output_path is None or not Path(output_path).exists():
            self._refresh_export_access_actions()
            QMessageBox.information(
                self,
                "Mở video xuất",
                "Chưa có video export nào sẵn sàng để mở.",
            )
            return
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(output_path))):
            QMessageBox.warning(
                self,
                "Mở video xuất",
                f"Không thể mở video export:\n{output_path}",
            )

    def _run_next_workflow_stage(self) -> None:
        if not self._workflow_queue:
            self._workflow_current_stage = None
            self._workflow_current_job_id = None
            self._update_workflow_status_label(
                f"Quy trình nhanh: đã hoàn tất {self._workflow_name or 'quy trình'}"
            )
            self._workflow_name = None
            return

        next_stage = self._workflow_queue[0]
        runner = self._workflow_stage_runner(next_stage)
        if runner is None:
            self._stop_workflow(
                message=f"Quy trình nhanh đã dừng: không tìm thấy tác vụ xử lý cho bước {next_stage}",
            )
            return

        job_id = runner()
        if not job_id:
            self._stop_workflow(
                message=(
                    f"Quy trình nhanh đã dừng ở bước {self._workflow_stage_label(next_stage)}. "
                    "Hãy bổ sung dữ liệu hoặc cấu hình rồi chạy lại."
                ),
            )
            return
        self._workflow_current_stage = next_stage
        self._workflow_current_job_id = job_id
        self._update_workflow_status_label()

    def _advance_workflow_on_success(self, job_id: str, stage: str) -> None:
        if not self._workflow_current_job_id or job_id != self._workflow_current_job_id:
            return
        if not self._workflow_queue or self._workflow_queue[0] != stage:
            return
        self._workflow_queue.pop(0)
        self._workflow_current_stage = None
        self._workflow_current_job_id = None
        if self._workflow_queue:
            QTimer.singleShot(0, self._run_next_workflow_stage)
        else:
            self._update_workflow_status_label(
                f"Quy trình nhanh: đã hoàn tất {self._workflow_name or 'quy trình'}"
            )
            self._workflow_name = None

    def _stop_workflow(
        self,
        checked: bool = False,
        *,
        message: str | None = None,
        update_status: bool = True,
    ) -> None:
        del checked
        self._workflow_queue = []
        self._workflow_current_stage = None
        self._workflow_current_job_id = None
        self._workflow_name = None
        if update_status:
            self._update_workflow_status_label(message or "Quy trình nhanh: đã dừng hàng đợi hiện tại")

    def _reload_subtitle_editor_from_db(self, *, force: bool = False) -> None:
        if not self._current_workspace:
            self._subtitle_table.setRowCount(0)
            self._subtitle_segment_snapshot = {}
            self._subtitle_editor_dirty = False
            self._sync_subtitle_subtext_toggle()
            self._subtitle_editor_status.setText("Chưa mở dự án")
            return
        if self._subtitle_editor_dirty and not force:
            self._subtitle_editor_status.setText("Trình biên tập có thay đổi chưa lưu. Hãy lưu hoặc nạp lại thủ công.")
            return

        _database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        editor_rows = [self._segment_to_editor_row(segment) for segment in subtitle_rows]
        self._set_subtitle_table_rows(editor_rows)
        self._subtitle_segment_snapshot = {}
        for row in editor_rows:
            segment_id = str(row["segment_id"])
            self._subtitle_segment_snapshot[segment_id] = {
                "start_ms": int(row["start_ms"]),
                "end_ms": int(row["end_ms"]),
                "translated_text": str(row.get("translated_text", "") or ""),
                "subtitle_text": str(row.get("subtitle_text", "") or ""),
                "tts_text": str(row.get("tts_text", "") or ""),
                "status": str(row.get("status", "draft") or "draft"),
            }
        self._subtitle_editor_dirty = False
        self._clear_subtitle_qc_ui()
        self._sync_subtitle_subtext_toggle()
        self._subtitle_editor_status.setText(
            f"Đã nạp {len(subtitle_rows)} dòng từ {self._subtitle_track_label(active_track)} vào trình biên tập"
        )
        if subtitle_rows:
            self._schedule_preview_reload()

    def _handle_subtitle_item_changed(self, _item: QTableWidgetItem) -> None:
        if self._subtitle_editor_loading:
            return
        self._mark_subtitle_editor_dirty("Trình biên tập có thay đổi chưa lưu")

    def _collect_subtitle_table_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for row_index in range(self._subtitle_table.rowCount()):
            segment_item = self._subtitle_table.item(row_index, 0)
            if segment_item is None:
                continue
            payload = segment_item.data(Qt.ItemDataRole.UserRole)
            if not isinstance(payload, dict):
                continue
            start_ms = parse_timestamp_ms(self._subtitle_table.item(row_index, 1).text())
            end_ms = parse_timestamp_ms(self._subtitle_table.item(row_index, 2).text())
            if end_ms <= start_ms:
                raise ValueError(f"Dòng {row_index + 1}: thời gian kết thúc phải lớn hơn thời gian bắt đầu.")
            row_payload = dict(payload)
            row_payload.update(
                {
                    "segment_index": row_index,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "source_text": self._subtitle_table.item(row_index, 3).text().strip(),
                    "translated_text": self._subtitle_table.item(row_index, 4).text().strip(),
                    "subtitle_text": self._subtitle_table.item(row_index, 5).text().strip(),
                    "tts_text": self._subtitle_table.item(row_index, 6).text().strip(),
                    "status": self._subtitle_table.item(row_index, 7).text().strip() or "draft",
                }
            )
            rows.append(row_payload)
        return rows

    def _collect_subtitle_edits(self) -> list[dict[str, object]]:
        edits: list[dict[str, object]] = []
        for current in self._collect_subtitle_table_rows():
            segment_id = current["segment_id"]
            original = self._subtitle_segment_snapshot.get(str(segment_id))
            if not original:
                continue
            current_values = {
                "start_ms": current["start_ms"],
                "end_ms": current["end_ms"],
                "translated_text": current["translated_text"],
                "subtitle_text": current["subtitle_text"],
                "tts_text": current["tts_text"],
            }
            if all(current_values[key] == original[key] for key in current_values):
                continue
            current_values["segment_id"] = str(segment_id)
            current_values["status"] = "edited"
            edits.append(current_values)
        return edits

    def _clear_subtitle_qc_ui(self) -> None:
        self._subtitle_qc_report = SubtitleQcReport(total_segments=0, issues=[])
        self._subtitle_qc_summary.setText("QC phụ đề chưa được chạy")
        self._subtitle_qc_table.setRowCount(0)
        blocker = QSignalBlocker(self._subtitle_table)
        try:
            for row_index in range(self._subtitle_table.rowCount()):
                for column_index in range(self._subtitle_table.columnCount()):
                    item = self._subtitle_table.item(row_index, column_index)
                    if item is not None:
                        item.setBackground(QColor())
        finally:
            del blocker

    def _apply_qc_report_to_ui(self, report: SubtitleQcReport) -> None:
        self._subtitle_qc_report = report
        issue_count = len(report.issues)
        self._subtitle_qc_summary.setText(
            "QC phụ đề:\n"
            f"- Tổng số dòng: {report.total_segments}\n"
            f"- Lỗi: {report.error_count}\n"
            f"- Cảnh báo: {report.warning_count}\n"
            f"- Dòng đạt chuẩn: {report.ok_count}\n"
            "- Luật mặc định: tối đa 2 dòng, 42 CPL, 18 CPS, thời lượng 800-7000 ms"
        )
        self._subtitle_qc_table.setRowCount(issue_count)

        worst_issue_by_segment: dict[str, SubtitleQcIssue] = {}
        severity_rank = {"error": 2, "warning": 1}
        for issue in report.issues:
            existing = worst_issue_by_segment.get(issue.segment_id)
            if not existing or severity_rank.get(issue.severity, 0) > severity_rank.get(existing.severity, 0):
                worst_issue_by_segment[issue.segment_id] = issue

        for row_index, issue in enumerate(report.issues):
            self._subtitle_qc_table.setItem(
                row_index,
                0,
                self._make_table_item(str(issue.segment_index), editable=False, user_data=issue.segment_id),
            )
            self._subtitle_qc_table.setItem(row_index, 1, self._make_table_item(issue.code, editable=False))
            self._subtitle_qc_table.setItem(row_index, 2, self._make_table_item(issue.severity, editable=False))
            self._subtitle_qc_table.setItem(row_index, 3, self._make_table_item(issue.message, editable=False))

        self._subtitle_qc_table.resizeRowsToContents()
        for row_index in range(self._subtitle_table.rowCount()):
            segment_item = self._subtitle_table.item(row_index, 0)
            if segment_item is None:
                continue
            payload = segment_item.data(Qt.ItemDataRole.UserRole)
            segment_id = str(payload.get("segment_id")) if isinstance(payload, dict) else ""
            issue = worst_issue_by_segment.get(segment_id)
            color = self._subtitle_issue_color(issue.severity) if issue else QColor()
            for column_index in range(self._subtitle_table.columnCount()):
                item = self._subtitle_table.item(row_index, column_index)
                if item is not None:
                    item.setBackground(color)

    def _run_subtitle_qc(self) -> None:
        try:
            rows = self._collect_subtitle_table_rows()
        except ValueError as exc:
            QMessageBox.warning(self, "QC phụ đề", str(exc))
            return
        report = analyze_subtitle_rows(rows, config=SubtitleQcConfig())
        self._apply_qc_report_to_ui(report)
        self._append_log_line(
            f"QC phụ đề: {report.error_count} lỗi, {report.warning_count} cảnh báo, {report.total_segments} dòng"
        )

    def _apply_shift_to_subtitle_rows(self) -> None:
        if self._subtitle_table.rowCount() == 0:
            return
        try:
            shift_ms = int(self._shift_input.text().strip() or "0")
        except ValueError:
            QMessageBox.warning(self, "Biên tập phụ đề", "Độ dịch (ms) phải là số nguyên.")
            return

        self._subtitle_editor_loading = True
        for row_index in range(self._subtitle_table.rowCount()):
            start_ms = parse_timestamp_ms(self._subtitle_table.item(row_index, 1).text())
            end_ms = parse_timestamp_ms(self._subtitle_table.item(row_index, 2).text())
            new_start = max(0, start_ms + shift_ms)
            new_end = max(new_start + 1, end_ms + shift_ms)
            self._subtitle_table.item(row_index, 1).setText(format_timestamp_ms(new_start))
            self._subtitle_table.item(row_index, 2).setText(format_timestamp_ms(new_end))
        self._subtitle_editor_loading = False
        self._mark_subtitle_editor_dirty(
            f"Đã dịch toàn bộ mốc thời gian {shift_ms} ms. Hãy lưu để ghi vào CSDL."
        )

    def _apply_find_replace(self) -> None:
        needle = self._find_input.text()
        if not needle:
            QMessageBox.warning(self, "Biên tập phụ đề", "Hãy nhập chuỗi cần tìm.")
            return
        replacement = self._replace_input.text()
        target = self._replace_target_combo.currentData()
        column_map = {
            "translated": [4],
            "subtitle": [5],
            "tts": [6],
            "all": [4, 5, 6],
        }
        columns = column_map.get(str(target), [5])
        replacement_count = 0
        self._subtitle_editor_loading = True
        for row_index in range(self._subtitle_table.rowCount()):
            for column_index in columns:
                item = self._subtitle_table.item(row_index, column_index)
                if item is None:
                    continue
                original_text = item.text()
                updated_text = original_text.replace(needle, replacement)
                if updated_text != original_text:
                    replacement_count += original_text.count(needle)
                    item.setText(updated_text)
        self._subtitle_editor_loading = False
        if replacement_count == 0:
            self._subtitle_editor_status.setText("Không tìm thấy chuỗi cần thay trong trình biên tập")
            return
        self._mark_subtitle_editor_dirty(
            f"Đã thay {replacement_count} lượt. Hãy lưu để ghi vào CSDL."
        )

    def _selected_subtitle_start_ms(self) -> int:
        row_index = self._subtitle_table.currentRow()
        if row_index < 0:
            return 0
        start_item = self._subtitle_table.item(row_index, 1)
        if start_item is None:
            return 0
        return parse_timestamp_ms(start_item.text())

    def _preview_subtitles(self, *, start_from_selected: bool) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        if not self._ensure_doctor_ready(stages=["preview"], dialog_title="Preview"):
            return
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Biên tập phụ đề", "Không thể lưu chỉnh sửa phụ đề trước khi xem trước.")
            return

        workspace = self._current_workspace
        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "Chưa có video", "Hãy chọn video nguồn hợp lệ.")
            return
        if not self._selected_export_preset():
            QMessageBox.warning(self, "Preset xuất", "Không tìm thấy preset xuất trong dự án.")
            return

        _database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR và dịch trước.")
            return

        try:
            self._cancel_preview_reload()
            ass_path = export_preview_subtitles(
                workspace,
                segments=subtitle_rows,
                format_name="ass",
                subtitle_subtext_mode=self._current_subtitle_subtext_mode(),
            )
            self._live_preview_ass_path = ass_path
            self._last_subtitle_outputs["ass"] = ass_path
            self._preview_controller.preview(
                source_video_path=source_video_path,
                subtitle_path=ass_path,
                mpv_dll_path=self._settings.dependency_paths.mpv_dll_path,
                start_ms=self._selected_subtitle_start_ms() if start_from_selected else 0,
            )
        except (PreviewUnavailableError, FileNotFoundError, RuntimeError) as exc:
            QMessageBox.warning(self, "Xem trước mpv", str(exc))
            return

        self._subtitle_editor_status.setText(
            "Đang mở preview mpv cho "
            f"{self._subtitle_track_label(active_track)}"
            + (" từ dòng đang chọn" if start_from_selected else " từ đầu video")
        )
        self._refresh_workspace_views()
        self._append_log_line(f"Mo preview mpv voi ASS: {ass_path}")

    def _save_subtitle_edits(self, checked: bool = False, *, silent: bool = False) -> bool:
        del checked
        if not self._current_workspace:
            if not silent:
                QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return False
        if not self._subtitle_editor_dirty:
            self._subtitle_editor_status.setText("Không có thay đổi cần lưu")
            return True

        try:
            rows = self._collect_subtitle_table_rows()
        except ValueError as exc:
            if not silent:
                QMessageBox.warning(self, "Biên tập phụ đề", str(exc))
            return False
        downstream_artifacts_stale = self._subtitle_rows_changed_since_snapshot(rows)
        if downstream_artifacts_stale:
            for row in rows:
                row["audio_path"] = None
                row["status"] = "edited"

        database = ProjectDatabase(self._current_workspace.database_path)
        active_track = database.get_active_subtitle_track(self._current_workspace.project_id)
        if active_track is None:
            active_track = database.ensure_canonical_subtitle_track(self._current_workspace.project_id)
            sync_project_snapshot(self._current_workspace)
        active_track, forked_from_canonical = self._ensure_editable_subtitle_track(database, active_track)
        records = build_subtitle_event_records(
            self._current_workspace.project_id,
            str(active_track["track_id"]),
            rows,
        )
        database.replace_subtitle_events(
            self._current_workspace.project_id,
            str(active_track["track_id"]),
            records,
        )
        self._invalidate_subtitle_pipeline_outputs(clear_tts_audio=downstream_artifacts_stale)
        self._reload_subtitle_editor_from_db(force=True)
        self._refresh_workspace_views()
        track_label = self._subtitle_track_label(active_track)
        if forked_from_canonical:
            self._append_log_line(f"Đã tách track phụ đề chuẩn sang track chỉnh sửa: {track_label}")
        self._append_log_line(f"Đã lưu track phụ đề {track_label} với {len(records)} dòng vào CSDL")
        if downstream_artifacts_stale:
            self._append_log_line("Đã xoá trạng thái TTS/track giọng/audio trộn cũ vì track phụ đề đã thay đổi.")
        if not silent:
            QMessageBox.information(
                self,
                "Biên tập phụ đề",
                (
                    f"Đã lưu track phụ đề {track_label} ({len(records)} dòng)."
                    + (" Track chuẩn đã được tách thành track chỉnh sửa riêng." if forked_from_canonical else "")
                ),
            )
        return True

    def _apply_translated_to_subtitle(self) -> None:
        if self._subtitle_table.rowCount() == 0:
            return
        self._subtitle_editor_loading = True
        for row_index in range(self._subtitle_table.rowCount()):
            source_text = self._subtitle_table.item(row_index, 3).text()
            translated_text = self._subtitle_table.item(row_index, 4).text()
            subtitle_item = self._subtitle_table.item(row_index, 5)
            tts_item = self._subtitle_table.item(row_index, 6)
            suggested = suggest_subtitle_text(translated_text, source_text)
            if subtitle_item is not None:
                subtitle_item.setText(suggested)
            if tts_item is not None and not tts_item.text().strip():
                tts_item.setText(suggested)
        self._subtitle_editor_loading = False
        self._mark_subtitle_editor_dirty("Đã chép bản dịch sang cột phụ đề. Hãy lưu để ghi vào CSDL.")

    def _apply_subtitle_to_tts(self) -> None:
        if self._subtitle_table.rowCount() == 0:
            return
        updated_count = 0
        self._subtitle_editor_loading = True
        for row_index in range(self._subtitle_table.rowCount()):
            source_text = self._subtitle_table.item(row_index, 3).text()
            translated_text = self._subtitle_table.item(row_index, 4).text()
            subtitle_text = self._subtitle_table.item(row_index, 5).text()
            tts_item = self._subtitle_table.item(row_index, 6)
            if tts_item is None:
                continue
            suggested = suggest_tts_text(subtitle_text, translated_text, source_text)
            if suggested and tts_item.text().strip() != suggested:
                tts_item.setText(suggested)
                updated_count += 1
        self._subtitle_editor_loading = False
        if updated_count:
            self._mark_subtitle_editor_dirty(
                f"Đã tạo Lời TTS từ phụ đề cho {updated_count} dòng. Hãy lưu để ghi vào CSDL."
            )
        else:
            self._subtitle_editor_status.setText("Lời TTS hiện tại đã khớp với phụ đề hoặc chưa có nội dung để tạo.")

    def _polish_tts_texts(self) -> None:
        if self._subtitle_table.rowCount() == 0:
            return
        updated_count = 0
        self._subtitle_editor_loading = True
        for row_index in range(self._subtitle_table.rowCount()):
            source_text = self._subtitle_table.item(row_index, 3).text()
            translated_text = self._subtitle_table.item(row_index, 4).text()
            subtitle_text = self._subtitle_table.item(row_index, 5).text()
            tts_item = self._subtitle_table.item(row_index, 6)
            if tts_item is None:
                continue
            polished = suggest_tts_text(
                subtitle_text,
                translated_text,
                source_text,
                existing_tts_text=tts_item.text(),
            )
            if polished and tts_item.text().strip() != polished:
                tts_item.setText(polished)
                updated_count += 1
        self._subtitle_editor_loading = False
        if updated_count:
            self._mark_subtitle_editor_dirty(
                f"Đã làm mượt Lời TTS cho {updated_count} dòng. Hãy lưu để ghi vào CSDL."
            )
        else:
            self._subtitle_editor_status.setText("Không có Lời TTS nào cần làm mượt thêm.")

    def _choose_bgm_file(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn tệp BGM",
            str(self._video_dialog_start_dir()),
            "Tệp âm thanh (*.wav *.mp3 *.m4a *.aac *.flac *.ogg);;Tất cả tệp (*.*)",
        )
        if file_path:
            self._bgm_path_input.setText(file_path)

    def _choose_vieneu_ref_audio_file(self) -> None:
        initial_dir = self._current_workspace.root_dir if self._current_workspace else self._video_dialog_start_dir()
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn audio mẫu cho VieNeu clone",
            str(initial_dir),
            "Tệp âm thanh (*.wav *.mp3 *.m4a *.aac *.flac *.ogg);;Tất cả tệp (*.*)",
        )
        if file_path:
            self._vieneu_ref_audio_input.setText(file_path)
            self._refresh_workspace_views()

    def _choose_watermark_file(self) -> None:
        initial_dir = self._current_workspace.root_dir if self._current_workspace else self._video_dialog_start_dir()
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn watermark hoặc logo",
            str(initial_dir),
            "Tệp ảnh (*.png *.webp *.jpg *.jpeg);;Tất cả tệp (*.*)",
        )
        if file_path:
            self._watermark_path_input.setText(file_path)
            self._refresh_workspace_views()

    def _reload_voice_presets(self) -> None:
        selected_preset_id = self._voice_combo.currentData()
        if not self._current_workspace:
            self._voice_presets = []
            blocked = self._voice_combo.blockSignals(True)
            self._voice_combo.clear()
            self._voice_combo.blockSignals(blocked)
            self._vieneu_environment = detect_vieneu_installation()
            self._voice_info.setText("Runtime giọng đọc: chưa mở dự án")
            self._sync_voice_preset_form()
            self._reload_speaker_bindings()
            self._reload_voice_policies()
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        project_selected_preset_id = database.get_active_voice_preset_id(self._current_workspace.project_id)
        self._voice_presets = list_voice_presets(self._current_workspace.root_dir)
        blocked = self._voice_combo.blockSignals(True)
        self._voice_combo.clear()
        for preset in self._voice_presets:
            self._voice_combo.addItem(f"{preset.name} ({preset.engine})", preset.voice_preset_id)
        self._voice_combo.blockSignals(blocked)
        resolved_preset_id = self._set_voice_combo_to_preset(
            str(project_selected_preset_id or selected_preset_id) if (project_selected_preset_id or selected_preset_id) else None
        )
        if resolved_preset_id and resolved_preset_id != project_selected_preset_id:
            self._persist_active_voice_preset_id(resolved_preset_id)
        try:
            self._installed_sapi_voices = list_installed_sapi_voices()
        except Exception:
            self._installed_sapi_voices = []
        self._vieneu_environment = detect_vieneu_installation()

        voice_lines = []
        if self._installed_sapi_voices:
            voice_lines.append("Giọng SAPI đã phát hiện:")
            voice_lines.extend(f"- {name}" for name in self._installed_sapi_voices)
        else:
            voice_lines.append("Giọng SAPI đã phát hiện: không đọc được hoặc hệ thống chưa có giọng")
        version_suffix = (
            f" v{self._vieneu_environment.package_version}" if self._vieneu_environment.package_version else ""
        )
        if self._vieneu_environment.package_installed:
            voice_lines.append(f"VieNeu SDK{version_suffix}: đã cài")
        else:
            voice_lines.append("VieNeu SDK: chưa cài package `vieneu`")
        if self._vieneu_environment.espeak_path:
            voice_lines.append(f"eSpeak NG: {self._vieneu_environment.espeak_path}")
        else:
            voice_lines.append("eSpeak NG: chưa tìm thấy, VieNeu local sẽ chưa chạy được")
        self._voice_info.setText("\n".join(voice_lines))
        self._sync_voice_preset_form()
        self._reload_speaker_bindings()
        self._reload_voice_policies()

    def _reload_export_presets(self) -> None:
        if not self._current_workspace:
            self._export_presets = []
            blocked = self._export_preset_combo.blockSignals(True)
            self._export_preset_combo.clear()
            self._export_preset_combo.blockSignals(blocked)
            self._sync_export_preset_form()
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        project_selected_preset_id = database.get_active_export_preset_id(self._current_workspace.project_id)
        self._export_presets = list_export_presets(self._current_workspace.root_dir)
        selected_preset_id = self._export_preset_combo.currentData()
        blocked = self._export_preset_combo.blockSignals(True)
        self._export_preset_combo.clear()
        for preset in self._export_presets:
            label = preset.name
            if preset.target_width and preset.target_height:
                label += f" ({preset.target_width}x{preset.target_height})"
            self._export_preset_combo.addItem(label, preset.export_preset_id)
        self._export_preset_combo.blockSignals(blocked)
        resolved_preset_id = self._set_export_combo_to_preset(
            str(project_selected_preset_id or selected_preset_id) if (project_selected_preset_id or selected_preset_id) else None
        )
        if resolved_preset_id and resolved_preset_id != project_selected_preset_id:
            self._persist_active_export_preset_id(resolved_preset_id)
        self._sync_export_preset_form()

    def _reload_watermark_profiles(self) -> None:
        if not self._current_workspace:
            self._watermark_profiles = []
            blocked = self._watermark_profile_combo.blockSignals(True)
            self._watermark_profile_combo.clear()
            self._watermark_profile_combo.blockSignals(blocked)
            self._sync_watermark_profile_form()
            return

        database = ProjectDatabase(self._current_workspace.database_path)
        project_selected_profile_id = database.get_active_watermark_profile_id(
            self._current_workspace.project_id
        )
        self._watermark_profiles = list_watermark_profiles(self._current_workspace.root_dir)
        selected_profile_id = self._watermark_profile_combo.currentData()
        blocked = self._watermark_profile_combo.blockSignals(True)
        self._watermark_profile_combo.clear()
        for profile in self._watermark_profiles:
            self._watermark_profile_combo.addItem(profile.name, profile.watermark_profile_id)
        self._watermark_profile_combo.blockSignals(blocked)
        resolved_profile_id = self._set_watermark_combo_to_profile(
            (
                str(project_selected_profile_id or selected_profile_id)
                if (project_selected_profile_id or selected_profile_id)
                else None
            )
        )
        if resolved_profile_id and resolved_profile_id != project_selected_profile_id:
            self._persist_active_watermark_profile_id(resolved_profile_id)
        self._sync_watermark_profile_form()

    def _set_voice_combo_to_preset(self, voice_preset_id: str | None) -> str | None:
        if self._voice_combo.count() == 0:
            return None
        blocked = self._voice_combo.blockSignals(True)
        for index in range(self._voice_combo.count()):
            if voice_preset_id and self._voice_combo.itemData(index) == voice_preset_id:
                self._voice_combo.setCurrentIndex(index)
                self._voice_combo.blockSignals(blocked)
                return str(self._voice_combo.itemData(index))
        self._voice_combo.setCurrentIndex(0)
        resolved = self._voice_combo.currentData()
        self._voice_combo.blockSignals(blocked)
        return str(resolved) if resolved else None

    def _set_export_combo_to_preset(self, export_preset_id: str | None) -> str | None:
        if self._export_preset_combo.count() == 0:
            return None
        blocked = self._export_preset_combo.blockSignals(True)
        for index in range(self._export_preset_combo.count()):
            if export_preset_id and self._export_preset_combo.itemData(index) == export_preset_id:
                self._export_preset_combo.setCurrentIndex(index)
                self._export_preset_combo.blockSignals(blocked)
                return str(self._export_preset_combo.itemData(index))
        self._export_preset_combo.setCurrentIndex(0)
        resolved = self._export_preset_combo.currentData()
        self._export_preset_combo.blockSignals(blocked)
        return str(resolved) if resolved else None

    def _set_watermark_combo_to_profile(self, profile_id: str | None) -> str | None:
        if self._watermark_profile_combo.count() == 0:
            return None
        blocked = self._watermark_profile_combo.blockSignals(True)
        for index in range(self._watermark_profile_combo.count()):
            if profile_id and self._watermark_profile_combo.itemData(index) == profile_id:
                self._watermark_profile_combo.setCurrentIndex(index)
                self._watermark_profile_combo.blockSignals(blocked)
                return str(self._watermark_profile_combo.itemData(index))
        self._watermark_profile_combo.setCurrentIndex(0)
        resolved = self._watermark_profile_combo.currentData()
        self._watermark_profile_combo.blockSignals(blocked)
        return str(resolved) if resolved else None

    def _persist_active_voice_preset_id(self, preset_id: str | None) -> None:
        if not self._current_workspace:
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        normalized = str(preset_id).strip() if preset_id else None
        if database.get_active_voice_preset_id(self._current_workspace.project_id) == normalized:
            return
        database.set_active_voice_preset_id(self._current_workspace.project_id, normalized)
        sync_project_snapshot(self._current_workspace)

    def _persist_active_export_preset_id(self, preset_id: str | None) -> None:
        if not self._current_workspace:
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        normalized = str(preset_id).strip() if preset_id else None
        if database.get_active_export_preset_id(self._current_workspace.project_id) == normalized:
            return
        database.set_active_export_preset_id(self._current_workspace.project_id, normalized)
        sync_project_snapshot(self._current_workspace)

    def _persist_active_watermark_profile_id(self, profile_id: str | None) -> None:
        if not self._current_workspace:
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        normalized = str(profile_id).strip() if profile_id else None
        if database.get_active_watermark_profile_id(self._current_workspace.project_id) == normalized:
            return
        database.set_active_watermark_profile_id(self._current_workspace.project_id, normalized)
        sync_project_snapshot(self._current_workspace)

    def _base_selected_voice_preset(self):
        if not self._voice_presets:
            return None
        preset_id = self._voice_combo.currentData()
        for preset in self._voice_presets:
            if preset.voice_preset_id == preset_id:
                return preset
        return self._voice_presets[0]

    def _selected_voice_preset(self, *, strict: bool = False):
        preset = self._base_selected_voice_preset()
        if not preset:
            return None

        engine = str(self._voice_engine_combo.currentData() or preset.engine or "sapi")
        engine_options = dict(preset.engine_options)
        if engine.lower() == "vieneu":
            engine_options.setdefault("mode", "local")
            ref_audio_path = self._vieneu_ref_audio_input.text().strip()
            ref_text = self._vieneu_ref_text_input.toPlainText().strip()
            if ref_audio_path:
                engine_options["ref_audio_path"] = ref_audio_path
            else:
                engine_options.pop("ref_audio_path", None)
            if ref_text:
                engine_options["ref_text"] = ref_text
            else:
                engine_options.pop("ref_text", None)
        else:
            engine_options.pop("ref_audio_path", None)
            engine_options.pop("ref_text", None)

        name = self._voice_profile_name_input.text().strip() or preset.name
        voice_id = self._voice_id_input.text().strip() or preset.voice_id or "default"
        language = self._voice_language_input.text().strip() or preset.language
        notes = self._voice_notes_input.toPlainText().strip()
        return preset.model_copy(
            update={
                "name": name,
                "engine": engine,
                "voice_id": voice_id,
                "language": language or None,
                "sample_rate": self._parse_voice_int_value(
                    self._voice_sample_rate_input.text(),
                    field_name="Tần số mẫu",
                    minimum=8000,
                    default=preset.sample_rate or 24000,
                    strict=strict,
                ),
                "speed": self._parse_voice_float_value(
                    self._voice_speed_profile_input.text(),
                    field_name="Tốc độ",
                    minimum=0.1,
                    maximum=4.0,
                    default=preset.speed or 1.0,
                    strict=strict,
                ),
                "volume": self._parse_voice_float_value(
                    self._voice_profile_volume_input.text(),
                    field_name="Âm lượng giọng",
                    minimum=0.0,
                    maximum=4.0,
                    default=preset.volume or 1.0,
                    strict=strict,
                ),
                "pitch": self._parse_voice_float_value(
                    self._voice_pitch_input.text(),
                    field_name="Cao độ",
                    minimum=-24.0,
                    maximum=24.0,
                    default=preset.pitch or 0.0,
                    strict=strict,
                ),
                "notes": notes,
                "engine_options": engine_options,
            }
        )

    def _handle_voice_preset_changed(self, index: int) -> None:
        del index
        self._persist_active_voice_preset_id(
            str(self._voice_combo.currentData()) if self._voice_combo.currentData() else None
        )
        self._sync_voice_preset_form()
        if self._current_workspace:
            self._refresh_workspace_views()

    def _handle_voice_profile_form_changed(self, *_args) -> None:
        preset = self._base_selected_voice_preset()
        if preset is None:
            self._voice_profile_status.setText("Chưa có preset giọng để chỉnh sửa")
            return
        engine = str(self._voice_engine_combo.currentData() or preset.engine or "sapi").lower()
        is_vieneu = engine == "vieneu"
        for widget in (
            self._vieneu_ref_audio_input,
            self._vieneu_ref_text_input,
            self._choose_vieneu_ref_audio_button,
        ):
            widget.setEnabled(is_vieneu)
        if engine == "vieneu":
            self._handle_voice_clone_form_changed()
        else:
            self._voice_clone_status.setText("Preset này không dùng chế độ VieNeu clone")
        self._voice_profile_status.setText(
            f"Đang sửa preset {preset.name}. Bấm 'Lưu preset' hoặc 'Lưu thành bản mới' để áp dụng."
        )
        if self._current_workspace:
            self._refresh_workspace_views()

    def _handle_voice_clone_form_changed(self) -> None:
        preset = self._selected_voice_preset(strict=False)
        if not preset or preset.engine.lower() != "vieneu":
            return
        self._update_vieneu_clone_status(mode=str(preset.engine_options.get("mode", "local")))
        if self._current_workspace:
            self._refresh_workspace_views()

    def _update_vieneu_clone_status(self, *, mode: str) -> None:
        ref_audio_path = self._vieneu_ref_audio_input.text().strip()
        ref_text = self._vieneu_ref_text_input.toPlainText().strip()
        resolved_audio_path: Path | None = None
        if ref_audio_path:
            candidate = Path(ref_audio_path).expanduser()
            if not candidate.is_absolute() and self._current_workspace:
                candidate = self._current_workspace.root_dir / candidate
            resolved_audio_path = candidate.resolve()
        if ref_audio_path and resolved_audio_path and not resolved_audio_path.exists():
            self._voice_clone_status.setText(f"Không tìm thấy audio mẫu clone: {resolved_audio_path} ({mode})")
        elif ref_audio_path and ref_text:
            transcript_size = len(ref_text.split())
            self._voice_clone_status.setText(
                f"Clone sẵn sàng ({mode}) - mẫu {transcript_size} từ"
            )
        elif ref_audio_path:
            self._voice_clone_status.setText(f"Đã có audio mẫu, cần thêm văn bản mẫu đúng 100% ({mode})")
        else:
            self._voice_clone_status.setText(
                f"Clone chưa được cấu hình ({mode}). Gợi ý: dùng audio sạch 10-30 giây và transcript khớp tuyệt đối."
            )

    def _sync_voice_preset_form(self) -> None:
        preset = self._base_selected_voice_preset()
        if not preset:
            self._voice_preset_notes.setText("Chưa có preset giọng")
            self._voice_profile_status.setText("Trình quản lý preset giọng chưa có preset")
            self._voice_clone_status.setText("Preset clone chưa được cấu hình")
            for widget in (
                self._voice_profile_name_input,
                self._voice_engine_combo,
                self._voice_id_input,
                self._voice_language_input,
                self._voice_sample_rate_input,
                self._voice_speed_profile_input,
                self._voice_profile_volume_input,
                self._voice_pitch_input,
                self._voice_notes_input,
                self._save_voice_preset_button,
                self._save_voice_preset_as_new_button,
                self._delete_voice_preset_button,
                self._batch_import_voice_profiles_button,
            ):
                widget.setEnabled(False)
            self._vieneu_ref_audio_input.clear()
            self._vieneu_ref_text_input.clear()
            for widget in (
                self._vieneu_ref_audio_input,
                self._vieneu_ref_text_input,
                self._choose_vieneu_ref_audio_button,
            ):
                widget.setEnabled(False)
            return

        self._voice_preset_notes.setText(preset.notes or "Không có ghi chú cho preset này")
        is_vieneu = preset.engine.lower() == "vieneu"
        engine_options = dict(preset.engine_options)
        ref_audio_path = str(engine_options.get("ref_audio_path", "")) if is_vieneu else ""
        ref_text = str(engine_options.get("ref_text", "")) if is_vieneu else ""
        mode = engine_options.get("mode", "local") if is_vieneu else "-"

        widgets = (
            self._voice_profile_name_input,
            self._voice_engine_combo,
            self._voice_id_input,
            self._voice_language_input,
            self._voice_sample_rate_input,
            self._voice_speed_profile_input,
            self._voice_profile_volume_input,
            self._voice_pitch_input,
            self._voice_notes_input,
            self._vieneu_ref_audio_input,
            self._vieneu_ref_text_input,
        )
        blocked_states = [widget.blockSignals(True) for widget in widgets]
        self._voice_profile_name_input.setText(preset.name)
        self._set_combo_value(self._voice_engine_combo, preset.engine.lower())
        self._voice_id_input.setText(preset.voice_id or "default")
        self._voice_language_input.setText(preset.language or "")
        self._voice_sample_rate_input.setText(str(preset.sample_rate))
        self._voice_speed_profile_input.setText(str(preset.speed))
        self._voice_profile_volume_input.setText(str(preset.volume))
        self._voice_pitch_input.setText(str(preset.pitch))
        self._voice_notes_input.setPlainText(preset.notes or "")

        self._vieneu_ref_audio_input.setText(ref_audio_path)
        self._vieneu_ref_text_input.setPlainText(ref_text)
        for widget, blocked in zip(widgets, blocked_states, strict=True):
            widget.blockSignals(blocked)

        for widget in (
            self._voice_profile_name_input,
            self._voice_engine_combo,
            self._voice_id_input,
            self._voice_language_input,
            self._voice_sample_rate_input,
            self._voice_speed_profile_input,
            self._voice_profile_volume_input,
            self._voice_pitch_input,
            self._voice_notes_input,
            self._vieneu_ref_audio_input,
            self._vieneu_ref_text_input,
            self._choose_vieneu_ref_audio_button,
            self._save_voice_preset_button,
            self._save_voice_preset_as_new_button,
            self._delete_voice_preset_button,
            self._batch_import_voice_profiles_button,
        ):
            widget.setEnabled(True)

        for widget in (
            self._vieneu_ref_audio_input,
            self._vieneu_ref_text_input,
            self._choose_vieneu_ref_audio_button,
        ):
            widget.setEnabled(is_vieneu)

        if not is_vieneu:
            self._voice_clone_status.setText("Preset này không dùng chế độ VieNeu clone")
        else:
            self._update_vieneu_clone_status(mode=str(mode))
        self._voice_profile_status.setText(
            "Trình quản lý preset giọng đã sẵn sàng. Bạn có thể sửa, nhân bản, xóa hoặc nhập hàng loạt."
        )

    def _character_name_map(self, database: ProjectDatabase) -> dict[str, str]:
        if not self._current_workspace:
            return {}
        mapping: dict[str, str] = {}
        for row in database.list_character_profiles(self._current_workspace.project_id):
            character_id = str(row["character_id"] or "").strip()
            if not character_id:
                continue
            display_name = str(row["canonical_name_vi"] or row["canonical_name_zh"] or "").strip()
            if display_name:
                mapping[character_id] = display_name
        return mapping

    @staticmethod
    def _speaker_binding_status_color(status_kind: str) -> QColor:
        if status_kind == "ok":
            return QColor(226, 239, 218)
        if status_kind == "missing":
            return QColor(255, 229, 229)
        if status_kind == "unbound":
            return QColor(255, 244, 214)
        return QColor()

    def _set_speaker_binding_row_status(self, row_index: int, *, status_text: str, status_kind: str) -> None:
        status_item = self._speaker_binding_table.item(row_index, 3)
        if status_item is None:
            status_item = QTableWidgetItem()
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._speaker_binding_table.setItem(row_index, 3, status_item)
        status_item.setText(status_text)
        status_item.setData(Qt.ItemDataRole.UserRole, status_kind)
        status_item.setBackground(self._speaker_binding_status_color(status_kind))

    def _update_speaker_binding_row_status(self, row_index: int, available_preset_ids: set[str] | None = None) -> None:
        combo = self._speaker_binding_table.cellWidget(row_index, 2)
        if not isinstance(combo, QComboBox):
            return
        preset_id = str(combo.currentData() or "").strip()
        preset_ids = available_preset_ids or {preset.voice_preset_id for preset in self._voice_presets}
        if not preset_id:
            self._set_speaker_binding_row_status(row_index, status_text="Chưa gán", status_kind="unbound")
            return
        if preset_id and preset_id not in preset_ids:
            self._set_speaker_binding_row_status(
                row_index,
                status_text="Preset đã gán không còn tồn tại",
                status_kind="missing",
            )
            return
        self._set_speaker_binding_row_status(
            row_index,
            status_text="Đã gán preset riêng",
            status_kind="ok",
        )

    def _refresh_speaker_binding_status_summary(self) -> None:
        row_count = self._speaker_binding_table.rowCount()
        if not self._current_workspace:
            self._speaker_binding_status.setText("Speaker binding: chưa mở dự án")
            return
        if row_count == 0:
            self._speaker_binding_status.setText("Speaker binding: chưa có speaker từ Contextual V2")
            return

        bound_count = 0
        unresolved_count = 0
        missing_count = 0
        for row_index in range(row_count):
            status_item = self._speaker_binding_table.item(row_index, 3)
            status_kind = str(status_item.data(Qt.ItemDataRole.UserRole) or "") if status_item else ""
            if status_kind == "ok":
                bound_count += 1
            elif status_kind == "missing":
                missing_count += 1
            else:
                unresolved_count += 1

        if bound_count == 0:
            self._speaker_binding_status.setText(
                f"Speaker binding: có {row_count} speaker nhận diện. Nếu chưa lưu binding nào, toàn bộ track sẽ dùng preset mặc định."
            )
            return

        details: list[str] = [f"{bound_count}/{row_count} speaker đã có preset riêng."]
        if missing_count:
            details.append(f"{missing_count} binding đang trỏ tới preset không còn tồn tại.")
        if unresolved_count:
            details.append("TTS sẽ bị chặn cho đến khi gán đủ các speaker đã nhận diện.")
        else:
            details.append("Đã đủ binding cho các speaker đã nhận diện.")
        self._speaker_binding_status.setText("Speaker binding: " + " ".join(details))

    def _set_speaker_binding_form_dirty(self, is_dirty: bool) -> None:
        self._speaker_binding_dirty = is_dirty
        marker = " Form hiện có thay đổi chưa lưu."
        status_text = self._speaker_binding_status.text().strip()
        if status_text:
            if is_dirty and marker not in status_text:
                self._speaker_binding_status.setText(status_text + marker)
            elif not is_dirty and marker in status_text:
                self._speaker_binding_status.setText(status_text.replace(marker, ""))
        if is_dirty:
            self._speaker_binding_hint.setText(
                "Lưu ý: bạn đang có thay đổi speaker binding trên form nhưng chưa lưu. "
                "TTS/export chỉ dùng mapping đã lưu trong dự án."
            )
        else:
            self._speaker_binding_hint.setText(
                "Mẹo: nếu đã lưu ít nhất 1 speaker binding, mọi speaker nhận diện rõ trong track hiện tại "
                "phải được gán preset. Speaker unknown vẫn dùng preset mặc định."
            )
        self._sync_voice_summary_with_binding_form_state()

    def _sync_voice_summary_with_binding_form_state(self) -> None:
        summary_text = self._voice_summary.text().strip()
        if not summary_text:
            return
        lines = [
            line
            for line in summary_text.splitlines()
            if not line.startswith("- Speaker binding trên form:")
            and not line.startswith("- Voice policy trên form:")
        ]
        if self._speaker_binding_dirty and self._speaker_binding_table.rowCount() > 0:
            lines.append("- Speaker binding trên form: có thay đổi chưa lưu; hãy bấm Lưu binding để áp dụng")
        if self._voice_policy_dirty and (
            self._character_voice_policy_table.rowCount() > 0
            or self._relationship_voice_policy_table.rowCount() > 0
            or self._register_voice_style_table.rowCount() > 0
        ):
            lines.append("- Voice policy/register style trên form: có thay đổi chưa lưu; hãy bấm Lưu voice policy để áp dụng")
        self._voice_summary.setText("\n".join(lines))

    def _handle_speaker_binding_selection_changed(self, row_index: int) -> None:
        if self._speaker_binding_loading:
            return
        self._update_speaker_binding_row_status(row_index)
        self._refresh_speaker_binding_status_summary()
        self._set_speaker_binding_form_dirty(True)

    def _fill_unbound_speakers_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Speaker binding", "Hãy chọn preset giọng mặc định trước.")
            return
        changed = 0
        for row_index in range(self._speaker_binding_table.rowCount()):
            combo = self._speaker_binding_table.cellWidget(row_index, 2)
            if not isinstance(combo, QComboBox):
                continue
            if str(combo.currentData() or "").strip():
                continue
            for combo_index in range(combo.count()):
                if str(combo.itemData(combo_index) or "").strip() == preset_id:
                    combo.setCurrentIndex(combo_index)
                    changed += 1
                    break
        self._refresh_speaker_binding_status_summary()
        if changed:
            self._set_speaker_binding_form_dirty(True)
            self._append_log_line(f"Đã gán preset hiện tại cho {changed} speaker chưa có binding")
        else:
            QMessageBox.information(self, "Speaker binding", "Không có speaker trống nào để gán nhanh.")

    def _fill_selected_speaker_bindings_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Speaker binding", "Hãy chọn preset giọng mặc định trước.")
            return
        selected_rows = self._selected_table_row_indexes(self._speaker_binding_table)
        if not selected_rows:
            QMessageBox.warning(self, "Speaker binding", "Hãy chọn ít nhất một dòng speaker trước.")
            return
        changed = 0
        for row_index in selected_rows:
            combo = self._speaker_binding_table.cellWidget(row_index, 2)
            if not isinstance(combo, QComboBox):
                continue
            for combo_index in range(combo.count()):
                if str(combo.itemData(combo_index) or "").strip() == preset_id:
                    if combo.currentIndex() != combo_index:
                        combo.setCurrentIndex(combo_index)
                        changed += 1
                    break
        self._refresh_speaker_binding_status_summary()
        if changed:
            self._set_speaker_binding_form_dirty(True)
            self._append_log_line(f"Đã gán preset hiện tại cho {changed}/{len(selected_rows)} speaker được chọn")
        else:
            QMessageBox.information(self, "Speaker binding", "Các speaker đã chọn đã dùng preset này hoặc không đổi được.")

    def _clear_selected_speaker_bindings(self) -> None:
        selected_rows = self._selected_table_row_indexes(self._speaker_binding_table)
        if not selected_rows:
            QMessageBox.warning(self, "Speaker binding", "Hãy chọn ít nhất một dòng speaker trước.")
            return
        changed = 0
        for row_index in selected_rows:
            combo = self._speaker_binding_table.cellWidget(row_index, 2)
            if isinstance(combo, QComboBox) and combo.currentIndex() != 0:
                combo.setCurrentIndex(0)
                changed += 1
        self._refresh_speaker_binding_status_summary()
        if changed:
            self._set_speaker_binding_form_dirty(True)
            self._append_log_line(f"Đã xóa preset trên {changed}/{len(selected_rows)} speaker được chọn")
        else:
            QMessageBox.information(self, "Speaker binding", "Các speaker đã chọn hiện đang để trống.")

    def _clear_speaker_binding_form(self) -> None:
        changed = False
        for row_index in range(self._speaker_binding_table.rowCount()):
            combo = self._speaker_binding_table.cellWidget(row_index, 2)
            if isinstance(combo, QComboBox):
                if combo.currentIndex() != 0:
                    changed = True
                combo.setCurrentIndex(0)
        self._refresh_speaker_binding_status_summary()
        if changed:
            self._set_speaker_binding_form_dirty(True)

    def _reload_speaker_bindings(self) -> None:
        self._speaker_binding_loading = True
        self._speaker_binding_table.setRowCount(0)
        if not self._current_workspace:
            self._speaker_binding_status.setText("Speaker binding: chưa mở dự án")
            self._speaker_binding_loading = False
            self._set_speaker_binding_form_dirty(False)
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        if not analysis_rows:
            self._speaker_binding_status.setText("Speaker binding: chưa có speaker từ Contextual V2")
            self._speaker_binding_loading = False
            self._set_speaker_binding_form_dirty(False)
            return

        character_name_map = self._character_name_map(database)
        candidates = discover_speaker_candidates(analysis_rows, character_name_map=character_name_map)
        binding_rows = database.list_speaker_bindings(self._current_workspace.project_id)
        binding_map = {
            (
                str(row["speaker_type"] or "character").strip() or "character",
                str(row["speaker_key"] or "").strip(),
            ): str(row["voice_preset_id"] or "").strip()
            for row in binding_rows
            if str(row["speaker_key"] or "").strip()
        }
        available_preset_ids = {preset.voice_preset_id for preset in self._voice_presets}

        for row_index, candidate in enumerate(candidates):
            self._speaker_binding_table.insertRow(row_index)
            speaker_item = QTableWidgetItem(candidate.label)
            speaker_item.setData(
                Qt.ItemDataRole.UserRole,
                {"speaker_type": candidate.speaker_type, "speaker_key": candidate.speaker_key},
            )
            speaker_item.setFlags(speaker_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._speaker_binding_table.setItem(row_index, 0, speaker_item)

            count_item = QTableWidgetItem(str(candidate.segment_count))
            count_item.setFlags(count_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._speaker_binding_table.setItem(row_index, 1, count_item)

            combo = QComboBox()
            combo.addItem("Chưa gán", "")
            for preset in self._voice_presets:
                combo.addItem(f"{preset.name} ({preset.engine})", preset.voice_preset_id)
            selected_preset_id = binding_map.get((candidate.speaker_type, candidate.speaker_key), "")
            if selected_preset_id:
                for combo_index in range(combo.count()):
                    if combo.itemData(combo_index) == selected_preset_id:
                        combo.setCurrentIndex(combo_index)
                        break
            self._speaker_binding_table.setCellWidget(row_index, 2, combo)
            combo.currentIndexChanged.connect(
                lambda _index, row_index=row_index: self._handle_speaker_binding_selection_changed(row_index)
            )
            self._update_speaker_binding_row_status(row_index, available_preset_ids=available_preset_ids)

        if not candidates:
            self._speaker_binding_status.setText("Speaker binding: chưa có speaker nhận diện đủ rõ để gán")
            self._speaker_binding_loading = False
            self._set_speaker_binding_form_dirty(False)
            return
        self._speaker_binding_loading = False
        self._refresh_speaker_binding_status_summary()
        self._set_speaker_binding_form_dirty(False)

    def _save_speaker_bindings(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Speaker binding", "Hãy tạo hoặc mở dự án trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        now = utc_now_iso()
        bindings: list[SpeakerBindingRecord] = []
        for row_index in range(self._speaker_binding_table.rowCount()):
            speaker_item = self._speaker_binding_table.item(row_index, 0)
            combo = self._speaker_binding_table.cellWidget(row_index, 2)
            if speaker_item is None or not isinstance(combo, QComboBox):
                continue
            payload = speaker_item.data(Qt.ItemDataRole.UserRole) or {}
            speaker_type = str(payload.get("speaker_type", "character") or "character").strip() or "character"
            speaker_key = str(payload.get("speaker_key", "") or "").strip()
            voice_preset_id = str(combo.currentData() or "").strip()
            if not speaker_key or not voice_preset_id:
                continue
            bindings.append(
                SpeakerBindingRecord(
                    binding_id=f"bind:{speaker_type}:{speaker_key}",
                    project_id=self._current_workspace.project_id,
                    speaker_type=speaker_type,
                    speaker_key=speaker_key,
                    voice_preset_id=voice_preset_id,
                    created_at=now,
                    updated_at=now,
                )
            )
        database.replace_speaker_bindings(self._current_workspace.project_id, bindings)
        self._set_speaker_binding_form_dirty(False)
        self._invalidate_subtitle_pipeline_outputs(clear_tts_audio=True)
        self._reload_speaker_bindings()
        self._refresh_workspace_views()
        self._append_log_line(f"Đã lưu {len(bindings)} speaker binding")
        QMessageBox.information(
            self,
            "Speaker binding",
            (
                f"Đã lưu {len(bindings)} speaker binding.\n"
                "- Nếu đã thay đổi mapping giọng, hãy chạy lại TTS rồi tạo lại track giọng."
            ),
        )

    def _set_voice_policy_row_status(
        self,
        table: QTableWidget,
        row_index: int,
        *,
        status_text: str,
        status_kind: str,
    ) -> None:
        status_item = table.item(row_index, VOICE_POLICY_STATUS_COLUMN)
        if status_item is None:
            status_item = QTableWidgetItem()
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            table.setItem(row_index, VOICE_POLICY_STATUS_COLUMN, status_item)
        status_item.setText(status_text)
        status_item.setData(Qt.ItemDataRole.UserRole, status_kind)
        status_item.setBackground(self._speaker_binding_status_color(status_kind))

    def _voice_policy_preset_combo(self, table: QTableWidget, row_index: int) -> QComboBox | None:
        combo = table.cellWidget(row_index, VOICE_POLICY_PRESET_COLUMN)
        return combo if isinstance(combo, QComboBox) else None

    def _voice_policy_override_input(
        self,
        table: QTableWidget,
        row_index: int,
        column: int,
    ) -> QLineEdit | None:
        widget = table.cellWidget(row_index, column)
        return widget if isinstance(widget, QLineEdit) else None

    @staticmethod
    def _coerce_optional_float(value: object | None) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_voice_policy_override_value(value: float | None) -> str:
        if value is None:
            return ""
        return f"{value:.2f}".rstrip("0").rstrip(".")

    @staticmethod
    def _parse_optional_voice_policy_float_value(
        raw_value: str,
        *,
        field_name: str,
        minimum: float,
        maximum: float,
    ) -> float | None:
        if not raw_value.strip():
            return None
        return MainWindow._parse_voice_float_value(
            raw_value,
            field_name=field_name,
            minimum=minimum,
            maximum=maximum,
            default=minimum,
            strict=True,
        )

    def _create_voice_policy_override_input(
        self,
        table: QTableWidget,
        row_index: int,
        *,
        value: float | None,
        placeholder: str,
    ) -> QLineEdit:
        input_widget = QLineEdit(self._format_voice_policy_override_value(value))
        input_widget.setPlaceholderText(placeholder)
        input_widget.textChanged.connect(
            lambda _text, table=table, row_index=row_index: self._handle_voice_policy_selection_changed(
                table,
                row_index,
            )
        )
        return input_widget

    def _voice_policy_override_payload(
        self,
        table: QTableWidget,
        row_index: int,
        *,
        strict: bool,
    ) -> dict[str, float]:
        payload: dict[str, float] = {}
        speed_input = self._voice_policy_override_input(table, row_index, VOICE_POLICY_SPEED_COLUMN)
        volume_input = self._voice_policy_override_input(table, row_index, VOICE_POLICY_VOLUME_COLUMN)
        pitch_input = self._voice_policy_override_input(table, row_index, VOICE_POLICY_PITCH_COLUMN)
        if speed_input is not None:
            speed_value = (
                self._parse_optional_voice_policy_float_value(
                    speed_input.text(),
                    field_name="Tốc độ policy",
                    minimum=0.1,
                    maximum=4.0,
                )
                if strict
                else self._coerce_optional_float(speed_input.text())
            )
            if speed_value is not None:
                payload["speed"] = speed_value
        if volume_input is not None:
            volume_value = (
                self._parse_optional_voice_policy_float_value(
                    volume_input.text(),
                    field_name="Âm lượng policy",
                    minimum=0.0,
                    maximum=4.0,
                )
                if strict
                else self._coerce_optional_float(volume_input.text())
            )
            if volume_value is not None:
                payload["volume"] = volume_value
        if pitch_input is not None:
            pitch_value = (
                self._parse_optional_voice_policy_float_value(
                    pitch_input.text(),
                    field_name="Cao độ policy",
                    minimum=-24.0,
                    maximum=24.0,
                )
                if strict
                else self._coerce_optional_float(pitch_input.text())
            )
            if pitch_value is not None:
                payload["pitch"] = pitch_value
        return payload

    def _current_voice_policy_style_payload(self, *, strict: bool) -> dict[str, float]:
        preset = self._selected_voice_preset(strict=strict)
        if preset is None:
            return {}
        return {
            "speed": float(preset.speed),
            "volume": float(preset.volume),
            "pitch": float(preset.pitch),
        }

    def _register_voice_style_override_input(
        self,
        row_index: int,
        column: int,
    ) -> QLineEdit | None:
        widget = self._register_voice_style_table.cellWidget(row_index, column)
        return widget if isinstance(widget, QLineEdit) else None

    def _register_voice_style_override_payload(
        self,
        row_index: int,
        *,
        strict: bool,
    ) -> dict[str, float]:
        payload: dict[str, float] = {}
        field_columns = {
            "speed": (REGISTER_STYLE_SPEED_COLUMN, "Tốc độ register style", 0.1, 4.0),
            "volume": (REGISTER_STYLE_VOLUME_COLUMN, "Âm lượng register style", 0.0, 4.0),
            "pitch": (REGISTER_STYLE_PITCH_COLUMN, "Cao độ register style", -24.0, 24.0),
        }
        for field_name, (column, label, minimum, maximum) in field_columns.items():
            input_widget = self._register_voice_style_override_input(row_index, column)
            if input_widget is None:
                continue
            value = (
                self._parse_optional_voice_policy_float_value(
                    input_widget.text(),
                    field_name=label,
                    minimum=minimum,
                    maximum=maximum,
                )
                if strict
                else self._coerce_optional_float(input_widget.text())
            )
            if value is not None:
                payload[field_name] = value
        return payload

    def _set_register_voice_style_row_status(
        self,
        row_index: int,
        *,
        status_text: str,
        status_kind: str,
    ) -> None:
        status_item = self._register_voice_style_table.item(row_index, REGISTER_STYLE_STATUS_COLUMN)
        if status_item is None:
            status_item = QTableWidgetItem()
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._register_voice_style_table.setItem(row_index, REGISTER_STYLE_STATUS_COLUMN, status_item)
        status_item.setText(status_text)
        status_item.setData(Qt.ItemDataRole.UserRole, status_kind)
        status_item.setBackground(self._speaker_binding_status_color(status_kind))

    def _update_register_voice_style_row_status(self, row_index: int) -> None:
        style_overrides = self._register_voice_style_override_payload(row_index, strict=False)
        if not style_overrides:
            self._set_register_voice_style_row_status(
                row_index,
                status_text="Chưa cấu hình style",
                status_kind="unbound",
            )
            return
        self._set_register_voice_style_row_status(
            row_index,
            status_text="Đã cấu hình register style",
            status_kind="ok",
        )

    def _apply_register_voice_style_to_row(self, row_index: int, style_payload: dict[str, float]) -> bool:
        changed = False
        field_columns = {
            "speed": REGISTER_STYLE_SPEED_COLUMN,
            "volume": REGISTER_STYLE_VOLUME_COLUMN,
            "pitch": REGISTER_STYLE_PITCH_COLUMN,
        }
        for field_name, column in field_columns.items():
            input_widget = self._register_voice_style_override_input(row_index, column)
            if input_widget is None or field_name not in style_payload:
                continue
            formatted_value = self._format_voice_policy_override_value(style_payload[field_name])
            if input_widget.text().strip() != formatted_value:
                input_widget.setText(formatted_value)
                changed = True
        return changed

    def _clear_register_voice_style_row_style(self, row_index: int) -> bool:
        changed = False
        for column in (
            REGISTER_STYLE_SPEED_COLUMN,
            REGISTER_STYLE_VOLUME_COLUMN,
            REGISTER_STYLE_PITCH_COLUMN,
        ):
            input_widget = self._register_voice_style_override_input(row_index, column)
            if input_widget is not None and input_widget.text().strip():
                input_widget.clear()
                changed = True
        return changed

    def _refresh_register_voice_style_status_summary(self) -> None:
        if not self._current_workspace:
            self._register_voice_style_status.setText("Register style: chưa mở dự án")
            return
        total_rows = self._register_voice_style_table.rowCount()
        if total_rows == 0:
            self._register_voice_style_status.setText(
                "Register style: chưa có tín hiệu register/tone/turn-function để cấu hình"
            )
            return
        configured_count = 0
        for row_index in range(total_rows):
            status_item = self._register_voice_style_table.item(row_index, REGISTER_STYLE_STATUS_COLUMN)
            status_kind = str(status_item.data(Qt.ItemDataRole.UserRole) or "") if status_item else ""
            if status_kind == "ok":
                configured_count += 1
        if configured_count == 0:
            self._register_voice_style_status.setText(
                f"Register style: 0/{total_rows} hàng đang cấu hình; thiếu policy thì runtime giữ nguyên style hiện tại"
            )
            return
        self._register_voice_style_status.setText(
            f"Register style: {configured_count}/{total_rows} hàng đã cấu hình"
        )

    def _apply_voice_policy_style_to_row(
        self,
        table: QTableWidget,
        row_index: int,
        style_payload: dict[str, float],
    ) -> bool:
        changed = False
        field_columns = {
            "speed": VOICE_POLICY_SPEED_COLUMN,
            "volume": VOICE_POLICY_VOLUME_COLUMN,
            "pitch": VOICE_POLICY_PITCH_COLUMN,
        }
        for field_name, column in field_columns.items():
            input_widget = self._voice_policy_override_input(table, row_index, column)
            if input_widget is None or field_name not in style_payload:
                continue
            formatted_value = self._format_voice_policy_override_value(style_payload[field_name])
            if input_widget.text().strip() != formatted_value:
                input_widget.setText(formatted_value)
                changed = True
        return changed

    def _clear_voice_policy_row_style(self, table: QTableWidget, row_index: int) -> bool:
        changed = False
        for column in (VOICE_POLICY_SPEED_COLUMN, VOICE_POLICY_VOLUME_COLUMN, VOICE_POLICY_PITCH_COLUMN):
            input_widget = self._voice_policy_override_input(table, row_index, column)
            if input_widget is not None and input_widget.text().strip():
                input_widget.clear()
                changed = True
        return changed

    def _clear_voice_policy_row(self, table: QTableWidget, row_index: int) -> bool:
        changed = False
        combo = self._voice_policy_preset_combo(table, row_index)
        if combo is not None and combo.currentIndex() != 0:
            combo.setCurrentIndex(0)
            changed = True
        for column in (VOICE_POLICY_SPEED_COLUMN, VOICE_POLICY_VOLUME_COLUMN, VOICE_POLICY_PITCH_COLUMN):
            input_widget = self._voice_policy_override_input(table, row_index, column)
            if input_widget is not None and input_widget.text().strip():
                input_widget.clear()
                changed = True
        return changed

    def _update_voice_policy_row_status(
        self,
        table: QTableWidget,
        row_index: int,
        *,
        available_preset_ids: set[str] | None = None,
    ) -> None:
        combo = self._voice_policy_preset_combo(table, row_index)
        if combo is None:
            return
        preset_id = str(combo.currentData() or "").strip()
        style_overrides = self._voice_policy_override_payload(table, row_index, strict=False)
        preset_ids = available_preset_ids or {preset.voice_preset_id for preset in self._voice_presets}
        if not preset_id and not style_overrides:
            self._set_voice_policy_row_status(table, row_index, status_text="Chưa gán", status_kind="unbound")
            return
        if preset_id and preset_id not in preset_ids:
            self._set_voice_policy_row_status(
                table,
                row_index,
                status_text="Preset đã gán không còn tồn tại",
                status_kind="missing",
            )
            return
        if table is self._relationship_voice_policy_table:
            if preset_id and style_overrides:
                ok_text = "Đã override preset + style"
            elif preset_id:
                ok_text = "Đã override theo quan hệ"
            else:
                ok_text = "Đã override style theo quan hệ"
        else:
            if preset_id and style_overrides:
                ok_text = "Đã cấu hình fallback + style"
            elif preset_id:
                ok_text = "Đã cấu hình fallback"
            else:
                ok_text = "Đã cấu hình style"
        self._set_voice_policy_row_status(table, row_index, status_text=ok_text, status_kind="ok")

    def _refresh_voice_policy_status_summary(self) -> None:
        if not self._current_workspace:
            self._voice_policy_status.setText("Voice policy: chưa mở dự án")
            return
        total_rows = self._character_voice_policy_table.rowCount() + self._relationship_voice_policy_table.rowCount()
        if total_rows == 0:
            self._voice_policy_status.setText("Voice policy: chưa có nhân vật hoặc quan hệ từ Contextual V2")
            return

        bound_count = 0
        missing_count = 0
        style_only_count = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                status_item = table.item(row_index, VOICE_POLICY_STATUS_COLUMN)
                status_kind = str(status_item.data(Qt.ItemDataRole.UserRole) or "") if status_item else ""
                if status_kind == "ok":
                    bound_count += 1
                    combo = self._voice_policy_preset_combo(table, row_index)
                    if combo is not None and not str(combo.currentData() or "").strip():
                        style_only_count += 1
                elif status_kind == "missing":
                    missing_count += 1

        if bound_count == 0:
            self._voice_policy_status.setText(
                "Voice policy: "
                + (
                    f"có {total_rows} hàng policy khả dụng. Nếu chưa lưu policy nào, "
                    "runtime sẽ dùng speaker binding rồi mới rơi về preset mặc định."
                )
            )
            return

        preset_bound_count = bound_count - style_only_count
        details = [f"{bound_count}/{total_rows} hàng voice policy đã được cấu hình."]
        if preset_bound_count:
            details.append(f"{preset_bound_count} hàng có preset override/fallback.")
        if style_only_count:
            details.append(f"{style_only_count} hàng chỉ dùng style override.")
        if missing_count:
            details.append(f"{missing_count} policy đang trỏ tới preset không còn tồn tại.")
        else:
            details.append("Các policy đã cấu hình hiện đều hợp lệ.")
        self._voice_policy_status.setText("Voice policy: " + " ".join(details))

    def _set_voice_policy_form_dirty(self, is_dirty: bool) -> None:
        self._voice_policy_dirty = is_dirty
        marker = " Form hiện có thay đổi chưa lưu."
        status_text = self._voice_policy_status.text().strip()
        if status_text:
            if is_dirty and marker not in status_text:
                self._voice_policy_status.setText(status_text + marker)
            elif not is_dirty and marker in status_text:
                self._voice_policy_status.setText(status_text.replace(marker, ""))
        if is_dirty:
            self._voice_policy_hint.setText(
                "Lưu ý: voice policy/register style trên form đang có thay đổi chưa lưu. Runtime chỉ dùng policy đã lưu trong dự án."
            )
        else:
            self._voice_policy_hint.setText(
                "Mẹo: voice policy là fallback mềm. Quan hệ speaker->listener ưu tiên hơn policy theo nhân vật; register style chỉ tinh chỉnh speed/volume/pitch và speaker binding vẫn là mức ưu tiên cao nhất."
            )
        self._sync_voice_summary_with_binding_form_state()

    def _handle_voice_policy_selection_changed(self, table: QTableWidget, row_index: int) -> None:
        if self._voice_policy_loading:
            return
        if table is self._register_voice_style_table:
            self._handle_register_voice_style_selection_changed(row_index)
            return
        self._update_voice_policy_row_status(table, row_index)
        self._refresh_voice_policy_status_summary()
        self._set_voice_policy_form_dirty(True)

    def _fill_unbound_voice_policies_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn preset giọng mặc định trước.")
            return
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                combo = self._voice_policy_preset_combo(table, row_index)
                if combo is None:
                    continue
                if str(combo.currentData() or "").strip():
                    continue
                for combo_index in range(combo.count()):
                    if str(combo.itemData(combo_index) or "").strip() == preset_id:
                        combo.setCurrentIndex(combo_index)
                        changed += 1
                        break
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã gán preset hiện tại cho {changed} hàng voice policy còn trống")
        else:
            QMessageBox.information(self, "Voice policy", "Không có hàng policy trống nào để gán nhanh.")

    def _fill_unstyled_voice_policies_with_current_style(self) -> None:
        try:
            style_payload = self._current_voice_policy_style_payload(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Voice policy", str(exc))
            return
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                if self._voice_policy_override_payload(table, row_index, strict=False):
                    continue
                if self._apply_voice_policy_style_to_row(table, row_index, style_payload):
                    changed += 1
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã điền style hiện tại cho {changed} hàng voice policy chưa có style")
        else:
            QMessageBox.information(self, "Voice policy", "Không có hàng policy nào đang trống style.")

    def _selected_table_row_indexes(self, table: QTableWidget) -> list[int]:
        selection_model = table.selectionModel()
        if selection_model is None:
            return [table.currentRow()] if table.currentRow() >= 0 else []
        selected_rows = sorted({index.row() for index in selection_model.selectedIndexes()})
        if selected_rows:
            return selected_rows
        return [table.currentRow()] if table.currentRow() >= 0 else []

    def _fill_selected_voice_policy_rows_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn preset giọng mặc định trước.")
            return
        selected_count = 0
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in self._selected_table_row_indexes(table):
                selected_count += 1
                combo = self._voice_policy_preset_combo(table, row_index)
                if combo is None:
                    continue
                for combo_index in range(combo.count()):
                    if str(combo.itemData(combo_index) or "").strip() == preset_id:
                        if combo.currentIndex() != combo_index:
                            combo.setCurrentIndex(combo_index)
                            changed += 1
                        break
        if selected_count == 0:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn ít nhất một dòng policy trước.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã gán preset hiện tại cho {changed}/{selected_count} dòng voice policy được chọn")
        else:
            QMessageBox.information(self, "Voice policy", "Các dòng đã chọn đã dùng preset này hoặc không đổi được.")

    def _fill_selected_voice_policy_rows_with_current_style(self) -> None:
        try:
            style_payload = self._current_voice_policy_style_payload(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Voice policy", str(exc))
            return
        selected_count = 0
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in self._selected_table_row_indexes(table):
                selected_count += 1
                if self._apply_voice_policy_style_to_row(table, row_index, style_payload):
                    changed += 1
        if selected_count == 0:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn ít nhất một dòng policy trước.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã điền style hiện tại cho {changed}/{selected_count} dòng voice policy được chọn")
        else:
            QMessageBox.information(self, "Voice policy", "Các dòng đã chọn đã có cùng style hoặc không đổi được.")

    def _clear_selected_voice_policy_rows(self) -> None:
        selected_count = 0
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in self._selected_table_row_indexes(table):
                selected_count += 1
                if self._clear_voice_policy_row(table, row_index):
                    changed += 1
        if selected_count == 0:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn ít nhất một dòng policy trước.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã xóa preset trên {changed}/{selected_count} dòng voice policy được chọn")
        else:
            QMessageBox.information(self, "Voice policy", "Các dòng đã chọn hiện đang để trống.")

    def _clear_selected_voice_policy_row_styles(self) -> None:
        selected_count = 0
        changed = 0
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in self._selected_table_row_indexes(table):
                selected_count += 1
                if self._clear_voice_policy_row_style(table, row_index):
                    changed += 1
        if selected_count == 0:
            QMessageBox.warning(self, "Voice policy", "Hãy chọn ít nhất một dòng policy trước.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã xóa style trên {changed}/{selected_count} dòng voice policy được chọn")
        else:
            QMessageBox.information(self, "Voice policy", "Các dòng đã chọn hiện chưa có style override.")

    def _clear_voice_policy_form(self) -> None:
        changed = False
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                if self._clear_voice_policy_row(table, row_index):
                    changed = True
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)

    def _clear_voice_policy_form_styles(self) -> None:
        changed = False
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                if self._clear_voice_policy_row_style(table, row_index):
                    changed = True
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)

    def _handle_register_voice_style_selection_changed(self, row_index: int) -> None:
        if self._voice_policy_loading:
            return
        self._update_register_voice_style_row_status(row_index)
        self._refresh_register_voice_style_status_summary()
        self._set_voice_policy_form_dirty(True)

    def _fill_unstyled_register_voice_rows_with_current_style(self) -> None:
        try:
            style_payload = self._current_voice_policy_style_payload(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Register style", str(exc))
            return
        changed = 0
        for row_index in range(self._register_voice_style_table.rowCount()):
            if self._register_voice_style_override_payload(row_index, strict=False):
                continue
            if self._apply_register_voice_style_to_row(row_index, style_payload):
                changed += 1
        self._refresh_register_voice_style_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã điền style hiện tại cho {changed} hàng register style còn trống")
        else:
            QMessageBox.information(self, "Register style", "Không có hàng register style trống nào để điền nhanh.")

    def _fill_selected_register_voice_rows_with_current_style(self) -> None:
        try:
            style_payload = self._current_voice_policy_style_payload(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Register style", str(exc))
            return
        selected_rows = self._selected_table_row_indexes(self._register_voice_style_table)
        if not selected_rows:
            QMessageBox.information(self, "Register style", "Hãy chọn ít nhất một hàng register style.")
            return
        changed = 0
        for row_index in selected_rows:
            if self._apply_register_voice_style_to_row(row_index, style_payload):
                changed += 1
        self._refresh_register_voice_style_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"Đã điền style hiện tại cho {changed} hàng register style được chọn")
        else:
            QMessageBox.information(
                self,
                "Register style",
                "Các hàng register style đã chọn đã có cùng giá trị style hoặc không thay đổi.",
            )

    def _clear_register_voice_style_form_styles(self) -> None:
        changed = False
        for row_index in range(self._register_voice_style_table.rowCount()):
            if self._clear_register_voice_style_row_style(row_index):
                changed = True
        self._refresh_register_voice_style_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)

    def _clear_selected_register_voice_row_styles(self) -> None:
        selected_rows = self._selected_table_row_indexes(self._register_voice_style_table)
        if not selected_rows:
            QMessageBox.information(self, "Register style", "Hãy chọn ít nhất một hàng register style.")
            return
        changed = False
        for row_index in selected_rows:
            if self._clear_register_voice_style_row_style(row_index):
                changed = True
        self._refresh_register_voice_style_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)

    def _reload_voice_policies(self) -> None:
        self._voice_policy_loading = True
        self._character_voice_policy_table.setRowCount(0)
        self._relationship_voice_policy_table.setRowCount(0)
        self._register_voice_style_table.setRowCount(0)
        if not self._current_workspace:
            self._voice_policy_status.setText("Voice policy: chưa mở dự án")
            self._register_voice_style_status.setText("Register style: chưa mở dự án")
            self._voice_policy_loading = False
            self._set_voice_policy_form_dirty(False)
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        if not analysis_rows:
            self._voice_policy_status.setText("Voice policy: chưa có dữ liệu Contextual V2")
            self._register_voice_style_status.setText("Register style: chưa có dữ liệu Contextual V2")
            self._voice_policy_loading = False
            self._set_voice_policy_form_dirty(False)
            return

        character_name_map = self._character_name_map(database)
        relationship_rows = database.list_relationship_profiles(self._current_workspace.project_id)
        voice_policy_rows = database.list_voice_policies(self._current_workspace.project_id)
        register_style_policy_rows = database.list_register_voice_style_policies(
            self._current_workspace.project_id
        )
        available_preset_ids = {preset.voice_preset_id for preset in self._voice_presets}

        character_policy_map = {
            str(row["speaker_character_id"] or "").strip(): row
            for row in voice_policy_rows
            if str(row["policy_scope"] or "character").strip() == "character"
            and str(row["speaker_character_id"] or "").strip()
        }
        relationship_policy_map = {
            (
                str(row["speaker_character_id"] or "").strip(),
                str(row["listener_character_id"] or "").strip(),
            ): row
            for row in voice_policy_rows
            if str(row["policy_scope"] or "").strip() == "relationship"
            and str(row["speaker_character_id"] or "").strip()
            and str(row["listener_character_id"] or "").strip()
        }
        register_style_policy_map = {
            (
                str(row["politeness"] or "").strip().lower(),
                str(row["power_direction"] or "").strip().lower(),
                str(row["emotional_tone"] or "").strip().lower(),
                str(row["turn_function"] or "").strip().lower(),
                str(row["relation_type"] or "").strip().lower(),
            ): row
            for row in register_style_policy_rows
        }

        character_candidates = discover_speaker_candidates(analysis_rows, character_name_map=character_name_map)
        known_character_keys = {candidate.speaker_key for candidate in character_candidates}
        for speaker_key in sorted(character_policy_map):
            if speaker_key in known_character_keys:
                continue
            display_name = character_name_map.get(speaker_key, "").strip()
            label = f"{display_name} ({speaker_key})" if display_name and display_name != speaker_key else speaker_key
            character_candidates.append(
                type(character_candidates[0])(
                    speaker_type="character",
                    speaker_key=speaker_key,
                    label=label,
                    segment_count=0,
                )
                if character_candidates
                else discover_speaker_candidates(
                    [{"speaker_json": {"character_id": speaker_key}}],
                    character_name_map=character_name_map,
                )[0]
            )
        character_candidates.sort(key=lambda item: (-item.segment_count, item.label))

        relationship_candidates = discover_relationship_voice_policy_candidates(
            analysis_rows,
            relationship_rows=relationship_rows,
            character_name_map=character_name_map,
        )
        known_relationships = {(item.speaker_key, item.listener_key) for item in relationship_candidates}
        for speaker_key, listener_key in sorted(relationship_policy_map):
            if (speaker_key, listener_key) in known_relationships:
                continue
            speaker_label = character_name_map.get(speaker_key, "").strip()
            listener_label = character_name_map.get(listener_key, "").strip()
            relationship_candidates.append(
                type(relationship_candidates[0])(
                    speaker_key=speaker_key,
                    listener_key=listener_key,
                    label=(
                        f"{speaker_label} ({speaker_key}) -> {listener_label} ({listener_key})"
                        if speaker_label and listener_label
                        else f"{speaker_key} -> {listener_key}"
                    ),
                    segment_count=0,
                )
                if relationship_candidates
                else discover_relationship_voice_policy_candidates(
                    [
                        {
                            "speaker_json": {"character_id": speaker_key},
                            "listeners_json": [{"character_id": listener_key}],
                        }
                    ],
                    character_name_map=character_name_map,
                )[0]
            )
        relationship_candidates.sort(key=lambda item: (-item.segment_count, item.label))
        register_candidates = discover_register_voice_style_candidates(
            analysis_rows,
            relationship_rows=relationship_rows,
        )
        known_register_signatures = {
            (
                candidate.politeness,
                candidate.power_direction,
                candidate.emotional_tone,
                candidate.turn_function,
                candidate.relation_type,
            )
            for candidate in register_candidates
        }
        for signature in sorted(register_style_policy_map):
            if signature in known_register_signatures:
                continue
            politeness, power_direction, emotional_tone, turn_function, relation_type = signature
            label_parts = [
                f"lich_su={politeness}" if politeness else "",
                f"quyen_luc={power_direction}" if power_direction else "",
                f"cam_xuc={emotional_tone}" if emotional_tone else "",
                f"chuc_nang={turn_function}" if turn_function else "",
                f"quan_he={relation_type}" if relation_type else "",
            ]
            register_candidates.append(
                RegisterVoiceStyleCandidate(
                    label=", ".join(part for part in label_parts if part),
                    segment_count=0,
                    politeness=politeness,
                    power_direction=power_direction,
                    emotional_tone=emotional_tone,
                    turn_function=turn_function,
                    relation_type=relation_type,
                )
            )
        register_candidates.sort(key=lambda item: (-item.segment_count, item.label))

        for row_index, candidate in enumerate(character_candidates):
            self._character_voice_policy_table.insertRow(row_index)
            item = QTableWidgetItem(candidate.label)
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    "policy_scope": "character",
                    "speaker_character_id": candidate.speaker_key,
                    "listener_character_id": "",
                },
            )
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._character_voice_policy_table.setItem(row_index, 0, item)
            count_item = QTableWidgetItem(str(candidate.segment_count))
            count_item.setFlags(count_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._character_voice_policy_table.setItem(row_index, 1, count_item)
            combo = QComboBox()
            combo.addItem("Chưa gán", "")
            for preset in self._voice_presets:
                combo.addItem(f"{preset.name} ({preset.engine})", preset.voice_preset_id)
            selected_policy_row = character_policy_map.get(candidate.speaker_key)
            selected_preset_id = (
                str(selected_policy_row["voice_preset_id"] or "").strip() if selected_policy_row is not None else ""
            )
            if selected_preset_id:
                for combo_index in range(combo.count()):
                    if combo.itemData(combo_index) == selected_preset_id:
                        combo.setCurrentIndex(combo_index)
                        break
            self._character_voice_policy_table.setCellWidget(row_index, VOICE_POLICY_PRESET_COLUMN, combo)
            combo.currentIndexChanged.connect(
                lambda _index, row_index=row_index: self._handle_voice_policy_selection_changed(
                    self._character_voice_policy_table,
                    row_index,
                )
            )
            self._character_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_SPEED_COLUMN,
                self._create_voice_policy_override_input(
                    self._character_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["speed_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._character_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_VOLUME_COLUMN,
                self._create_voice_policy_override_input(
                    self._character_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["volume_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._character_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_PITCH_COLUMN,
                self._create_voice_policy_override_input(
                    self._character_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["pitch_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._update_voice_policy_row_status(
                self._character_voice_policy_table,
                row_index,
                available_preset_ids=available_preset_ids,
            )

        for row_index, candidate in enumerate(relationship_candidates):
            self._relationship_voice_policy_table.insertRow(row_index)
            item = QTableWidgetItem(candidate.label)
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    "policy_scope": "relationship",
                    "speaker_character_id": candidate.speaker_key,
                    "listener_character_id": candidate.listener_key,
                },
            )
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._relationship_voice_policy_table.setItem(row_index, 0, item)
            count_item = QTableWidgetItem(str(candidate.segment_count))
            count_item.setFlags(count_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._relationship_voice_policy_table.setItem(row_index, 1, count_item)
            combo = QComboBox()
            combo.addItem("Chưa gán", "")
            for preset in self._voice_presets:
                combo.addItem(f"{preset.name} ({preset.engine})", preset.voice_preset_id)
            selected_policy_row = relationship_policy_map.get((candidate.speaker_key, candidate.listener_key))
            selected_preset_id = (
                str(selected_policy_row["voice_preset_id"] or "").strip() if selected_policy_row is not None else ""
            )
            if selected_preset_id:
                for combo_index in range(combo.count()):
                    if combo.itemData(combo_index) == selected_preset_id:
                        combo.setCurrentIndex(combo_index)
                        break
            self._relationship_voice_policy_table.setCellWidget(row_index, VOICE_POLICY_PRESET_COLUMN, combo)
            combo.currentIndexChanged.connect(
                lambda _index, row_index=row_index: self._handle_voice_policy_selection_changed(
                    self._relationship_voice_policy_table,
                    row_index,
                )
            )
            self._relationship_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_SPEED_COLUMN,
                self._create_voice_policy_override_input(
                    self._relationship_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["speed_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._relationship_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_VOLUME_COLUMN,
                self._create_voice_policy_override_input(
                    self._relationship_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["volume_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._relationship_voice_policy_table.setCellWidget(
                row_index,
                VOICE_POLICY_PITCH_COLUMN,
                self._create_voice_policy_override_input(
                    self._relationship_voice_policy_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_policy_row["pitch_override"] if selected_policy_row is not None else None
                    ),
                    placeholder="Mặc định",
                ),
            )
            self._update_voice_policy_row_status(
                self._relationship_voice_policy_table,
                row_index,
                available_preset_ids=available_preset_ids,
            )

        for row_index, candidate in enumerate(register_candidates):
            self._register_voice_style_table.insertRow(row_index)
            signature = (
                candidate.politeness,
                candidate.power_direction,
                candidate.emotional_tone,
                candidate.turn_function,
                candidate.relation_type,
            )
            item = QTableWidgetItem(candidate.label)
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    "politeness": candidate.politeness,
                    "power_direction": candidate.power_direction,
                    "emotional_tone": candidate.emotional_tone,
                    "turn_function": candidate.turn_function,
                    "relation_type": candidate.relation_type,
                },
            )
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._register_voice_style_table.setItem(row_index, REGISTER_STYLE_LABEL_COLUMN, item)
            count_item = QTableWidgetItem(str(candidate.segment_count))
            count_item.setFlags(count_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._register_voice_style_table.setItem(row_index, REGISTER_STYLE_COUNT_COLUMN, count_item)
            for column, value in (
                (REGISTER_STYLE_POLITENESS_COLUMN, candidate.politeness),
                (REGISTER_STYLE_POWER_COLUMN, candidate.power_direction),
                (REGISTER_STYLE_EMOTION_COLUMN, candidate.emotional_tone),
                (REGISTER_STYLE_TURN_COLUMN, candidate.turn_function),
                (REGISTER_STYLE_RELATION_COLUMN, candidate.relation_type),
            ):
                detail_item = QTableWidgetItem(value or "-")
                detail_item.setFlags(detail_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self._register_voice_style_table.setItem(row_index, column, detail_item)
            selected_register_row = register_style_policy_map.get(signature)
            self._register_voice_style_table.setCellWidget(
                row_index,
                REGISTER_STYLE_SPEED_COLUMN,
                self._create_voice_policy_override_input(
                    self._register_voice_style_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_register_row["speed_override"] if selected_register_row is not None else None
                    ),
                    placeholder="Giữ nguyên",
                ),
            )
            self._register_voice_style_table.setCellWidget(
                row_index,
                REGISTER_STYLE_VOLUME_COLUMN,
                self._create_voice_policy_override_input(
                    self._register_voice_style_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_register_row["volume_override"] if selected_register_row is not None else None
                    ),
                    placeholder="Giữ nguyên",
                ),
            )
            self._register_voice_style_table.setCellWidget(
                row_index,
                REGISTER_STYLE_PITCH_COLUMN,
                self._create_voice_policy_override_input(
                    self._register_voice_style_table,
                    row_index,
                    value=self._coerce_optional_float(
                        selected_register_row["pitch_override"] if selected_register_row is not None else None
                    ),
                    placeholder="Giữ nguyên",
                ),
            )
            self._update_register_voice_style_row_status(row_index)

        self._voice_policy_loading = False
        self._refresh_voice_policy_status_summary()
        self._refresh_register_voice_style_status_summary()
        self._set_voice_policy_form_dirty(False)

    def _save_voice_policies(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Voice policy", "Hãy tạo hoặc mở dự án trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        now = utc_now_iso()
        policies: list[VoicePolicyRecord] = []
        register_style_policies: list[RegisterVoiceStylePolicyRecord] = []
        for table in (self._character_voice_policy_table, self._relationship_voice_policy_table):
            for row_index in range(table.rowCount()):
                policy_item = table.item(row_index, 0)
                combo = self._voice_policy_preset_combo(table, row_index)
                if policy_item is None or combo is None:
                    continue
                payload = policy_item.data(Qt.ItemDataRole.UserRole) or {}
                policy_scope = str(payload.get("policy_scope", "character") or "character").strip() or "character"
                speaker_character_id = str(payload.get("speaker_character_id", "") or "").strip()
                listener_character_id = str(payload.get("listener_character_id", "") or "").strip()
                voice_preset_id = str(combo.currentData() or "").strip()
                try:
                    style_overrides = self._voice_policy_override_payload(table, row_index, strict=True)
                except ValueError as exc:
                    QMessageBox.warning(self, "Voice policy", str(exc))
                    return
                if not speaker_character_id or (not voice_preset_id and not style_overrides):
                    continue
                policy_id = (
                    f"voicepolicy:relationship:{speaker_character_id}:{listener_character_id}"
                    if policy_scope == "relationship"
                    else f"voicepolicy:character:{speaker_character_id}"
                )
                policies.append(
                    VoicePolicyRecord(
                        policy_id=policy_id,
                        project_id=self._current_workspace.project_id,
                        policy_scope=policy_scope,
                        speaker_character_id=speaker_character_id,
                        listener_character_id=listener_character_id or None,
                        voice_preset_id=voice_preset_id,
                        speed_override=style_overrides.get("speed"),
                        volume_override=style_overrides.get("volume"),
                        pitch_override=style_overrides.get("pitch"),
                        created_at=now,
                        updated_at=now,
                    )
                )
        for row_index in range(self._register_voice_style_table.rowCount()):
            policy_item = self._register_voice_style_table.item(row_index, REGISTER_STYLE_LABEL_COLUMN)
            if policy_item is None:
                continue
            payload = policy_item.data(Qt.ItemDataRole.UserRole) or {}
            try:
                style_overrides = self._register_voice_style_override_payload(row_index, strict=True)
            except ValueError as exc:
                QMessageBox.warning(self, "Register style", str(exc))
                return
            if not style_overrides:
                continue
            politeness = str(payload.get("politeness", "") or "").strip().lower()
            power_direction = str(payload.get("power_direction", "") or "").strip().lower()
            emotional_tone = str(payload.get("emotional_tone", "") or "").strip().lower()
            turn_function = str(payload.get("turn_function", "") or "").strip().lower()
            relation_type = str(payload.get("relation_type", "") or "").strip().lower()
            policy_suffix = ":".join(
                part if part else "_" for part in (
                    politeness,
                    power_direction,
                    emotional_tone,
                    turn_function,
                    relation_type,
                )
            )
            register_style_policies.append(
                RegisterVoiceStylePolicyRecord(
                    policy_id=f"registerstyle:{policy_suffix}",
                    project_id=self._current_workspace.project_id,
                    politeness=politeness or None,
                    power_direction=power_direction or None,
                    emotional_tone=emotional_tone or None,
                    turn_function=turn_function or None,
                    relation_type=relation_type or None,
                    speed_override=style_overrides.get("speed"),
                    volume_override=style_overrides.get("volume"),
                    pitch_override=style_overrides.get("pitch"),
                    created_at=now,
                    updated_at=now,
                )
            )
        database.replace_voice_policies(self._current_workspace.project_id, policies)
        database.replace_register_voice_style_policies(
            self._current_workspace.project_id,
            register_style_policies,
        )
        self._set_voice_policy_form_dirty(False)
        self._invalidate_subtitle_pipeline_outputs(clear_tts_audio=True)
        self._reload_voice_policies()
        self._refresh_workspace_views()
        self._append_log_line(
            f"Đã lưu {len(policies)} voice policy và {len(register_style_policies)} register style policy"
        )
        QMessageBox.information(
            self,
            "Voice policy",
            (
                f"Đã lưu {len(policies)} voice policy và {len(register_style_policies)} register style policy.\n"
                "- Nếu đã thay đổi policy giọng, hãy chạy lại TTS rồi tạo lại track giọng."
            ),
        )

    @staticmethod
    def _voice_plan_block_lines(plan: object | None) -> list[str]:
        if plan is None:
            return []
        lines: list[str] = []
        unresolved_speakers = list(getattr(plan, "unresolved_speakers", []) or [])
        missing_preset_ids = list(getattr(plan, "missing_preset_ids", []) or [])
        if unresolved_speakers:
            lines.append(f"Blocked because speaker chưa gán preset: {', '.join(unresolved_speakers)}")
        if missing_preset_ids:
            lines.append(f"Blocked because preset không còn tồn tại: {', '.join(missing_preset_ids)}")
        return lines

    def _refresh_effective_voice_plan_preview(
        self,
        *,
        subtitle_rows: list[object],
        require_localized: bool,
        default_preset: object | None,
        segment_voice_presets: dict[str, object] | None,
        voice_plan: object | None,
    ) -> None:
        if not self._current_workspace:
            self._effective_voice_plan_preview.setPlainText("Chưa mở dự án.")
            return
        if default_preset is None:
            self._effective_voice_plan_preview.setPlainText("Chưa chọn preset giọng mặc định.")
            return
        voice_rows = [
            row for row in subtitle_rows if self._tts_output_text(row, require_localized=require_localized)
        ]
        if not voice_rows:
            self._effective_voice_plan_preview.setPlainText("Track hiện tại chưa có dòng đủ dữ liệu để chạy TTS.")
            return
        preset_source_counts: dict[str, int] = {}
        style_source_counts: dict[str, int] = {}
        preview_lines = [
            "Effective voice plan:",
            f"- Preset mặc định: {getattr(default_preset, 'name', '-')}",
        ]
        blocked_lines = self._voice_plan_block_lines(voice_plan)
        preview_lines.extend(f"- {line}" for line in blocked_lines)
        for row in voice_rows:
            segment_id = str(row["segment_id"])
            preset_source = (
                str(getattr(voice_plan, "segment_voice_sources", {}).get(segment_id, "fallback"))
                if voice_plan is not None
                else "fallback"
            )
            style_source = (
                str(getattr(voice_plan, "segment_voice_style_sources", {}).get(segment_id, "fallback"))
                if voice_plan is not None
                else "fallback"
            )
            preset_source_counts[preset_source] = preset_source_counts.get(preset_source, 0) + 1
            style_source_counts[style_source] = style_source_counts.get(style_source, 0) + 1
        preview_lines.append(
            "- Nguồn preset: "
            + ", ".join(f"{key}={value}" for key, value in sorted(preset_source_counts.items()))
        )
        preview_lines.append(
            "- Nguồn style: "
            + ", ".join(f"{key}={value}" for key, value in sorted(style_source_counts.items()))
        )
        preview_lines.append("- Mẫu dòng hiệu lực:")
        for row in voice_rows[:8]:
            segment_id = str(row["segment_id"])
            effective_preset = (
                segment_voice_presets.get(segment_id, default_preset)
                if segment_voice_presets is not None
                else default_preset
            )
            speaker_key = (
                str(getattr(voice_plan, "segment_speaker_keys", {}).get(segment_id, "?"))
                if voice_plan is not None
                else "?"
            )
            preset_source = (
                str(getattr(voice_plan, "segment_voice_sources", {}).get(segment_id, "fallback"))
                if voice_plan is not None
                else "fallback"
            )
            style_source = (
                str(getattr(voice_plan, "segment_voice_style_sources", {}).get(segment_id, "fallback"))
                if voice_plan is not None
                else "fallback"
            )
            style_source_details = (
                dict(getattr(voice_plan, "segment_voice_style_source_details", {}).get(segment_id, {}))
                if voice_plan is not None
                else {}
            )
            detail_suffix = ""
            if style_source_details:
                detail_suffix = (
                    ", field_sources="
                    + ",".join(
                        f"{field_name}:{source_name}"
                        for field_name, source_name in sorted(style_source_details.items())
                    )
                )
            preview_lines.append(
                "- "
                + f"{segment_id} / {speaker_key}: preset={getattr(effective_preset, 'voice_preset_id', '-')}"
                + f" [{preset_source}], style=({float(getattr(effective_preset, 'speed', 1.0)):.2f},"
                + f" {float(getattr(effective_preset, 'volume', 1.0)):.2f},"
                + f" {float(getattr(effective_preset, 'pitch', 0.0)):.2f})"
                + f" [{style_source}]"
                + detail_suffix
            )
        self._effective_voice_plan_preview.setPlainText("\n".join(preview_lines))

    def _rerun_downstream_only(self) -> None:
        self._start_workflow(
            ["tts", "voice_track", "mixdown", "export_video"],
            workflow_name="Chạy lại downstream",
        )

    def _resolve_tts_voice_plan(
        self,
        database: ProjectDatabase,
        subtitle_rows: list[object],
        *,
        require_localized: bool,
        dialog_title: str,
        warn_on_unresolved: bool,
    ) -> tuple[object | None, dict[str, object] | None, dict[str, str] | None, object]:
        default_preset = self._selected_voice_preset(strict=False)
        if default_preset is None:
            if warn_on_unresolved:
                QMessageBox.warning(self, dialog_title, "Không tìm thấy preset giọng trong dự án.")
            return None, None, None, None

        available_presets = {preset.voice_preset_id: preset for preset in self._voice_presets}
        available_presets[default_preset.voice_preset_id] = default_preset
        voice_rows = [
            row
            for row in subtitle_rows
            if self._tts_output_text(row, require_localized=require_localized)
        ]
        binding_rows = (
            database.list_speaker_bindings(self._current_workspace.project_id) if self._current_workspace else []
        )
        voice_policy_rows = (
            database.list_voice_policies(self._current_workspace.project_id) if self._current_workspace else []
        )
        register_style_policy_rows = (
            database.list_register_voice_style_policies(self._current_workspace.project_id)
            if self._current_workspace
            else []
        )
        relationship_rows = (
            database.list_relationship_profiles(self._current_workspace.project_id)
            if self._current_workspace
            else []
        )
        analysis_rows = (
            database.list_segment_analyses(self._current_workspace.project_id) if self._current_workspace else []
        )
        plan = build_speaker_binding_plan(
            subtitle_rows=voice_rows,
            analysis_rows=analysis_rows,
            binding_rows=binding_rows,
            voice_policy_rows=voice_policy_rows,
            relationship_rows=relationship_rows,
            register_style_policy_rows=register_style_policy_rows,
            available_preset_ids=set(available_presets),
        )
        if (
            not plan.active_bindings
            and not getattr(plan, "active_voice_policies", False)
            and not getattr(plan, "active_register_voice_styles", False)
        ):
            return default_preset, None, plan.segment_speaker_keys or None, plan

        if plan.missing_preset_ids or plan.unresolved_speakers:
            if warn_on_unresolved:
                lines = ["Voice plan hiện chưa an toàn, chưa thể chạy TTS."]
                lines.extend(f"- {line}" for line in self._voice_plan_block_lines(plan))
                lines.append(
                    "- Hãy vào tab Lồng tiếng, hoàn tất speaker binding/voice policy/register style rồi thử lại."
                )
                QMessageBox.warning(self, dialog_title, "\n".join(lines))
            return default_preset, None, plan.segment_speaker_keys or None, plan

        segment_voice_presets = resolve_segment_voice_presets(
            plan=plan,
            default_preset=default_preset,
            available_presets=available_presets,
        )
        return default_preset, segment_voice_presets or None, plan.segment_speaker_keys or None, plan

    @staticmethod
    def _parse_voice_float_value(
        raw_value: str,
        *,
        field_name: str,
        minimum: float,
        maximum: float,
        default: float,
        strict: bool,
    ) -> float:
        try:
            value = float(raw_value.strip() or str(default))
        except ValueError:
            if not strict:
                return default
            raise ValueError(f"{field_name} phải là số")
        if minimum <= value <= maximum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} phải nằm trong khoảng {minimum}..{maximum}")

    @staticmethod
    def _parse_voice_int_value(
        raw_value: str,
        *,
        field_name: str,
        minimum: int,
        default: int,
        strict: bool,
    ) -> int:
        try:
            value = int(raw_value.strip() or str(default))
        except ValueError:
            if not strict:
                return default
            raise ValueError(f"{field_name} phải là số nguyên")
        if value >= minimum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} phải >= {minimum}")

    def _build_unique_voice_preset_id(
        self,
        base_id: str,
        *,
        excluding_profile_id: str | None = None,
    ) -> str:
        existing_ids = {
            preset.voice_preset_id
            for preset in self._voice_presets
            if preset.voice_preset_id != excluding_profile_id
        }
        if base_id not in existing_ids:
            return base_id
        suffix = 2
        while f"{base_id}-{suffix}" in existing_ids:
            suffix += 1
        return f"{base_id}-{suffix}"

    def _save_current_voice_preset(self, checked: bool = False, *, save_as_new: bool = False) -> None:
        del checked
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        base_preset = self._base_selected_voice_preset()
        name = self._voice_profile_name_input.text().strip() or (base_preset.name if base_preset else "")
        if not name:
            QMessageBox.warning(self, "Preset giọng", "Hãy nhập tên preset giọng trước khi lưu.")
            return
        try:
            preset = self._selected_voice_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Preset giọng", str(exc))
            return
        if not preset:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy preset giọng để lưu.")
            return

        preset_id = (
            self._build_unique_voice_preset_id(
                self._slugify_token(name, fallback="voice-profile"),
                excluding_profile_id=None,
            )
            if save_as_new or base_preset is None
            else base_preset.voice_preset_id
        )
        preset = preset.model_copy(update={"voice_preset_id": preset_id, "name": name})
        output_path = save_voice_preset(self._current_workspace.root_dir, preset)
        self._reload_voice_presets()
        self._set_voice_combo_to_preset(preset.voice_preset_id)
        self._persist_active_voice_preset_id(preset.voice_preset_id)
        self._sync_voice_preset_form()
        self._refresh_workspace_views()
        self._append_log_line(f"Đã lưu preset giọng: {output_path}")
        QMessageBox.information(self, "Preset giọng", f"Đã lưu preset tại:\n{output_path}")

    def _delete_selected_voice_preset(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        preset = self._base_selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy preset để xóa.")
            return
        if len(self._voice_presets) <= 1:
            QMessageBox.warning(self, "Preset giọng", "Dự án phải còn ít nhất 1 preset giọng.")
            return
        answer = QMessageBox.question(
            self,
            "Xóa preset giọng",
            f"Xóa preset '{preset.name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return

        deleted_path = delete_voice_preset(self._current_workspace.root_dir, preset.voice_preset_id)
        if not deleted_path:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy file preset để xóa.")
            return
        self._reload_voice_presets()
        resolved_preset_id = self._set_voice_combo_to_preset(None)
        if resolved_preset_id:
            self._persist_active_voice_preset_id(resolved_preset_id)
        self._sync_voice_preset_form()
        self._refresh_workspace_views()
        self._append_log_line(f"Đã xóa preset giọng: {deleted_path}")
        QMessageBox.information(self, "Preset giọng", f"Đã xóa preset tại:\n{deleted_path}")

    def _batch_import_voice_profiles(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        try:
            template_preset = self._selected_voice_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Nhập hàng loạt", str(exc))
            return
        if template_preset is None:
            QMessageBox.warning(self, "Nhập hàng loạt", "Không tìm thấy preset mẫu.")
            return
        if template_preset.engine.lower() != "vieneu":
            template_preset = template_preset.model_copy(
                update={
                    "engine": "vieneu",
                    "sample_rate": 24000,
                    "language": template_preset.language or "vi",
                    "engine_options": {"mode": "local"},
                }
            )

        report = batch_import_voice_clone_presets(
            self._current_workspace.root_dir,
            template_preset=template_preset,
        )
        if not report.imported_presets:
            QMessageBox.warning(
                self,
                "Nhập hàng loạt",
                "Không import được profile nào. Hãy đặt file audio mẫu trong `assets/voices` kèm file `.txt` cùng tên.",
            )
            return

        self._reload_voice_presets()
        first_preset_id = report.imported_presets[0].voice_preset_id
        self._set_voice_combo_to_preset(first_preset_id)
        self._persist_active_voice_preset_id(first_preset_id)
        self._sync_voice_preset_form()
        self._refresh_workspace_views()
        self._voice_profile_status.setText(
            "Đã nhập hàng loạt "
            f"{len(report.imported_presets)} preset; thiếu `.txt`={len(report.skipped_missing_text)}; "
            f"`.txt` rỗng={len(report.skipped_empty_text)}"
        )
        self._append_log_line(
            "Nhập hàng loạt preset giọng: "
            f"đã nhập={len(report.imported_presets)} "
            f"thiếu .txt={len(report.skipped_missing_text)} "
            f".txt rỗng={len(report.skipped_empty_text)}"
        )
        QMessageBox.information(
            self,
            "Nhập hàng loạt",
            "Đã nhập preset giọng:\n"
            f"- Đã tạo: {len(report.imported_presets)}\n"
            f"- Thiếu file `.txt`: {len(report.skipped_missing_text)}\n"
            f"- File `.txt` rỗng: {len(report.skipped_empty_text)}",
        )

    def _base_selected_export_preset(self):
        if not self._export_presets:
            return None
        preset_id = self._export_preset_combo.currentData()
        for preset in self._export_presets:
            if preset.export_preset_id == preset_id:
                return preset
        return self._export_presets[0]

    def _base_selected_watermark_profile(self) -> WatermarkProfile | None:
        if not self._watermark_profiles:
            return None
        profile_id = self._watermark_profile_combo.currentData()
        for profile in self._watermark_profiles:
            if profile.watermark_profile_id == profile_id:
                return profile
        return self._watermark_profiles[0]

    def _sync_export_preset_form(self) -> None:
        preset = self._base_selected_export_preset()
        blocked = self._burn_subtitles_checkbox.blockSignals(True)
        if preset is None:
            self._burn_subtitles_checkbox.setChecked(True)
            self._burn_subtitles_checkbox.setEnabled(False)
        else:
            self._burn_subtitles_checkbox.setEnabled(True)
            self._burn_subtitles_checkbox.setChecked(bool(preset.burn_subtitles))
        self._burn_subtitles_checkbox.blockSignals(blocked)

    def _handle_export_preset_changed(self, index: int) -> None:
        del index
        self._persist_active_export_preset_id(
            str(self._export_preset_combo.currentData()) if self._export_preset_combo.currentData() else None
        )
        self._sync_export_preset_form()
        if self._current_workspace:
            self._refresh_workspace_views()

    def _handle_export_mode_changed(self, _state: int) -> None:
        if self._current_workspace:
            self._refresh_workspace_views()

    def _sync_watermark_profile_form(self) -> None:
        profile = self._base_selected_watermark_profile()
        if profile is None:
            values = WatermarkProfile(
                watermark_profile_id="watermark-unsaved",
                name="",
                watermark_enabled=False,
                watermark_path=None,
                watermark_position="top-right",
                watermark_opacity=0.85,
                watermark_scale=0.16,
                watermark_margin=24,
                notes="Chưa có profile watermark. Bạn có thể nhập thông số rồi lưu thành profile mới.",
            )
        else:
            values = profile

        widgets = (
            self._watermark_profile_name_input,
            self._watermark_enabled_checkbox,
            self._watermark_path_input,
            self._watermark_position_combo,
            self._watermark_opacity_input,
            self._watermark_scale_input,
            self._watermark_margin_input,
        )
        blocked_states = [widget.blockSignals(True) for widget in widgets]
        self._watermark_profile_name_input.setText(values.name)
        self._watermark_enabled_checkbox.setChecked(bool(values.watermark_enabled))
        self._watermark_path_input.setText(values.watermark_path or "")
        self._set_combo_value(
            self._watermark_position_combo,
            str(values.watermark_position or "top-right"),
        )
        self._watermark_opacity_input.setText(str(values.watermark_opacity))
        self._watermark_scale_input.setText(str(values.watermark_scale))
        self._watermark_margin_input.setText(str(values.watermark_margin))
        for widget, blocked in zip(widgets, blocked_states, strict=True):
            widget.blockSignals(blocked)

        if profile is None:
            self._watermark_profile_status.setText(values.notes)
        else:
            self._watermark_profile_status.setText(profile.notes or "Không có ghi chú cho profile watermark này")

    def _handle_watermark_profile_changed(self, index: int) -> None:
        del index
        self._persist_active_watermark_profile_id(
            str(self._watermark_profile_combo.currentData())
            if self._watermark_profile_combo.currentData()
            else None
        )
        self._sync_watermark_profile_form()
        if self._current_workspace:
            self._refresh_workspace_views()

    def _handle_watermark_form_changed(self) -> None:
        base_profile = self._base_selected_watermark_profile()
        if base_profile is not None:
            self._watermark_profile_status.setText(
                f"Đang sửa profile {base_profile.name}. Bấm 'Lưu profile' hoặc 'Lưu thành bản mới' để lưu lại."
            )
        else:
            self._watermark_profile_status.setText(
                "Đang sửa profile watermark tạm thời. Bấm 'Lưu thành bản mới' để tái sử dụng cho lần sau."
            )
        if self._current_workspace:
            self._refresh_workspace_views()

    def _selected_watermark_profile(self, *, strict: bool = False) -> WatermarkProfile | None:
        base_profile = self._base_selected_watermark_profile()
        raw_name = self._watermark_profile_name_input.text().strip()
        raw_path = self._watermark_path_input.text().strip()
        name = raw_name or (base_profile.name if base_profile else "")
        if not name and not raw_path and not self._watermark_enabled_checkbox.isChecked() and base_profile is None:
            return None

        profile_id = (
            base_profile.watermark_profile_id
            if base_profile is not None
            else self._slugify_token(name or "watermark-profile", fallback="watermark-profile")
        )
        position = str(self._watermark_position_combo.currentData() or "top-right")
        return WatermarkProfile(
            watermark_profile_id=profile_id,
            name=name or "Profile watermark",
            watermark_enabled=self._watermark_enabled_checkbox.isChecked(),
            watermark_path=raw_path or None,
            watermark_position=position,
            watermark_opacity=self._parse_watermark_float_value(
                self._watermark_opacity_input.text(),
                field_name="Watermark opacity",
                minimum=0.0,
                maximum=1.0,
                default=base_profile.watermark_opacity if base_profile else 0.85,
                strict=strict,
            ),
            watermark_scale=self._parse_watermark_float_value(
                self._watermark_scale_input.text(),
                field_name="Watermark scale",
                minimum=0.01,
                maximum=1.0,
                default=base_profile.watermark_scale if base_profile else 0.16,
                strict=strict,
            ),
            watermark_margin=self._parse_watermark_int_value(
                self._watermark_margin_input.text(),
                field_name="Watermark margin",
                minimum=0,
                default=base_profile.watermark_margin if base_profile else 24,
                strict=strict,
            ),
            notes=base_profile.notes if base_profile else "",
        )

    def _selected_export_preset(self, *, strict: bool = False):
        preset = self._base_selected_export_preset()
        if not preset:
            return None
        watermark_profile = self._selected_watermark_profile(strict=strict)
        updates = {"burn_subtitles": self._burn_subtitles_checkbox.isChecked()}
        if watermark_profile is not None:
            updates.update(
                {
                    "watermark_enabled": watermark_profile.watermark_enabled,
                    "watermark_path": watermark_profile.watermark_path,
                    "watermark_position": watermark_profile.watermark_position,
                    "watermark_opacity": watermark_profile.watermark_opacity,
                    "watermark_scale": watermark_profile.watermark_scale,
                    "watermark_margin": watermark_profile.watermark_margin,
                }
            )
        return preset.model_copy(update=updates)

    @staticmethod
    def _set_combo_value(combo: QComboBox, value: str) -> None:
        for index in range(combo.count()):
            if combo.itemData(index) == value:
                combo.setCurrentIndex(index)
                return
        if combo.count() > 0:
            combo.setCurrentIndex(0)

    @staticmethod
    def _slugify_token(raw_value: str, *, fallback: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", raw_value.strip().lower()).strip("-")
        return slug or fallback

    @staticmethod
    def _parse_watermark_float_value(
        raw_value: str,
        *,
        field_name: str,
        minimum: float,
        maximum: float,
        default: float,
        strict: bool,
    ) -> float:
        try:
            value = float(raw_value.strip() or str(default))
        except ValueError:
            if not strict:
                return default
            raise ValueError(f"{field_name} phải là số")
        if minimum <= value <= maximum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} phải nằm trong khoảng {minimum}..{maximum}")

    @staticmethod
    def _parse_watermark_int_value(
        raw_value: str,
        *,
        field_name: str,
        minimum: int,
        default: int,
        strict: bool,
    ) -> int:
        try:
            value = int(raw_value.strip() or str(default))
        except ValueError:
            if not strict:
                return default
            raise ValueError(f"{field_name} phải là số nguyên")
        if value >= minimum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} phải >= {minimum}")

    def _build_unique_watermark_profile_id(
        self,
        base_id: str,
        *,
        excluding_profile_id: str | None = None,
    ) -> str:
        existing_ids = {
            profile.watermark_profile_id
            for profile in self._watermark_profiles
            if profile.watermark_profile_id != excluding_profile_id
        }
        if base_id not in existing_ids:
            return base_id
        suffix = 2
        while f"{base_id}-{suffix}" in existing_ids:
            suffix += 1
        return f"{base_id}-{suffix}"

    def _save_current_watermark_profile(self, checked: bool = False, *, save_as_new: bool = False) -> None:
        del checked
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return
        base_profile = self._base_selected_watermark_profile()
        name = self._watermark_profile_name_input.text().strip() or (
            base_profile.name if base_profile else ""
        )
        if not name:
            QMessageBox.warning(self, "Profile watermark", "Hãy nhập tên profile trước khi lưu.")
            return

        base_id = self._slugify_token(name, fallback="watermark-profile")
        profile_id = (
            self._build_unique_watermark_profile_id(
                base_id,
                excluding_profile_id=None,
            )
            if save_as_new or base_profile is None
            else base_profile.watermark_profile_id
        )
        try:
            profile = self._selected_watermark_profile(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Profile watermark", str(exc))
            return
        if profile is None:
            QMessageBox.warning(self, "Profile watermark", "Không có dữ liệu để lưu.")
            return
        profile = profile.model_copy(
            update={
                "watermark_profile_id": profile_id,
                "name": name,
            }
        )

        output_path = save_watermark_profile(self._current_workspace.root_dir, profile)
        self._reload_watermark_profiles()
        self._set_watermark_combo_to_profile(profile.watermark_profile_id)
        self._persist_active_watermark_profile_id(profile.watermark_profile_id)
        self._sync_watermark_profile_form()
        self._refresh_workspace_views()
        self._append_log_line(f"Đã lưu watermark profile: {output_path}")
        QMessageBox.information(self, "Profile watermark", f"Đã lưu profile tại:\n{output_path}")

    @staticmethod
    def _parse_volume_value(raw_value: str, *, field_name: str) -> float:
        try:
            value = float(raw_value.strip() or "0")
        except ValueError as exc:
            raise ValueError(f"{field_name} phải là số") from exc
        if value < 0:
            raise ValueError(f"{field_name} không được âm")
        return value

    def _resolve_bgm_path(self) -> Path | None:
        raw_value = self._bgm_path_input.text().strip()
        return self._resolve_workspace_file_path(raw_value)

    def _resolve_watermark_path(self) -> Path | None:
        export_preset = self._selected_export_preset(strict=False)
        if not export_preset or not export_preset.watermark_path:
            return None
        return self._resolve_workspace_file_path(export_preset.watermark_path)

    def _resolve_workspace_file_path(self, raw_value: str) -> Path | None:
        normalized = raw_value.strip()
        if not normalized:
            return None
        candidate = Path(normalized).expanduser()
        if not candidate.is_absolute() and self._current_workspace:
            candidate = self._current_workspace.root_dir / candidate
        return candidate.resolve() if candidate.exists() else None

    def _selected_subtitle_row_index(self) -> int:
        row_index = self._subtitle_table.currentRow()
        return row_index if row_index >= 0 else -1

    def _split_selected_subtitle_row(self) -> None:
        row_index = self._selected_subtitle_row_index()
        if row_index < 0:
            QMessageBox.warning(self, "Biên tập phụ đề", "Hãy chọn một dòng để tách.")
            return
        try:
            rows = self._collect_subtitle_table_rows()
            first, second = split_editor_row(rows[row_index])
        except ValueError as exc:
            QMessageBox.warning(self, "Biên tập phụ đề", str(exc))
            return

        new_rows = rows[:row_index] + [first, second] + rows[row_index + 1 :]
        self._replace_subtitle_editor_rows(
            new_rows,
            status_message=f"Đã tách dòng {row_index + 1} thành 2 đoạn. Hãy lưu để áp dụng.",
        )
        self._subtitle_table.selectRow(row_index)

    def _merge_selected_subtitle_row_with_next(self) -> None:
        row_index = self._selected_subtitle_row_index()
        if row_index < 0:
            QMessageBox.warning(self, "Biên tập phụ đề", "Hãy chọn một dòng để gộp.")
            return
        try:
            rows = self._collect_subtitle_table_rows()
        except ValueError as exc:
            QMessageBox.warning(self, "Biên tập phụ đề", str(exc))
            return
        if row_index >= len(rows) - 1:
            QMessageBox.warning(self, "Biên tập phụ đề", "Không có dòng tiếp theo để gộp.")
            return

        merged = merge_editor_rows(rows[row_index], rows[row_index + 1])
        new_rows = rows[:row_index] + [merged] + rows[row_index + 2 :]
        self._replace_subtitle_editor_rows(
            new_rows,
            status_message=f"Đã gộp dòng {row_index + 1} với dòng kế tiếp. Hãy lưu để áp dụng.",
        )
        self._subtitle_table.selectRow(row_index)

    def _sync_settings_to_form(self) -> None:
        dependency_paths = self._settings.dependency_paths
        self._ui_language_input.setText(self._settings.ui_language)
        self._sync_ui_mode_controls()
        self._ffmpeg_path_input.setText(dependency_paths.ffmpeg_path or "")
        self._ffprobe_path_input.setText(dependency_paths.ffprobe_path or "")
        self._mpv_path_input.setText(dependency_paths.mpv_dll_path or "")
        self._model_cache_input.setText(self._settings.model_cache_dir or "")
        self._openai_key_input.setText(self._settings.openai_api_key or "")
        self._default_translation_model_input.setText(self._settings.default_translation_model)
        self._translation_model_input.setText(self._settings.default_translation_model)

    def _save_settings(self) -> None:
        dependency_paths = self._settings.dependency_paths
        self._settings.ui_language = self._ui_language_input.text().strip() or "vi"
        if self._settings_ui_mode_combo is not None:
            self._settings.ui_mode = normalize_ui_mode(
                str(self._settings_ui_mode_combo.currentData() or UI_MODE_SIMPLE_V2)
            )
        dependency_paths.ffmpeg_path = self._ffmpeg_path_input.text().strip() or None
        dependency_paths.ffprobe_path = self._ffprobe_path_input.text().strip() or None
        dependency_paths.mpv_dll_path = self._mpv_path_input.text().strip() or None
        self._settings.model_cache_dir = self._model_cache_input.text().strip() or None
        self._settings.openai_api_key = self._openai_key_input.text().strip() or None
        self._settings.default_translation_model = (
            self._default_translation_model_input.text().strip() or "gpt-4.1-mini"
        )
        save_settings(self._settings)
        self._sync_ui_mode_controls()
        self._reload_project_profile_options()
        self._apply_ui_mode()
        self._append_log_line("Đã lưu cài đặt")
        QMessageBox.information(self, "Đã lưu", "Cài đặt ứng dụng đã được lưu.")

    def _check_ffmpeg(self) -> None:
        installation = detect_ffmpeg_installation(self._settings)
        lines = [
            f"ffmpeg: {'OK' if installation.ffmpeg.available else 'Missing'}",
            f"  path: {installation.ffmpeg.executable or '-'}",
            f"  version: {installation.ffmpeg.version_line or installation.ffmpeg.error or '-'}",
            f"ffprobe: {'OK' if installation.ffprobe.available else 'Missing'}",
            f"  path: {installation.ffprobe.executable or '-'}",
            f"  version: {installation.ffprobe.version_line or installation.ffprobe.error or '-'}",
        ]
        text = "\n".join(lines)
        self._ffmpeg_status.setText(text)
        self._append_log_line("Kiểm tra FFmpeg:\n" + text)

    @staticmethod
    def _format_bytes(value: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        amount = float(value)
        unit = units[0]
        for unit in units:
            if amount < 1024.0 or unit == units[-1]:
                break
            amount /= 1024.0
        return f"{amount:.2f} {unit}"

    def _selected_cache_bucket_names(self) -> tuple[str, ...]:
        bucket_name = str(self._cache_bucket_combo.currentData() or "").strip()
        if not bucket_name:
            return ("audio", "asr", "translate", "translate_contextual", "tts", "mix", "subs", "exports")
        return (bucket_name,)

    def _refresh_doctor_labels(self, report) -> None:
        headline = (
            f"Doctor: {report.error_count} blocking, {report.warning_count} warning"
            if report.requested_stages
            else f"Doctor: {report.error_count} error, {report.warning_count} warning"
        )
        lines = [headline]
        for item in report.checks:
            lines.append(f"- {item.name}: {item.status} | {item.message}")
        text = "\n".join(lines)
        self._doctor_summary.setText(text)
        self._settings_doctor_status.setText(text)

    def _effective_doctor_voice_preset(self):
        try:
            return self._selected_voice_preset(strict=False)
        except Exception:
            return None

    def _run_doctor_report(self, *, stages: list[str] | tuple[str, ...] | None = None):
        report = run_doctor(
            settings=self._settings,
            workspace=self._current_workspace,
            requested_stages=stages,
            voice_preset=self._effective_doctor_voice_preset(),
        )
        self._last_doctor_report = report
        self._refresh_doctor_labels(report)
        return report

    def _ensure_doctor_ready(self, *, stages: list[str] | tuple[str, ...], dialog_title: str) -> bool:
        report = self._run_doctor_report(stages=stages)
        blocked_message = format_blocked_message(report, stages=stages, action_label=dialog_title.lower())
        if not blocked_message:
            return True
        QMessageBox.warning(self, dialog_title, blocked_message)
        return False

    def _create_backup_if_possible(
        self,
        *,
        stage: str,
        reason: str,
        dialog_title: str,
        show_dialog: bool = False,
    ) -> bool:
        if not self._current_workspace:
            return False
        try:
            manifest = create_workspace_backup(self._current_workspace, reason=reason, stage=stage)
        except OSError as exc:
            QMessageBox.warning(
                self,
                dialog_title,
                f"Blocked because không tạo được backup trước khi tiếp tục:\n- {exc}",
            )
            return False
        self._append_log_line(f"Tạo backup trước stage {stage}: {manifest.backup_dir}")
        if show_dialog:
            QMessageBox.information(self, dialog_title, f"Đã tạo backup tại:\n{manifest.backup_dir}")
        return True

    def _run_project_doctor_check(self) -> None:
        report = self._run_doctor_report()
        QMessageBox.information(
            self,
            "Doctor",
            "\n".join(
                [f"Doctor report: {report.error_count} error, {report.warning_count} warning"]
                + [f"- {item.name}: {item.status} | {item.message}" for item in report.checks]
            ),
        )

    def _run_settings_doctor_check(self) -> None:
        report = self._run_doctor_report()
        QMessageBox.information(
            self,
            "Doctor",
            "\n".join(
                [f"Doctor report: {report.error_count} error, {report.warning_count} warning"]
                + [f"- {item.name}: {item.status} | {item.message}" for item in report.checks]
            ),
        )

    def _inspect_workspace_safety(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Workspace safety", "Hãy mở dự án trước.")
            return
        report = inspect_workspace(self._current_workspace)
        self._last_repair_report = report
        lines = [
            f"Workspace safety: {report.error_count} error, {report.warning_count} warning",
            f"- Đã kiểm tra {report.checked_file_count} file/path và {report.checked_job_count} job",
        ]
        lines.extend(f"- {issue.code}: {issue.message}" for issue in report.issues[:12])
        self._workspace_repair_summary.setText("\n".join(lines))
        QMessageBox.information(self, "Workspace safety", "\n".join(lines))

    def _repair_workspace_metadata(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Workspace safety", "Hãy mở dự án trước.")
            return
        report = repair_workspace_metadata(self._current_workspace)
        self._last_repair_report = report
        lines = [
            f"Sửa metadata xong: {len(report.fixed_items)} mục được sửa",
            f"- Còn lại: {report.error_count} error, {report.warning_count} warning",
        ]
        lines.extend(f"- {item}" for item in report.fixed_items[:12])
        self._workspace_repair_summary.setText("\n".join(lines))
        QMessageBox.information(self, "Workspace safety", "\n".join(lines))
        self._refresh_workspace_views()

    def _create_manual_workspace_backup(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Backup", "Hãy mở dự án trước.")
            return
        self._create_backup_if_possible(
            stage="manual_backup",
            reason="Manual workspace backup from UI",
            dialog_title="Backup",
            show_dialog=True,
        )
        self._refresh_workspace_views()

    def _prune_orphan_cache(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Cache ops", "Hãy mở dự án trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        report = cleanup_cache(self._current_workspace, database, bucket_names=None)
        QMessageBox.information(
            self,
            "Cache ops",
            (
                "Đã dọn cache mồ côi.\n"
                f"- Số file xóa: {len(report.deleted_paths)}\n"
                f"- Dung lượng giải phóng: {self._format_bytes(report.deleted_bytes)}"
            ),
        )
        self._refresh_workspace_views()

    def _clear_selected_cache_bucket(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Cache ops", "Hãy mở dự án trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        bucket_names = self._selected_cache_bucket_names()
        report = cleanup_cache(self._current_workspace, database, bucket_names=list(bucket_names))
        QMessageBox.information(
            self,
            "Cache ops",
            (
                f"Đã dọn bucket: {', '.join(bucket_names)}\n"
                f"- Số file xóa: {len(report.deleted_paths)}\n"
                f"- Dung lượng giải phóng: {self._format_bytes(report.deleted_bytes)}"
            ),
        )
        self._refresh_workspace_views()

    def _refresh_ops_views(self, database: ProjectDatabase) -> None:
        if not self._current_workspace:
            return
        workspace = self._current_workspace
        self._last_doctor_report = run_doctor(
            settings=self._settings,
            workspace=workspace,
            voice_preset=self._effective_doctor_voice_preset(),
        )
        self._refresh_doctor_labels(self._last_doctor_report)

        self._last_repair_report = inspect_workspace(workspace)
        backup_root = get_backups_root(workspace.root_dir)
        backup_count = (
            len([path for path in backup_root.iterdir() if path.is_dir()])
            if backup_root.exists()
            else 0
        )
        self._workspace_repair_summary.setText(
            "\n".join(
                [
                    f"Workspace safety: {self._last_repair_report.error_count} error, {self._last_repair_report.warning_count} warning",
                    f"- Đã kiểm tra {self._last_repair_report.checked_file_count} file/path và {self._last_repair_report.checked_job_count} job",
                    f"- Backups: {backup_count} thư mục trong {backup_root}",
                ]
            )
        )

        self._last_cache_inventory = build_cache_inventory(workspace, database)
        total_files = sum(bucket.file_count for bucket in self._last_cache_inventory.buckets)
        total_bytes = sum(bucket.total_bytes for bucket in self._last_cache_inventory.buckets)
        orphan_files = sum(bucket.orphan_file_count for bucket in self._last_cache_inventory.buckets)
        orphan_bytes = sum(bucket.orphan_bytes for bucket in self._last_cache_inventory.buckets)
        self._cache_ops_summary.setText(
            "\n".join(
                [
                    f"Cache ops: {total_files} file, {self._format_bytes(total_bytes)}",
                    f"- Orphan: {orphan_files} file, {self._format_bytes(orphan_bytes)}",
                    "- Cleanup sẽ giữ lại artifact đang được runtime_state/job_runs tham chiếu.",
                ]
            )
        )

    def _run_probe_media_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["probe_media"], dialog_title="Doc metadata"):
            return None

        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "Chưa có video", "Hãy chọn video nguồn hợp lệ.")
            return None

        workspace = self._current_workspace
        ffprobe_path = self._settings.dependency_paths.ffprobe_path

        def handler(context: JobContext) -> JobResult:
            context.report_progress(5, "Đang đọc metadata bằng ffprobe")
            metadata = probe_media(source_video_path, ffprobe_path=ffprobe_path)
            asset = attach_source_video_to_project(workspace, metadata)
            context.report_progress(100, "Đã cập nhật video nguồn")
            return JobResult(
                message="Đã đọc metadata video",
                extra={"metadata": metadata, "asset": asset},
            )

        return self._job_manager.submit_job(
            stage="probe_media",
            description="Đọc metadata video nguồn và cập nhật MediaAsset",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_extract_audio_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["extract_audio"], dialog_title="Tach am thanh"):
            return None

        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "Chưa có video", "Hãy chọn video nguồn hợp lệ.")
            return None

        workspace = self._current_workspace
        ffprobe_path = self._settings.dependency_paths.ffprobe_path
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path
        metadata_hint = self._media_metadata

        def handler(context: JobContext) -> JobResult:
            metadata = metadata_hint
            if metadata is None or metadata.source_path != source_video_path.resolve():
                context.report_progress(2, "Đang đọc thông tin video nguồn")
                metadata = probe_media(source_video_path, ffprobe_path=ffprobe_path)
                attach_source_video_to_project(workspace, metadata)
            if not metadata.primary_audio_stream:
                raise RuntimeError("Video không có audio stream để tách.")

            artifacts = extract_audio_artifacts(
                context,
                workspace=workspace,
                metadata=metadata,
                ffmpeg_path=ffmpeg_path,
            )
            return JobResult(
                message="Đã tách âm thanh 16 kHz và 48 kHz",
                output_paths=[artifacts.audio_16k_path, artifacts.audio_48k_path],
                extra={"metadata": metadata, "artifacts": artifacts},
            )

        return self._job_manager.submit_job(
            stage="extract_audio",
            description="Tách audio 16 kHz và 48 kHz vào bộ nhớ đệm",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_asr_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["asr"], dialog_title="ASR"):
            return None

        workspace = self._current_workspace
        artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
        if not artifacts or not artifacts.audio_16k_path.exists():
            QMessageBox.warning(
                self,
                "Chưa có audio cho ASR",
                "Hãy tách audio trước khi chạy ASR.",
            )
            return None

        options = TranscriptionOptions(
            model_name=self._asr_model_combo.currentText(),
            language=None if self._asr_language_combo.currentText() == "auto" else self._asr_language_combo.currentText(),
            vad_filter=self._vad_checkbox.isChecked(),
            word_timestamps=self._word_timestamps_checkbox.isChecked(),
        )
        settings = self._settings
        duration_ms = self._media_metadata.duration_ms if self._media_metadata else None

        def handler(context: JobContext) -> JobResult:
            engine = FasterWhisperEngine(settings)
            result = engine.transcribe(
                context,
                audio_path=str(artifacts.audio_16k_path),
                options=options,
                duration_ms=duration_ms,
            )
            persisted = persist_transcription_result(
                workspace,
                result=result,
                options=options,
            )
            return JobResult(
                message=f"Đã lưu {persisted.segment_count} phân đoạn ASR",
                output_paths=[persisted.segments_json_path],
                extra={"result": result, "persisted": persisted},
            )

        return self._job_manager.submit_job(
            stage="asr",
            description="Chạy faster-whisper và lưu phân đoạn vào CSDL",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _reload_prompt_templates(self) -> None:
        if not self._current_workspace:
            self._prompt_templates = []
            self._prompt_combo.clear()
            self._translation_mode_info.setText("legacy")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        project_row = database.get_project()
        source_language = str(project_row["source_language"] if project_row else "auto")
        target_language = str(project_row["target_language"] if project_row else "vi")
        ensure_prompt_templates(self._current_workspace.root_dir, source_language, target_language)
        translation_mode = self._current_translation_mode(project_row)
        if translation_mode == "contextual_v2":
            self._prompt_templates = list_prompt_templates(
                self._current_workspace.root_dir,
                translation_mode=translation_mode,
                role="dialogue_adaptation",
            )
            self._translation_mode_info.setText("Contextual V2")
        else:
            self._prompt_templates = list_prompt_templates(
                self._current_workspace.root_dir,
                translation_mode="legacy",
            )
            self._translation_mode_info.setText("Legacy")
        if not self._prompt_templates:
            self._prompt_templates = list_prompt_templates(self._current_workspace.root_dir)
        self._prompt_combo.clear()
        for template in self._prompt_templates:
            self._prompt_combo.addItem(f"{template.name} ({template.template_id})", template.template_id)
        state = load_project_profile_state(self._current_workspace.root_dir)
        recommended_template_id = state.recommended_prompt_template_id if state is not None else None
        if recommended_template_id:
            index = self._prompt_combo.findData(recommended_template_id)
            if index >= 0:
                self._prompt_combo.setCurrentIndex(index)

    def _selected_prompt_template(self):
        if not self._prompt_templates:
            return None
        template_id = self._prompt_combo.currentData()
        for template in self._prompt_templates:
            if template.template_id == template_id:
                return template
        return self._prompt_templates[0]

    def _current_translation_mode(self, project_row: object | None = None) -> str:
        if not self._current_workspace:
            return "legacy"
        if project_row is None:
            database = ProjectDatabase(self._current_workspace.database_path)
            project_row = database.get_project()
        if not project_row:
            return "legacy"
        try:
            value = str(project_row["translation_mode"] or "").strip()
        except Exception:
            value = str(getattr(project_row, "translation_mode", "") or "").strip()
        return value or "legacy"

    @staticmethod
    def _analysis_json_field(row: object, field: str, default: object) -> object:
        try:
            raw_value = row[field]
        except Exception:
            raw_value = getattr(row, field, default)
        if raw_value in (None, ""):
            return default
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(str(raw_value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return default

    def _reload_review_queue(self) -> None:
        self._pending_review_segment_id = None
        self._review_table.setRowCount(0)
        self._review_context_text.clear()
        for widget in (
            self._review_speaker_input,
            self._review_listener_input,
            self._review_self_term_input,
            self._review_address_term_input,
            self._review_subtitle_input,
            self._review_tts_input,
        ):
            widget.clear()
        if not self._current_workspace:
            self._review_summary.setText("Chưa có dự án")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        if self._current_translation_mode(database.get_project()) != "contextual_v2":
            self._review_summary.setText("Dự án này đang dùng chế độ dịch legacy")
            return
        review_rows = database.list_review_queue_items(self._current_workspace.project_id)
        self._review_summary.setText(
            f"Hàng review semantic: {len(review_rows)} dòng cần duyệt trước TTS/export"
        )
        self._review_table.setRowCount(len(review_rows))
        for row_index, row in enumerate(review_rows):
            speaker = self._analysis_json_field(row, "speaker_json", {})
            listeners = self._analysis_json_field(row, "listeners_json", [])
            policy = self._analysis_json_field(row, "honorific_policy_json", {})
            reasons = self._analysis_json_field(row, "review_reason_codes_json", [])
            items = [
                QTableWidgetItem(str(int(row["segment_index"]) + 1)),
                QTableWidgetItem(str(int(row["scene_index"]) + 1)),
                QTableWidgetItem(str(row["source_text"] or "")),
                QTableWidgetItem(str(speaker.get("character_id", "unknown"))),
                QTableWidgetItem(
                    str((listeners[0] or {}).get("character_id", "unknown")) if listeners else "unknown"
                ),
                QTableWidgetItem(
                    f"{policy.get('self_term', '')}/{policy.get('address_term', '')}".strip("/")
                ),
                QTableWidgetItem(", ".join(str(item) for item in reasons) or str(row["review_question"] or "")),
            ]
            items[0].setData(Qt.ItemDataRole.UserRole, str(row["segment_id"]))
            for column_index, item in enumerate(items):
                self._review_table.setItem(row_index, column_index, item)
        if review_rows:
            self._review_table.selectRow(0)

    def _handle_review_selection_changed(self) -> None:
        if not self._current_workspace:
            return
        current_row = self._review_table.currentRow()
        if current_row < 0:
            return
        id_item = self._review_table.item(current_row, 0)
        if id_item is None:
            return
        segment_id = str(id_item.data(Qt.ItemDataRole.UserRole) or "")
        if not segment_id:
            return
        self._pending_review_segment_id = segment_id
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_row = database.get_segment_analysis(self._current_workspace.project_id, segment_id)
        if analysis_row is None:
            return
        speaker = self._analysis_json_field(analysis_row, "speaker_json", {})
        listeners = self._analysis_json_field(analysis_row, "listeners_json", [])
        policy = self._analysis_json_field(analysis_row, "honorific_policy_json", {})
        self._review_speaker_input.setText(str(speaker.get("character_id", "")))
        self._review_listener_input.setText(
            str((listeners[0] or {}).get("character_id", "")) if listeners else ""
        )
        self._review_self_term_input.setText(str(policy.get("self_term", "")))
        self._review_address_term_input.setText(str(policy.get("address_term", "")))
        self._review_subtitle_input.setText(str(analysis_row["approved_subtitle_text"] or ""))
        self._review_tts_input.setText(str(analysis_row["approved_tts_text"] or ""))

        review_row = next(
            (
                row
                for row in database.list_review_queue_items(self._current_workspace.project_id)
                if str(row["segment_id"]) == segment_id
            ),
            None,
        )
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        analysis_by_index = {int(row["segment_index"]): row for row in analysis_rows}
        segments = database.list_segments(self._current_workspace.project_id)
        context_lines: list[str] = []
        selected_index = int(analysis_row["segment_index"])
        for row in segments[max(0, selected_index - 3) : selected_index + 4]:
            row_index = int(row["segment_index"])
            analysis_context_row = analysis_by_index.get(row_index)
            prefix = ">>" if row_index == selected_index else "  "
            context_lines.append(f"{prefix} [{row_index + 1}] ZH: {row['source_text'] or ''}")
            subtitle_preview = ""
            tts_preview = ""
            if analysis_context_row is not None:
                subtitle_preview = str(analysis_context_row["approved_subtitle_text"] or "")
                tts_preview = str(analysis_context_row["approved_tts_text"] or "")
            else:
                subtitle_preview = str(row["subtitle_text"] or "")
                tts_preview = str(row["tts_text"] or "")
            if subtitle_preview or tts_preview:
                context_lines.append(f"{prefix}        VI-sub: {subtitle_preview}")
                context_lines.append(f"{prefix}        VI-tts: {tts_preview}")
        review_reason_codes = self._analysis_json_field(analysis_row, "review_reason_codes_json", [])
        review_question = str(analysis_row["review_question"] or "")
        scene_summary = str(review_row["short_scene_summary"] or "") if review_row is not None else ""
        self._review_context_text.setPlainText(
            "\n".join(
                [
                    f"Scene: {analysis_row['scene_id']}",
                    f"Tóm tắt scene: {self._review_table.item(current_row, 1).text() if self._review_table.item(current_row, 1) else ''}",
                    f"Lý do: {', '.join(str(item) for item in review_reason_codes)}",
                    f"Câu hỏi review: {review_question}",
                    "",
                    "Ngữ cảnh:",
                    *context_lines,
                ]
            )
        )
        self._review_context_text.setPlainText(
            "\n".join(
                [
                    f"Scene: {analysis_row['scene_id']}",
                    f"Tóm tắt scene: {scene_summary}",
                    f"Lý do: {', '.join(str(item) for item in review_reason_codes)}",
                    f"Câu hỏi review: {review_question}",
                    "",
                    "Ngữ cảnh:",
                    *context_lines,
                ]
            )
        )
        review_reason_text = ", ".join(str(item) for item in review_reason_codes) or "Không có"
        review_question_text = review_question or "Không có"
        self._review_context_text.setPlainText(
            "\n".join(
                [
                    f"Scene: {analysis_row['scene_id']}",
                    f"Tóm tắt scene: {scene_summary}",
                    f"Lý do review: {review_reason_text}",
                    f"Câu hỏi review: {review_question_text}",
                    "",
                    "Ngữ cảnh:",
                    *context_lines,
                ]
            )
        )

    def _ensure_contextual_semantic_ready(self, database: ProjectDatabase, *, dialog_title: str) -> bool:
        if not self._current_workspace:
            return False
        if self._current_translation_mode(database.get_project()) != "contextual_v2":
            return True
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        if not analysis_rows:
            QMessageBox.warning(
                self,
                dialog_title,
                "Chưa có kết quả Contextual V2. Hãy chạy dịch trước khi tiếp tục.",
            )
            return False
        pending_rows = [
            row
            for row in analysis_rows
            if bool(row["needs_human_review"]) or not bool(row["semantic_qc_passed"])
        ]
        if not pending_rows:
            return True
        self._reload_review_queue()
        QMessageBox.warning(
            self,
            dialog_title,
            (
                f"Không thể tiếp tục vì còn {len(pending_rows)} dòng chưa qua semantic review/QC.\n"
                "- Hãy xử lý các dòng trong bảng Review Ngữ Cảnh trước khi chạy TTS hoặc export."
            ),
        )
        return False

    def _selected_review_segment_ids(self) -> list[str]:
        segment_ids: list[str] = []
        selection_model = self._review_table.selectionModel()
        if selection_model is None:
            return segment_ids
        for index in selection_model.selectedRows():
            item = self._review_table.item(index.row(), 0)
            if item is None:
                continue
            segment_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
            if segment_id:
                segment_ids.append(segment_id)
        return segment_ids

    def _select_review_rows_by_scope(self, scope: str) -> None:
        if not self._current_workspace:
            return
        segment_id = self._pending_review_segment_id
        if not segment_id:
            QMessageBox.warning(self, "Review", "Hãy chọn một dòng review trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        try:
            target_ids = resolve_review_target_segment_ids(
                database,
                project_id=self._current_workspace.project_id,
                segment_id=segment_id,
                scope="project-relationship" if scope == "relation" else scope,
            )
        except ValueError as exc:
            QMessageBox.warning(self, "Review", str(exc))
            return
        if not target_ids:
            QMessageBox.information(self, "Review", "Không tìm thấy dòng nào phù hợp để chọn.")
            return
        target_set = set(target_ids)
        selection_model = self._review_table.selectionModel()
        if selection_model is None:
            return
        first_row = -1
        with QSignalBlocker(self._review_table):
            self._review_table.clearSelection()
            for row_index in range(self._review_table.rowCount()):
                item = self._review_table.item(row_index, 0)
                if item is None:
                    continue
                row_segment_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
                if row_segment_id not in target_set:
                    continue
                if first_row < 0:
                    first_row = row_index
                selection_model.select(
                    self._review_table.model().index(row_index, 0),
                    QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows,
                )
            if first_row >= 0:
                selection_model.setCurrentIndex(
                    self._review_table.model().index(first_row, 0),
                    QItemSelectionModel.SelectionFlag.NoUpdate,
                )
        self._handle_review_selection_changed()

    def _apply_review_resolution_to_selected_rows(self) -> None:
        selected_segment_ids = self._selected_review_segment_ids()
        if not selected_segment_ids:
            QMessageBox.warning(self, "Review", "Hãy chọn ít nhất một dòng review trước.")
            return
        self._run_review_resolution(scope="line", explicit_segment_ids=selected_segment_ids)

    def _run_review_resolution(self, *, scope: str, explicit_segment_ids: list[str] | None = None) -> None:
        if not self._current_workspace:
            return
        segment_id = self._pending_review_segment_id
        if not segment_id:
            QMessageBox.warning(self, "Review", "Hãy chọn một dòng review trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        project_row = database.get_project()
        if project_row is None:
            return
        if not self._create_backup_if_possible(
            stage="review_resolution",
            reason="Review resolution may overwrite canonical contextual outputs",
            dialog_title="Review",
        ):
            return
        try:
            updated_count = apply_review_resolution(
                database,
                project_id=self._current_workspace.project_id,
                segment_id=segment_id,
                speaker_id=self._review_speaker_input.text().strip() or "unknown",
                listener_id=self._review_listener_input.text().strip() or "unknown",
                self_term=self._review_self_term_input.text().strip(),
                address_term=self._review_address_term_input.text().strip(),
                subtitle_text=self._review_subtitle_input.text().strip(),
                tts_text=self._review_tts_input.text().strip(),
                scope=scope,
                target_language=str(project_row["target_language"] or "vi"),
                updated_at=utc_now_iso(),
                explicit_segment_ids=explicit_segment_ids,
            )
        except ValueError as exc:
            QMessageBox.warning(self, "Review", str(exc))
            return
        self._reload_subtitle_editor_from_db(force=True)
        self._reload_review_queue()
        self._refresh_workspace_views()
        resolution_label = "dòng chọn" if explicit_segment_ids else scope
        self._append_log_line(f"Đã áp review resolution cho {updated_count} dòng ({resolution_label})")

    def _apply_review_resolution(self, scope: str) -> None:
        self._run_review_resolution(scope=scope)
        return
        if not self._current_workspace:
            return
        segment_id = self._pending_review_segment_id
        if not segment_id:
            QMessageBox.warning(self, "Review", "Hãy chọn một dòng review trước.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_row = database.get_segment_analysis(self._current_workspace.project_id, segment_id)
        project_row = database.get_project()
        if analysis_row is None or project_row is None:
            return
        speaker_id = self._review_speaker_input.text().strip() or "unknown"
        listener_id = self._review_listener_input.text().strip() or "unknown"
        self_term = self._review_self_term_input.text().strip()
        address_term = self._review_address_term_input.text().strip()
        subtitle_text = self._review_subtitle_input.text().strip()
        tts_text = self._review_tts_input.text().strip()
        current_speaker = self._analysis_json_field(analysis_row, "speaker_json", {})
        current_listeners = self._analysis_json_field(analysis_row, "listeners_json", [])
        current_policy = self._analysis_json_field(analysis_row, "honorific_policy_json", {})
        updated_speaker = {**current_speaker, "character_id": speaker_id, "source": "manual", "confidence": 1.0}
        updated_listeners = (
            [
                {
                    **(current_listeners[0] if current_listeners else {}),
                    "character_id": listener_id,
                    "role": "primary",
                    "confidence": 1.0,
                }
            ]
            if listener_id
            else []
        )
        relationship_id = f"rel:{speaker_id}->{listener_id}"
        updated_policy = {
            **current_policy,
            "policy_id": relationship_id,
            "self_term": self_term,
            "address_term": address_term,
            "locked": True,
            "confidence": 1.0,
        }
        target_rows = [analysis_row]
        if scope != "line":
            all_rows = database.list_segment_analyses(self._current_workspace.project_id)
            selected_pair = (
                str(current_speaker.get("character_id", "unknown")),
                str((current_listeners[0] or {}).get("character_id", "unknown")) if current_listeners else "unknown",
            )
            if scope == "scene":
                target_rows = [
                    row
                    for row in all_rows
                    if str(row["scene_id"]) == str(analysis_row["scene_id"])
                    and (
                        str(self._analysis_json_field(row, "speaker_json", {}).get("character_id", "unknown")),
                        str(
                            (self._analysis_json_field(row, "listeners_json", [])[0] or {}).get(
                                "character_id", "unknown"
                            )
                        )
                        if self._analysis_json_field(row, "listeners_json", [])
                        else "unknown",
                    )
                    == selected_pair
                ]
            elif scope == "project-relationship":
                target_rows = [
                    row
                    for row in all_rows
                    if (
                        str(self._analysis_json_field(row, "speaker_json", {}).get("character_id", "unknown")),
                        str(
                            (self._analysis_json_field(row, "listeners_json", [])[0] or {}).get(
                                "character_id", "unknown"
                            )
                        )
                        if self._analysis_json_field(row, "listeners_json", [])
                        else "unknown",
                    )
                    == selected_pair
                ]
                existing_relationship_row = next(
                    (
                        row
                        for row in database.list_relationship_profiles(self._current_workspace.project_id)
                        if str(row["from_character_id"]) == speaker_id and str(row["to_character_id"]) == listener_id
                    ),
                    None,
                )
                database.upsert_relationship_profiles(
                    [
                        build_locked_relationship_record(
                            existing=(
                                relationship_record_from_row(
                                    existing_relationship_row,
                                    project_id=self._current_workspace.project_id,
                                )
                                if existing_relationship_row is not None
                                else None
                            ),
                            project_id=self._current_workspace.project_id,
                            relationship_id=relationship_id,
                            speaker_id=speaker_id,
                            listener_id=listener_id,
                            self_term=self_term,
                            address_term=address_term,
                            now=utc_now_iso(),
                        )
                    ]
                )
        for row in target_rows:
            database.update_segment_analysis_review(
                self._current_workspace.project_id,
                str(row["segment_id"]),
                speaker_json=updated_speaker,
                listeners_json=updated_listeners,
                honorific_policy_json=updated_policy,
                approved_subtitle_text=(
                    subtitle_text
                    if str(row["segment_id"]) == segment_id and subtitle_text
                    else str(row["approved_subtitle_text"] or "")
                ),
                approved_tts_text=(
                    tts_text
                    if str(row["segment_id"]) == segment_id and tts_text
                    else str(row["approved_tts_text"] or "")
                ),
                needs_human_review=False,
                review_status="locked" if scope != "line" else "approved",
                review_scope=scope,
                review_reason_codes_json=[],
                review_question="",
            )
        recompute_semantic_qc(
            database,
            project_id=self._current_workspace.project_id,
            target_language=str(project_row["target_language"] or "vi"),
        )
        self._reload_subtitle_editor_from_db(force=True)
        self._reload_review_queue()
        self._refresh_workspace_views()

    def _run_translation_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["translate"], dialog_title="Dich"):
            return None
        workspace = self._current_workspace
        database = ProjectDatabase(workspace.database_path)
        project_row = database.get_project()
        segments = database.list_segments(workspace.project_id)
        template = self._selected_prompt_template()
        if not project_row or not segments:
            QMessageBox.warning(self, "Chưa có phân đoạn", "Hãy chạy ASR trước khi dịch.")
            return None
        if not template:
            QMessageBox.warning(self, "Chưa có prompt", "Không tìm thấy prompt template trong dự án.")
            return None
        if not self._create_backup_if_possible(
            stage="translate",
            reason="Translation rerun may overwrite canonical translation state",
            dialog_title="Dich",
        ):
            return None

        source_language = segments[0]["source_lang"] or project_row["source_language"] or "auto"
        target_language = project_row["target_language"]
        model = self._translation_model_input.text().strip() or self._settings.default_translation_model
        translation_mode = self._current_translation_mode(project_row)
        if translation_mode == "contextual_v2":
            stage_hash = build_contextual_translation_stage_hash(
                segments=segments,
                template=template,
                project_root=workspace.root_dir,
                model=model,
                source_language=source_language,
                target_language=target_language,
            )
        else:
            stage_hash = build_translation_stage_hash(
                segments=segments,
                template=template,
                model=model,
                source_language=source_language,
                target_language=target_language,
            )
        settings = self._settings

        def handler(context: JobContext) -> JobResult:
            engine = OpenAITranslationEngine(settings)
            if translation_mode == "contextual_v2":
                cached_payload = load_cached_contextual_translation(workspace, stage_hash)
                if cached_payload:
                    cache_path = restore_cached_contextual_translation(
                        workspace,
                        database=database,
                        payload=cached_payload,
                        target_language=target_language,
                    )
                    qc_summary = recompute_semantic_qc(
                        database,
                        project_id=workspace.project_id,
                        target_language=target_language,
                    )
                    context.report_progress(100, "Dùng lại cache Contextual V2")
                    return JobResult(
                        message="Dùng cache Contextual V2",
                        output_paths=[cache_path],
                        extra={
                            "translated_count": len(database.list_segment_analyses(workspace.project_id)),
                            "cache_path": cache_path,
                            "translation_mode": translation_mode,
                            "pending_review_count": database.count_pending_segment_reviews(workspace.project_id),
                            "semantic_qc": qc_summary,
                        },
                    )
                contextual_result = run_contextual_translation(
                    context,
                    workspace=workspace,
                    database=database,
                    engine=engine,
                    segments=segments,
                    selected_template=template,
                    source_language=source_language,
                    target_language=target_language,
                    model=model,
                )
                cache_path = persist_contextual_translation_result(
                    workspace,
                    database=database,
                    stage_hash=stage_hash,
                    selected_template=template,
                    target_language=target_language,
                    scenes=contextual_result["scenes"],
                    character_profiles=contextual_result["character_profiles"],
                    relationship_profiles=contextual_result["relationship_profiles"],
                    analyses=contextual_result["segment_analyses"],
                )
                return JobResult(
                    message=f"Đã chạy Contextual V2 cho {len(contextual_result['segment_analyses'])} phân đoạn",
                    output_paths=[cache_path],
                    extra={
                        "translated_count": len(contextual_result["segment_analyses"]),
                        "cache_path": cache_path,
                        "translation_mode": translation_mode,
                        "pending_review_count": database.count_pending_segment_reviews(workspace.project_id),
                        "semantic_qc": contextual_result["semantic_qc"],
                    },
                )

            cached = load_cached_translations(workspace, stage_hash)
            if cached:
                database.apply_segment_translations(workspace.project_id, cached)
                context.report_progress(100, "Dùng lại cache bản dịch")
                cache_path = workspace.cache_dir / "translate" / stage_hash / "segments_translated.json"
                return JobResult(
                    message=f"Dùng cache bản dịch cho {len(cached)} phân đoạn",
                    output_paths=[cache_path],
                    extra={
                        "translated_count": len(cached),
                        "cache_path": cache_path,
                        "translation_mode": translation_mode,
                    },
                )

            translated_items = engine.translate_segments(
                context,
                segments=segments,
                template=template,
                source_language=source_language,
                target_language=target_language,
                model=model,
            )
            cache_path = persist_translations(
                workspace,
                translated_items=translated_items,
                stage_hash=stage_hash,
                template=template,
                model=model,
                source_language=source_language,
                target_language=target_language,
            )
            return JobResult(
                message=f"Đã dịch {len(translated_items)} phân đoạn",
                output_paths=[cache_path],
                extra={
                    "translated_count": len(translated_items),
                    "cache_path": cache_path,
                    "translation_mode": translation_mode,
                },
            )

        return self._job_manager.submit_job(
            stage="translate",
            description="Dịch phân đoạn bằng OpenAI Structured Outputs",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_tts_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["tts"], dialog_title="TTS"):
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "TTS", "Không thể lưu chỉnh sửa phụ đề trước khi chạy TTS.")
            return None
        workspace = self._current_workspace
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy preset giọng trong dự án.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR và dịch trước.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="tts",
            dialog_title="TTS",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="TTS"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="TTS",
            warn_on_unresolved=True,
        )
        if preset is None:
            return None
        if not self._create_backup_if_possible(
            stage="tts",
            reason="TTS rerun may overwrite active voice artifacts",
            dialog_title="TTS",
        ):
            return None
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
            segment_voice_presets=segment_voice_presets,
        )

        def handler(context: JobContext) -> JobResult:
            cached = load_synthesized_segments(workspace, stage_hash)
            if cached and all(item.raw_wav_path.exists() for item in cached.artifacts):
                database.apply_subtitle_event_audio_paths(
                    workspace.project_id,
                    str(active_track["track_id"]),
                    [
                        {
                            "segment_id": item.segment_id,
                            "audio_path": str(item.raw_wav_path),
                            "status": "tts_ready",
                        }
                        for item in cached.artifacts
                    ],
                )
                context.report_progress(100, "Dùng lại cache clip TTS")
                return JobResult(
                    message=f"Dùng cache TTS cho {len(cached.artifacts)} phân đoạn",
                    output_paths=[cached.manifest_path],
                    extra={
                        "manifest_path": cached.manifest_path,
                        "artifact_count": len(cached.artifacts),
                        "voice_engine": preset.engine.lower(),
                    },
                )

            synthesized = synthesize_segments(
                context,
                workspace=workspace,
                segments=subtitle_rows,
                preset=preset,
                engine=create_tts_engine(preset, project_root=workspace.root_dir),
                allow_source_fallback=not require_localized,
                segment_voice_presets=segment_voice_presets,
                segment_speaker_keys=segment_speaker_keys,
            )
            database.apply_subtitle_event_audio_paths(
                workspace.project_id,
                str(active_track["track_id"]),
                [
                    {
                        "segment_id": item.segment_id,
                        "audio_path": str(item.raw_wav_path),
                        "status": "tts_ready",
                    }
                    for item in synthesized.artifacts
                ],
            )
            return JobResult(
                message=f"Đã tạo {len(synthesized.artifacts)} clip TTS",
                output_paths=[synthesized.manifest_path],
                extra={
                    "manifest_path": synthesized.manifest_path,
                    "artifact_count": len(synthesized.artifacts),
                    "voice_engine": preset.engine.lower(),
                },
            )

        return self._job_manager.submit_job(
            stage="tts",
            description="Tạo clip TTS từ nội dung phụ đề hoặc lời đọc",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_build_voice_track_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["voice_track"], dialog_title="Track giong"):
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Track giọng", "Không thể lưu chỉnh sửa phụ đề trước khi tạo track giọng.")
            return None
        workspace = self._current_workspace
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy preset giọng trong dự án.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR và dịch trước.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="tts",
            dialog_title="Track giọng",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Track giọng"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Track giọng",
            warn_on_unresolved=True,
        )
        if preset is None:
            return None
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
            segment_voice_presets=segment_voice_presets,
        )
        cached = load_synthesized_segments(workspace, stage_hash)
        if not cached:
            QMessageBox.warning(self, "TTS", "Hãy chạy TTS trước khi tạo track giọng.")
            return None
        missing_artifact_indexes = self._missing_tts_artifact_row_indexes(
            subtitle_rows,
            cached.artifacts,
            require_localized=require_localized,
        )
        if missing_artifact_indexes:
            self._focus_subtitle_table_row(missing_artifact_indexes[0])
            QMessageBox.warning(
                self,
                "Track giọng",
                (
                    "Cache TTS hiện tại chưa đủ cho toàn bộ dòng cần đọc.\n"
                    f"- Dòng thiếu clip: {self._format_row_number_list(missing_artifact_indexes)}\n"
                    "- Hãy chạy lại TTS sau khi hoàn tất nội dung tiếng đích."
                ),
            )
            return None

        video_row = database.get_primary_video_asset(workspace.project_id)
        total_duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else max(
            int(row["end_ms"]) for row in subtitle_rows
        )
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path

        def handler(context: JobContext) -> JobResult:
            result = build_voice_track(
                context,
                workspace=workspace,
                artifacts=cached.artifacts,
                ffmpeg_path=ffmpeg_path,
                total_duration_ms=total_duration_ms,
            )
            database.apply_subtitle_event_audio_paths(
                workspace.project_id,
                str(active_track["track_id"]),
                [
                    {
                        "segment_id": item.segment_id,
                        "audio_path": str(item.fitted_wav_path or item.raw_wav_path),
                        "status": "voice_ready",
                    }
                    for item in result.fitted_clips
                ],
            )
            return JobResult(
                message="Đã tạo track giọng",
                output_paths=[result.manifest_path, result.voice_track_path],
                extra={
                    "voice_track_path": result.voice_track_path,
                    "manifest_path": result.manifest_path,
                    "fitted_count": len(result.fitted_clips),
                },
            )

        return self._job_manager.submit_job(
            stage="voice_track",
            description="Căn chỉnh clip TTS theo timeline và tạo track giọng",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_mixdown_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["mixdown"], dialog_title="Tron am thanh"):
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Trộn âm thanh", "Không thể lưu chỉnh sửa phụ đề trước khi trộn âm thanh.")
            return None
        workspace = self._current_workspace
        artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
        if not artifacts or not artifacts.audio_48k_path.exists():
            QMessageBox.warning(self, "Chưa có audio 48 kHz", "Hãy tách âm thanh trước khi trộn.")
            return None
        database, _active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR và dịch trước.")
            return None
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giọng", "Không tìm thấy preset giọng trong dự án.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="tts",
            dialog_title="Trộn âm thanh",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Trộn âm thanh"):
            return None
        if not self._last_voice_track_output or not self._last_voice_track_output.exists():
            QMessageBox.warning(self, "Chưa có track giọng", "Hãy tạo track giọng trước khi trộn âm thanh.")
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Trộn âm thanh",
            warn_on_unresolved=True,
        )
        if preset is None:
            return None
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
            segment_voice_presets=segment_voice_presets,
        )
        cached = load_synthesized_segments(workspace, stage_hash)
        if not cached:
            QMessageBox.warning(self, "Trộn âm thanh", "Không tìm thấy cache TTS hiện tại. Hãy chạy lại TTS.")
            return None
        missing_artifact_indexes = self._missing_tts_artifact_row_indexes(
            subtitle_rows,
            cached.artifacts,
            require_localized=require_localized,
        )
        if missing_artifact_indexes:
            self._focus_subtitle_table_row(missing_artifact_indexes[0])
            QMessageBox.warning(
                self,
                "Trộn âm thanh",
                (
                    "Track giọng hiện tại không còn khớp với nội dung TTS.\n"
                    f"- Dòng thiếu clip: {self._format_row_number_list(missing_artifact_indexes)}\n"
                    "- Hãy chạy lại TTS rồi tạo lại track giọng trước khi trộn."
                ),
            )
            return None
        video_row = database.get_primary_video_asset(workspace.project_id)
        total_duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else max(
            int(row["end_ms"]) for row in subtitle_rows
        )
        expected_voice_track_stage_hash = build_voice_track_stage_hash(
            cached.artifacts,
            total_duration_ms=total_duration_ms,
        )
        expected_voice_track_path = workspace.cache_dir / "mix" / expected_voice_track_stage_hash / "voice_track.wav"
        if expected_voice_track_path.resolve() != self._last_voice_track_output.resolve():
            QMessageBox.warning(
                self,
                "Trộn âm thanh",
                (
                    "Track giọng hiện tại không còn đồng bộ với phụ đề hoặc preset giọng.\n"
                    "- Hãy tạo lại track giọng trước khi trộn âm thanh."
                ),
            )
            return None
        mixdown_inputs = self._current_mixdown_inputs_or_warn(dialog_title="Trộn âm thanh")
        if mixdown_inputs is None:
            return None
        original_volume, voice_volume, bgm_path, bgm_volume = mixdown_inputs
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path

        def handler(context: JobContext) -> JobResult:
            result = mix_audio_tracks(
                context,
                workspace=workspace,
                original_audio_path=artifacts.audio_48k_path,
                voice_track_path=self._last_voice_track_output,
                ffmpeg_path=ffmpeg_path,
                original_volume=original_volume,
                voice_volume=voice_volume,
                bgm_path=bgm_path,
                bgm_volume=bgm_volume,
            )
            return JobResult(
                message="Đã trộn âm thanh",
                output_paths=[result.manifest_path, result.mixed_audio_path],
                extra={"mixed_audio_path": result.mixed_audio_path, "manifest_path": result.manifest_path},
            )

        return self._job_manager.submit_job(
            stage="mixdown",
            description="Trộn track giọng với âm thanh gốc và BGM tùy chọn",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_export_subtitles_job(self, format_name: str) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Biên tập phụ đề", "Không thể lưu chỉnh sửa phụ đề trước khi xuất file.")
            return None
        workspace = self._current_workspace
        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR hoặc dịch trước.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="subtitle",
            dialog_title=f"Xuất {format_name.upper()}",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(
            database,
            dialog_title=f"Xuất {format_name.upper()}",
        ):
            return None
        if not self._ensure_qc_passed_for_export(subtitle_rows, dialog_title=f"Xuất {format_name.upper()}"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)

        def handler(context: JobContext) -> JobResult:
            context.report_progress(
                10,
                f"Đang tạo {format_name.upper()} từ {self._subtitle_track_label(active_track)}",
            )
            output_path = export_subtitles(
                workspace,
                segments=subtitle_rows,
                format_name=format_name,
                allow_source_fallback=not require_localized,
                subtitle_subtext_mode=self._current_subtitle_subtext_mode(),
            )
            context.report_progress(100, f"Đã tạo {format_name.upper()}")
            return JobResult(
                message=f"Đã xuất {format_name.upper()}",
                output_paths=[output_path],
                extra={"format_name": format_name, "output_path": output_path},
            )

        return self._job_manager.submit_job(
            stage=f"export_{format_name}",
            description=f"Xuất phụ đề {format_name.upper()}",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_video_export_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return None
        if not self._ensure_doctor_ready(stages=["export_video"], dialog_title="Xuat video"):
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Biên tập phụ đề", "Không thể lưu chỉnh sửa phụ đề trước khi xuất video.")
            return None

        workspace = self._current_workspace
        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "Chưa có video", "Hãy chọn video nguồn hợp lệ.")
            return None
        try:
            export_preset = self._selected_export_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Preset xuất", str(exc))
            return None
        if not export_preset:
            QMessageBox.warning(self, "Preset xuất", "Không tìm thấy preset xuất trong dự án.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "Chưa có track phụ đề", "Hãy chạy ASR và dịch trước.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="subtitle",
            dialog_title="Xuất video",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Xuất video"):
            return None
        if not self._ensure_qc_passed_for_export(subtitle_rows, dialog_title="Xuất video"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Xuất video",
            warn_on_unresolved=True,
        )
        if preset is None:
            return None
        if not self._create_backup_if_possible(
            stage="export_video",
            reason="Export rerun may overwrite downstream artifacts",
            dialog_title="Xuat video",
        ):
            return None

        video_row = database.get_primary_video_asset(workspace.project_id)
        duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else None
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path
        replacement_audio_path: Path | None = None
        if self._last_voice_track_output and self._last_voice_track_output.exists():
            if not preset:
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    "Không tìm thấy preset giọng hiện tại. Hãy chọn lại preset rồi tạo lại track giọng trước khi xuất video.",
                )
                return None
            total_duration_ms = duration_ms or max(int(row["end_ms"]) for row in subtitle_rows)
            expected_voice_track_path, missing_tts_indexes = self._expected_voice_track_path_for_rows(
                workspace=workspace,
                subtitle_rows=subtitle_rows,
                preset=preset,
                total_duration_ms=total_duration_ms,
                require_localized=require_localized,
                segment_voice_presets=segment_voice_presets,
            )
            if missing_tts_indexes:
                self._focus_subtitle_table_row(missing_tts_indexes[0])
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    (
                        "Track giọng hiện tại không còn khớp với track phụ đề đang active.\n"
                        f"- Dòng thiếu clip TTS: {self._format_row_number_list(missing_tts_indexes)}\n"
                        "- Hãy chạy lại TTS, tạo track giọng và trộn âm thanh trước khi xuất video."
                    ),
                )
                return None
            if expected_voice_track_path is None or expected_voice_track_path.resolve() != self._last_voice_track_output.resolve():
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    (
                        "Track giọng hiện tại không còn đồng bộ với phụ đề hoặc preset giọng.\n"
                        "- Hãy tạo lại track giọng rồi trộn âm thanh trước khi xuất video."
                    ),
                )
                return None
            artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
            if not artifacts or not artifacts.audio_48k_path.exists():
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    "Đã có track giọng nhưng chưa có audio 48 kHz để trộn. Hãy chạy Chuẩn bị media rồi trộn âm thanh trước.",
                )
                return None
            mixdown_inputs = self._current_mixdown_inputs_or_warn(dialog_title="Xuất video")
            if mixdown_inputs is None:
                return None
            original_volume, voice_volume, bgm_path, bgm_volume = mixdown_inputs
            expected_mixed_audio_path = self._expected_mixed_audio_path(
                workspace,
                original_audio_path=artifacts.audio_48k_path,
                voice_track_path=self._last_voice_track_output,
                original_volume=original_volume,
                voice_volume=voice_volume,
                bgm_path=bgm_path,
                bgm_volume=bgm_volume,
            )
            if not self._last_mixed_audio_output or not self._last_mixed_audio_output.exists():
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    "Đã có track giọng nhưng chưa có âm thanh đã trộn. Hãy chạy Trộn âm thanh trước khi xuất video.",
                )
                return None
            if expected_mixed_audio_path.resolve() != self._last_mixed_audio_output.resolve():
                QMessageBox.warning(
                    self,
                    "Xuất video",
                    (
                        "Âm thanh đã trộn hiện tại không còn đồng bộ với track giọng, mức âm lượng hoặc BGM.\n"
                        "- Hãy chạy lại Trộn âm thanh trước khi xuất video."
                    ),
                )
                return None
            replacement_audio_path = self._last_mixed_audio_output
        subtitle_format = "ass" if export_preset.burn_subtitles else "srt"
        export_mode_label = "hard-sub" if export_preset.burn_subtitles else "soft-sub"
        watermark_profile = self._selected_watermark_profile(strict=False)

        def handler(context: JobContext) -> JobResult:
            context.report_progress(
                5,
                f"Đang tạo {subtitle_format.upper()} cho {self._subtitle_track_label(active_track)}",
            )
            subtitle_path = export_subtitles(
                workspace,
                segments=subtitle_rows,
                format_name=subtitle_format,
                allow_source_fallback=not require_localized,
                subtitle_subtext_mode=self._current_subtitle_subtext_mode(),
            )
            output_path = export_hardsub_video(
                context,
                workspace=workspace,
                source_video_path=source_video_path,
                subtitle_path=subtitle_path,
                ffmpeg_path=ffmpeg_path,
                duration_ms=duration_ms,
                replacement_audio_path=replacement_audio_path,
                export_preset=export_preset,
                export_preset_id=export_preset.export_preset_id,
            )
            return JobResult(
                message=f"Đã xuất video {export_mode_label}",
                output_paths=[subtitle_path, output_path],
                extra={
                    "subtitle_path": subtitle_path,
                    "output_path": output_path,
                    "replacement_audio_path": replacement_audio_path,
                    "export_preset_id": export_preset.export_preset_id,
                    "watermark_path": export_preset.watermark_path,
                    "watermark_profile_id": (
                        watermark_profile.watermark_profile_id if watermark_profile else None
                    ),
                    "burn_subtitles": export_preset.burn_subtitles,
                },
            )

        return self._job_manager.submit_job(
            stage="export_video",
            description=(
                "Ghi cứng ASS vào video nguồn bằng FFmpeg"
                if export_preset.burn_subtitles
                else "Gắn track phụ đề vào video nguồn bằng FFmpeg"
            ),
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_hardsub_export_job(self) -> None:
        self._run_video_export_job()

    def _run_smoke_job(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
            return

        def smoke_handler(context: JobContext) -> JobResult:
            for progress in range(0, 101, 10):
                context.cancellation_token.raise_if_canceled()
                context.report_progress(progress, f"Kiểm tra tiến trình {progress}%")
                context.sleep_with_cancel(0.15)
            return JobResult(message="Tác vụ kiểm tra đã hoàn tất")

        self._job_manager.submit_job(
            stage="smoke",
            description="Kiểm tra hàng đợi tác vụ, tiến trình và thao tác hủy",
            handler=smoke_handler,
            project_id=self._current_workspace.project_id,
            project_db_path=self._current_workspace.database_path,
        )

    def _handle_job_updated(self, state: object) -> None:
        self._status_panel.upsert_job(state)
        self._append_log_line(
            f"[{getattr(state, 'status')}] {getattr(state, 'stage')} "
            f"{getattr(state, 'job_id')} - {getattr(state, 'message')}"
        )
        status = getattr(state, "status")
        if status == JobStatus.SUCCESS.value:
            self._apply_success_state(state)
            self._advance_workflow_on_success(
                getattr(state, "job_id"),
                getattr(state, "stage"),
            )
        elif status in {JobStatus.FAILED.value, JobStatus.CANCELED.value}:
            self._handle_workflow_interruption(
                getattr(state, "job_id"),
                getattr(state, "stage"),
                status=status,
                message=getattr(state, "message", ""),
            )

    def _handle_workflow_interruption(
        self,
        job_id: str,
        stage: str,
        *,
        status: str,
        message: str,
    ) -> None:
        if not self._workflow_current_job_id or job_id != self._workflow_current_job_id:
            return
        if self._workflow_current_stage != stage:
            return
        status_label = "thất bại" if status == JobStatus.FAILED.value else "đã bị hủy"
        self._stop_workflow(
            message=(
                f"Quy trình nhanh dừng lại: bước {self._workflow_stage_label(stage)} {status_label}.\n"
                f"- Thông báo: {message or '-'}"
            ),
        )

    def _handle_retry_requested(self, job_id: str) -> None:
        new_job_id = self._job_manager.retry_job(job_id)
        if not new_job_id:
            QMessageBox.warning(self, "Không thể chạy lại", "Không tìm thấy job gốc.")
            return
        self._append_log_line(f"Retry job {job_id} -> {new_job_id}")

    def _apply_success_state(self, state: object) -> None:
        if self._current_workspace and getattr(state, "project_id", None) not in {
            None,
            self._current_workspace.project_id,
        }:
            return
        stage = getattr(state, "stage")
        extra = getattr(state, "extra", {}) or {}
        if stage == "probe_media" and extra.get("metadata"):
            self._media_metadata = extra["metadata"]
            if self._current_workspace:
                self._current_workspace.source_video_path = self._media_metadata.source_path
            self._refresh_workspace_views()
        elif stage == "extract_audio" and extra.get("artifacts"):
            self._audio_artifacts = extra["artifacts"]
            if extra.get("metadata"):
                self._media_metadata = extra["metadata"]
            self._refresh_workspace_views()
        elif stage == "asr" and self._current_workspace:
            persisted = extra.get("persisted")
            if persisted:
                self._asr_summary.setText(
                    "ASR hoàn tất:\n"
                    f"- Số phân đoạn: {persisted.segment_count}\n"
                    f"- Cache: {persisted.cache_dir}\n"
                    f"- JSON: {persisted.segments_json_path}"
                )
            else:
                database = ProjectDatabase(self._current_workspace.database_path)
                self._asr_summary.setText(
                    f"ASR hoàn tất, số phân đoạn trong CSDL: {database.count_segments(self._current_workspace.project_id)}"
                )
            self._reload_subtitle_editor_from_db(force=True)
        elif stage == "translate" and self._current_workspace:
            translated_count = extra.get("translated_count", 0)
            cache_path = extra.get("cache_path")
            translation_mode = extra.get("translation_mode", self._current_translation_mode())
            pending_review_count = extra.get("pending_review_count")
            semantic_qc = extra.get("semantic_qc") or {}
            summary_lines = [
                "Dịch hoàn tất:",
                f"- Chế độ: {translation_mode}",
                f"- Số dòng đã dịch: {translated_count}",
                f"- Cache: {cache_path}",
            ]
            if pending_review_count is not None:
                summary_lines.append(f"- Dòng cần review: {pending_review_count}")
            if semantic_qc:
                summary_lines.append(
                    f"- Semantic QC: {semantic_qc.get('error_count', 0)} lỗi, {semantic_qc.get('warning_count', 0)} cảnh báo"
                )
            self._translation_summary.setText(
                "\n".join(summary_lines)
            )
            self._reload_subtitle_editor_from_db(force=True)
            self._reload_review_queue()
        elif stage in {"export_srt", "export_ass"}:
            output_path = extra.get("output_path")
            format_name = extra.get("format_name")
            if output_path and format_name:
                self._last_subtitle_outputs[format_name] = output_path
            self._refresh_workspace_views()
        elif stage == "tts":
            manifest_path = extra.get("manifest_path")
            if manifest_path:
                self._last_tts_manifest = manifest_path
            self._reload_subtitle_editor_from_db(force=True)
            self._refresh_workspace_views()
        elif stage == "voice_track":
            voice_track_path = extra.get("voice_track_path")
            if voice_track_path:
                self._last_voice_track_output = voice_track_path
            self._reload_subtitle_editor_from_db(force=True)
            self._refresh_workspace_views()
        elif stage == "mixdown":
            mixed_audio_path = extra.get("mixed_audio_path")
            if mixed_audio_path:
                self._last_mixed_audio_output = mixed_audio_path
            self._refresh_workspace_views()
        elif stage in {"export_hardsub", "export_video"}:
            output_path = extra.get("output_path")
            if output_path:
                self._last_export_output = output_path
            self._refresh_workspace_views()

    def _refresh_workspace_views(self) -> None:
        if not self._current_workspace:
            return

        database = ProjectDatabase(self._current_workspace.database_path)
        project_row = database.get_project()
        backup_root = get_backups_root(self._current_workspace.root_dir)
        backup_count = len([path for path in backup_root.iterdir() if path.is_dir()]) if backup_root.exists() else 0
        self._project_summary.setText(
            "Dự án hiện tại:\n"
            f"- Tên: {self._current_workspace.name}\n"
            f"- Thư mục: {self._current_workspace.root_dir}\n"
            f"- CSDL: {self._current_workspace.database_path}\n"
            f"- Cache: {self._current_workspace.cache_dir}\n"
            f"- Exports: {self._current_workspace.exports_dir}\n"
            f"- Ops: {self._current_workspace.root_dir / '.ops'}\n"
            f"- Backup folders: {backup_count}"
        )
        self._reload_speaker_bindings()
        self._reload_voice_policies()
        self._refresh_ops_views(database)
        video_row = database.get_primary_video_asset(self._current_workspace.project_id)
        if video_row:
            self._media_summary.setText(
                "Video nguồn:\n"
                f"- Đường dẫn: {video_row['path']}\n"
                f"- Thời lượng: {video_row['duration_ms']} ms\n"
                f"- Độ phân giải: {video_row['width']}x{video_row['height']}\n"
                f"- FPS: {video_row['fps']}\n"
                f"- Âm thanh: {video_row['audio_channels']} kênh @ {video_row['sample_rate']} Hz"
            )
        else:
            self._media_summary.setText("Chưa có video nguồn")

        segment_rows = database.list_segments(self._current_workspace.project_id)
        segment_count = len(segment_rows)
        translated_count = sum(1 for row in segment_rows if row["status"] == "translated")
        active_track = database.get_active_subtitle_track(self._current_workspace.project_id)
        if active_track is None:
            active_track = database.ensure_canonical_subtitle_track(self._current_workspace.project_id)
            sync_project_snapshot(self._current_workspace)
        subtitle_rows = (
            database.list_subtitle_events(
                self._current_workspace.project_id,
                track_id=str(active_track["track_id"]),
            )
            if active_track is not None
            else []
        )
        require_localized = self._requires_localized_output(database, subtitle_rows)
        subtitle_ready_count = sum(
            1
            for row in subtitle_rows
            if self._subtitle_output_text(row, require_localized=require_localized)
        )
        tts_text_ready_count = sum(
            1
            for row in subtitle_rows
            if self._tts_output_text(row, require_localized=require_localized)
        )
        qc_report = (
            analyze_subtitle_rows(self._normalize_rows_for_qc(subtitle_rows), config=SubtitleQcConfig())
            if subtitle_rows
            else SubtitleQcReport(total_segments=0, issues=[])
        )
        if self._audio_artifacts:
            self._asr_summary.setText(
                "Bộ nhớ đệm âm thanh sẵn sàng:\n"
                f"- ASR 16 kHz: {self._audio_artifacts.audio_16k_path}\n"
                f"- Mix 48 kHz: {self._audio_artifacts.audio_48k_path}\n"
                f"- Phân đoạn trong CSDL: {segment_count}"
            )
        else:
            self._asr_summary.setText(f"Chưa có cache âm thanh. Phân đoạn trong CSDL: {segment_count}")
        target_language = project_row["target_language"] if project_row else "-"
        translation_mode = self._current_translation_mode(project_row)
        pending_review_count = (
            database.count_pending_segment_reviews(self._current_workspace.project_id)
            if translation_mode == "contextual_v2"
            else 0
        )
        self._translation_summary.setText(
            "Trạng thái dịch:\n"
            f"- Chế độ: {translation_mode}\n"
            f"- Ngôn ngữ đích: {target_language}\n"
            f"- Mẫu prompt: {len(self._prompt_templates)}\n"
            f"- Đã dịch: {translated_count}/{segment_count}\n"
            f"- Dòng cần review: {pending_review_count}"
        )
        if translation_mode == "contextual_v2":
            self._review_summary.setText(
                f"Hàng review semantic: {pending_review_count} dòng cần duyệt trước TTS/export"
            )
        else:
            self._review_summary.setText("Dự án này đang dùng chế độ dịch legacy")

        current_preset = self._selected_voice_preset()
        resolved_preset = current_preset
        segment_voice_presets: dict[str, object] | None = None
        speaker_binding_lines: list[str] = []
        voice_binding_ready = True
        voice_plan = None
        if database and subtitle_rows:
            resolved_preset, segment_voice_presets, _segment_speaker_keys, voice_plan = self._resolve_tts_voice_plan(
                database,
                subtitle_rows,
                require_localized=require_localized,
                dialog_title="Lồng tiếng",
                warn_on_unresolved=False,
            )
            if voice_plan is not None:
                if getattr(voice_plan, "active_bindings", False):
                    blocked_lines = self._voice_plan_block_lines(voice_plan)
                    if blocked_lines:
                        voice_binding_ready = False
                        speaker_binding_lines.extend(f"- {line}" for line in blocked_lines)
                    else:
                        speaker_binding_lines.append(
                            f"- Speaker binding: đã gán theo speaker cho {len(getattr(voice_plan, 'segment_voice_preset_ids', {}))} dòng"
                        )
                else:
                    speaker_binding_lines.append(
                        "- Speaker binding: chưa bật, toàn bộ sẽ dùng preset mặc định"
                    )
        if database and subtitle_rows and voice_plan is not None:
            if getattr(voice_plan, "active_voice_policies", False) or getattr(
                voice_plan,
                "active_register_voice_styles",
                False,
            ):
                relationship_hits = int(getattr(voice_plan, "relationship_policy_hits", 0))
                character_hits = int(getattr(voice_plan, "character_policy_hits", 0))
                relationship_style_hits = int(getattr(voice_plan, "relationship_style_hits", 0))
                character_style_hits = int(getattr(voice_plan, "character_style_hits", 0))
                register_style_hits = int(getattr(voice_plan, "register_style_hits", 0))
                if relationship_hits or character_hits:
                    speaker_binding_lines.append(
                        f"- Voice policy: relationship={relationship_hits} dòng, character={character_hits} dòng"
                    )
                if relationship_style_hits or character_style_hits or register_style_hits:
                    speaker_binding_lines.append(
                        f"- Voice style: relationship={relationship_style_hits} dòng, character={character_style_hits} dòng, register={register_style_hits} dòng"
                    )
                if not (
                    relationship_hits
                    or character_hits
                    or relationship_style_hits
                    or character_style_hits
                    or register_style_hits
                ):
                    speaker_binding_lines.append(
                        "- Voice policy/style: đã bật nhưng chưa khớp dòng nào; runtime sẽ rơi về speaker binding hoặc preset mặc định"
                    )
            else:
                speaker_binding_lines.append("- Voice policy/style: chưa bật")
        self._refresh_effective_voice_plan_preview(
            subtitle_rows=subtitle_rows,
            require_localized=require_localized,
            default_preset=resolved_preset,
            segment_voice_presets=segment_voice_presets,
            voice_plan=voice_plan,
        )
        tts_ready_count = sum(1 for row in subtitle_rows if row["audio_path"])
        total_duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else (
            max((int(row["end_ms"]) for row in subtitle_rows), default=0)
        )
        expected_voice_track_path = None
        voice_track_ready = False
        if resolved_preset and subtitle_rows and total_duration_ms > 0:
            expected_voice_track_path, missing_tts_indexes = self._expected_voice_track_path_for_rows(
                workspace=self._current_workspace,
                subtitle_rows=subtitle_rows,
                preset=resolved_preset,
                total_duration_ms=total_duration_ms,
                require_localized=require_localized,
                segment_voice_presets=segment_voice_presets,
            )
            voice_track_ready = bool(
                expected_voice_track_path
                and not missing_tts_indexes
                and self._last_voice_track_output
                and self._last_voice_track_output.exists()
                and expected_voice_track_path.resolve() == self._last_voice_track_output.resolve()
            )
            if not voice_binding_ready:
                voice_track_ready = False
        voice_lines = [
            "Lồng tiếng:",
            f"- Số preset giọng: {len(self._voice_presets)}",
            f"- Track phụ đề đang dùng: {self._subtitle_track_label(active_track)}",
            f"- Nội dung sẵn sàng để lồng tiếng: {tts_text_ready_count}/{len(subtitle_rows)}",
            f"- Dòng đã có audio TTS: {tts_ready_count}/{len(subtitle_rows)}",
            f"- Preset đang chọn: {current_preset.name if current_preset else '-'}",
        ]
        if self._installed_sapi_voices:
            voice_lines.append(f"- Giọng SAPI phát hiện: {len(self._installed_sapi_voices)}")
        version_suffix = f" v{self._vieneu_environment.package_version}" if self._vieneu_environment.package_version else ""
        if self._vieneu_environment.package_installed:
            voice_lines.append(f"- VieNeu SDK{version_suffix}: đã cài")
            if self._vieneu_environment.espeak_path:
                voice_lines.append(f"- eSpeak NG: {self._vieneu_environment.espeak_path}")
            else:
                voice_lines.append("- eSpeak NG: chưa tìm thấy cho VieNeu local")
        else:
            voice_lines.append("- VieNeu SDK: chưa cài")
        voice_lines.extend(speaker_binding_lines)
        if not voice_binding_ready:
            voice_lines.append("- Trạng thái voice plan: blocked, chưa an toàn để chạy TTS hoặc xuất video")
        if current_preset and current_preset.engine.lower() == "vieneu":
            try:
                voice_lines.append(f"- Chế độ VieNeu: {get_vieneu_mode(current_preset)}")
            except ValueError as exc:
                voice_lines.append(f"- Cấu hình VieNeu lỗi: {exc}")
            ref_audio_path = str(current_preset.engine_options.get("ref_audio_path", "")).strip()
            ref_text = str(current_preset.engine_options.get("ref_text", "")).strip()
            if ref_audio_path:
                voice_lines.append(f"- Audio mẫu clone: {ref_audio_path}")
            if ref_text:
                preview = ref_text if len(ref_text) <= 72 else ref_text[:69] + "..."
                voice_lines.append(f"- Văn bản mẫu clone: {preview}")
        if self._last_tts_manifest:
            voice_lines.append(f"- Manifest TTS: {self._last_tts_manifest}")
        if self._last_voice_track_output:
            voice_lines.append(f"- Track giọng: {self._last_voice_track_output}")
        voice_lines.append(
            "- Đồng bộ track giọng: "
            + (
                "Có"
                if voice_track_ready
                else "Cần tạo lại"
                if self._last_voice_track_output
                else "Chưa có"
            )
        )
        self._voice_summary.setText("\n".join(voice_lines))

        mix_lines = ["Trộn âm thanh:"]
        if self._audio_artifacts:
            mix_lines.append(f"- Audio gốc 48 kHz: {self._audio_artifacts.audio_48k_path}")
        else:
            mix_lines.append("- Audio gốc 48 kHz: chưa có")
        mix_lines.append(f"- BGM: {self._resolve_bgm_path() or '-'}")
        mix_lines.append(
            f"- Âm lượng: gốc={self._original_volume_input.text()} "
            f"giọng={self._voice_volume_input.text()} bgm={self._bgm_volume_input.text()}"
        )
        mixed_audio_ready = False
        if voice_track_ready and self._last_voice_track_output and self._audio_artifacts:
            mixdown_inputs = self._current_mixdown_inputs()
            if mixdown_inputs is None:
                mix_lines.append("- Trạng thái: thông số mix chưa hợp lệ")
            else:
                original_volume, voice_volume, bgm_path, bgm_volume = mixdown_inputs
                expected_mixed_audio_path = self._expected_mixed_audio_path(
                    self._current_workspace,
                    original_audio_path=self._audio_artifacts.audio_48k_path,
                    voice_track_path=self._last_voice_track_output,
                    original_volume=original_volume,
                    voice_volume=voice_volume,
                    bgm_path=bgm_path,
                    bgm_volume=bgm_volume,
                )
                mixed_audio_ready = bool(
                    self._last_mixed_audio_output
                    and self._last_mixed_audio_output.exists()
                    and expected_mixed_audio_path.resolve() == self._last_mixed_audio_output.resolve()
                )
                mix_lines.append(
                    f"- Trạng thái: {'Đồng bộ với track giọng hiện tại' if mixed_audio_ready else 'Cần trộn lại'}"
                )
        elif self._last_voice_track_output and self._last_voice_track_output.exists():
            if self._audio_artifacts:
                mix_lines.append("- Trạng thái: track giọng hiện tại đã cũ, hãy tạo lại trước khi trộn")
            else:
                mix_lines.append("- Trạng thái: thiếu audio 48 kHz để trộn")
        if self._last_mixed_audio_output:
            mix_lines.append(f"- Âm thanh đã trộn: {self._last_mixed_audio_output}")
        self._mix_summary.setText("\n".join(mix_lines))

        subtitle_lines = [
            "Biên tập phụ đề:",
            f"- Track đang dùng: {self._subtitle_track_label(active_track)}",
            f"- Dòng sẵn sàng để xuất: {subtitle_ready_count}/{len(subtitle_rows)}",
            f"- QC: {qc_report.error_count} lỗi, {qc_report.warning_count} cảnh báo",
            f"- Có thay đổi chưa lưu: {'Có' if self._subtitle_editor_dirty else 'Không'}",
        ]
        if self._last_subtitle_outputs:
            subtitle_lines.append("- Tệp đầu ra:")
            for format_name, output_path in sorted(self._last_subtitle_outputs.items()):
                subtitle_lines.append(f"- {format_name.upper()}: {output_path}")
        self._subtitle_summary.setText("\n".join(subtitle_lines))

        source_video_path = self._resolve_source_video_path()
        selected_export_preset = self._selected_export_preset(strict=False)
        selected_watermark_profile = self._selected_watermark_profile(strict=False)
        export_mode = "Ghi cứng ASS" if selected_export_preset and selected_export_preset.burn_subtitles else "Mux soft-sub"
        watermark_mode = (
            "Bật" if selected_export_preset and selected_export_preset.watermark_enabled else "Tắt"
        )
        watermark_profile_label = (
            selected_watermark_profile.name
            if selected_watermark_profile
            else "Theo preset xuất"
        )
        watermark_path_label = self._resolve_watermark_path() or (
            selected_export_preset.watermark_path if selected_export_preset and selected_export_preset.watermark_path else "-"
        )
        if self._last_export_output:
            self._export_summary.setText(
                "Video đầu ra:\n"
                f"- Nguồn: {source_video_path or '-'}\n"
                f"- Preset xuất: {selected_export_preset.name if selected_export_preset else '-'}\n"
                f"- Chế độ: {export_mode if selected_export_preset else '-'}\n"
                f"- Profile watermark: {watermark_profile_label}\n"
                f"- Watermark: {watermark_mode} / {watermark_path_label}\n"
                f"- Âm thanh đã trộn: {self._last_mixed_audio_output or '-'}\n"
                f"- Tệp đầu ra: {self._last_export_output}"
            )
        elif source_video_path:
            self._export_summary.setText(
                "Sẵn sàng xuất video:\n"
                f"- Nguồn: {source_video_path}\n"
                f"- Track phụ đề: {self._subtitle_track_label(active_track)}\n"
                f"- Số dòng phụ đề: {len(subtitle_rows)}\n"
                f"- Preset xuất: {selected_export_preset.name if selected_export_preset else '-'}\n"
                f"- Chế độ: {export_mode if selected_export_preset else '-'}\n"
                f"- Profile watermark: {watermark_profile_label}\n"
                f"- Watermark: {watermark_mode} / {watermark_path_label}\n"
                f"- Âm thanh đã trộn: {self._last_mixed_audio_output or '-'}\n"
                f"- Thư mục xuất: {self._current_workspace.exports_dir}"
            )
        else:
            self._export_summary.setText("Chưa có video nguồn để xuất")

        pipeline_lines = [
            "Checklist quy trình:",
            f"- Metadata video: {'Sẵn sàng' if video_row else 'Thiếu'}",
            f"- Bộ đệm audio 16 kHz/48 kHz: {'Sẵn sàng' if self._audio_artifacts else 'Thiếu'}",
            f"- Phân đoạn ASR: {segment_count}",
            f"- Dịch: {translated_count}/{segment_count}",
            f"- Dòng phụ đề tiếng đích: {subtitle_ready_count}/{len(subtitle_rows)}",
            f"- QC phụ đề: {'Đạt' if qc_report.error_count == 0 else f'{qc_report.error_count} lỗi'}"
            + (f", {qc_report.warning_count} cảnh báo" if qc_report.warning_count else ""),
            f"- Nội dung lồng tiếng tiếng đích: {tts_text_ready_count}/{len(subtitle_rows)}",
            f"- Clip TTS: {tts_ready_count}/{len(subtitle_rows)}",
            f"- Track giọng: {'Sẵn sàng' if voice_track_ready else 'Thiếu hoặc cần tạo lại'}",
            f"- Âm thanh đã trộn: {'Sẵn sàng' if mixed_audio_ready else 'Thiếu hoặc cần trộn lại'}",
            (
                "- Sẵn sàng xuất: Có"
                if video_row
                and subtitle_rows
                and subtitle_ready_count == len(subtitle_rows)
                and qc_report.error_count == 0
                and (
                    not self._last_voice_track_output
                    or (
                        voice_track_ready
                        and mixed_audio_ready
                    )
                )
                else "- Sẵn sàng xuất: Không"
            ),
            f"- Video đầu ra: {'Sẵn sàng' if self._last_export_output and self._last_export_output.exists() else 'Chưa có'}",
        ]
        self._pipeline_summary.setText("\n".join(pipeline_lines))
        self._refresh_export_access_actions()
        if not self._workflow_current_stage:
            self._update_workflow_status_label()

    def closeEvent(self, event) -> None:  # noqa: N802
        self._cancel_preview_reload()
        self._preview_controller.close()
        super().closeEvent(event)

    def _append_log_line(self, message: str) -> None:
        self._logs_console.appendPlainText(message)


