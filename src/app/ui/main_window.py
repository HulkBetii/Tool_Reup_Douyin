from __future__ import annotations

import json
import re
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QItemSelectionModel, QSignalBlocker, QTimer, Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
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
from app.core.paths import get_appdata_dir
from app.core.settings import AppSettings, save_settings
from app.exporting.models import WatermarkProfile
from app.exporting.presets import (
    list_export_presets,
    list_watermark_profiles,
    save_watermark_profile,
)
from app.media.extract_audio import extract_audio_artifacts, load_cached_audio_artifacts
from app.media.ffprobe_service import attach_source_video_to_project, probe_media
from app.media.models import ExtractedAudioArtifacts, MediaMetadata
from app.project.bootstrap import bootstrap_project, open_project, sync_project_snapshot, utc_now_iso
from app.project.database import (
    CANONICAL_SUBTITLE_TRACK_KIND,
    ProjectDatabase,
    USER_SUBTITLE_TRACK_KIND,
)
from app.project.models import (
    ProjectInitRequest,
    ProjectWorkspace,
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
    build_speaker_binding_plan,
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

        self._tabs.addTab(self._wrap_scrollable_tab(self._project_tab), "Dá»± Ã¡n")
        self._tabs.addTab(self._wrap_scrollable_tab(self._translate_tab), "ASR & Dá»‹ch")
        self._tabs.addTab(self._wrap_scrollable_tab(self._subtitle_tab), "Phá»¥ Ä‘á»")
        self._tabs.addTab(self._wrap_scrollable_tab(self._voiceover_tab), "Lá»“ng tiáº¿ng")
        self._tabs.addTab(self._wrap_scrollable_tab(self._export_tab), "Xuáº¥t báº£n")
        self._tabs.addTab(self._wrap_scrollable_tab(self._settings_tab), "CÃ i Ä‘áº·t")
        self._tabs.addTab(self._wrap_scrollable_tab(self._logs_tab), "Nháº­t kÃ½")

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._tabs)
        splitter.addWidget(self._status_panel)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(splitter)
        self.setCentralWidget(container)

        self._sync_settings_to_form()
        self._append_log_line("Khá»Ÿi táº¡o giao diá»‡n hoÃ n táº¥t")

    def _build_project_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self._project_summary = self._create_info_label("ChÆ°a má»Ÿ dá»± Ã¡n")
        self._media_summary = self._create_info_label("ChÆ°a cÃ³ video nguá»“n")
        self._pipeline_summary = self._create_info_label("Checklist quy trÃ¬nh chÆ°a cÃ³ dá»¯ liá»‡u")
        self._workflow_status = self._create_info_label("Quy trÃ¬nh nhanh: sáºµn sÃ ng")

        group = QGroupBox("Khá»Ÿi táº¡o / má»Ÿ dá»± Ã¡n")
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._project_name_input = QLineEdit("Dá»± Ã¡n má»›i")
        self._project_root_input = QLineEdit(str(Path.cwd() / "workspace"))
        self._source_video_input = QLineEdit()
        self._source_lang_combo = QComboBox()
        self._source_lang_combo.addItems(["auto", "vi", "zh", "en"])
        self._target_lang_combo = QComboBox()
        self._target_lang_combo.addItems(["vi", "zh", "en"])

        browse_button = QPushButton("Chá»n thÆ° má»¥c")
        browse_button.clicked.connect(self._choose_project_root)
        create_button = QPushButton("Táº¡o dá»± Ã¡n")
        create_button.clicked.connect(self._create_project)
        open_button = QPushButton("Má»Ÿ dá»± Ã¡n")
        open_button.clicked.connect(self._open_project)
        smoke_button = QPushButton("Cháº¡y tÃ¡c vá»¥ thá»­")
        smoke_button.clicked.connect(self._run_smoke_job)
        choose_video_button = QPushButton("Chá»n video")
        choose_video_button.clicked.connect(self._choose_source_video)
        probe_button = QPushButton("Äá»c metadata")
        probe_button.clicked.connect(self._run_probe_media_job)
        extract_button = QPushButton("TÃ¡ch Ã¢m thanh")
        extract_button.clicked.connect(self._run_extract_audio_job)
        prepare_media_button = QPushButton("Chuáº©n bá»‹ media")
        prepare_media_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio"],
                workflow_name="Chuáº©n bá»‹ media",
            )
        )
        asr_translate_button = QPushButton("ASR -> Dá»‹ch")
        asr_translate_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["asr", "translate"],
                workflow_name="ASR -> Dá»‹ch",
            )
        )
        dub_button = QPushButton("Lá»“ng tiáº¿ng nhanh")
        dub_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["tts", "voice_track", "mixdown"],
                workflow_name="Lá»“ng tiáº¿ng nhanh",
            )
        )
        full_pipeline_button = QPushButton("Cháº¡y toÃ n bá»™ quy trÃ¬nh")
        full_pipeline_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio", "asr", "translate", "tts", "voice_track", "mixdown", "export_video"],
                workflow_name="ToÃ n bá»™ quy trÃ¬nh",
            )
        )
        stop_workflow_button = QPushButton("Dá»«ng quy trÃ¬nh")
        stop_workflow_button.clicked.connect(self._stop_workflow)

        button_row = QHBoxLayout()
        button_row.addWidget(browse_button)
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
        workflow_row_top.addWidget(dub_button)
        workflow_row_top.addStretch(1)
        workflow_row_bottom = QHBoxLayout()
        workflow_row_bottom.addWidget(full_pipeline_button)
        workflow_row_bottom.addWidget(stop_workflow_button)
        workflow_row_bottom.addStretch(1)
        workflow_row = QVBoxLayout()
        workflow_row.addLayout(workflow_row_top)
        workflow_row.addLayout(workflow_row_bottom)
        workflow_container = QWidget()
        workflow_container.setLayout(workflow_row)

        workflow_group = QGroupBox("Quy trÃ¬nh nhanh")
        workflow_form = QFormLayout(workflow_group)
        self._configure_form_layout(workflow_form)
        workflow_form.addRow("Tráº¡ng thÃ¡i", self._workflow_status)
        workflow_form.addRow("", workflow_container)

        form.addRow("TÃªn dá»± Ã¡n", self._project_name_input)
        form.addRow("ThÆ° má»¥c dá»± Ã¡n", self._project_root_input)
        form.addRow("Video nguá»“n", self._source_video_input)
        form.addRow("", source_container)
        form.addRow("NgÃ´n ngá»¯ nguá»“n", self._source_lang_combo)
        form.addRow("Dá»‹ch sang", self._target_lang_combo)
        form.addRow("", button_container)

        layout.addWidget(self._project_summary)
        layout.addWidget(self._media_summary)
        layout.addWidget(self._pipeline_summary)
        layout.addWidget(group)
        layout.addWidget(workflow_group)
        layout.addWidget(
            self._build_placeholder_group(
                "HÆ°á»›ng dáº«n nhanh",
                "1. Chá»n video nguá»“n rá»“i táº¡o hoáº·c má»Ÿ dá»± Ã¡n.\n"
                "2. DÃ¹ng â€œChuáº©n bá»‹ mediaâ€ Ä‘á»ƒ Ä‘á»c metadata vÃ  tÃ¡ch Ã¢m thanh.\n"
                "3. DÃ¹ng â€œASR -> Dá»‹châ€ Ä‘á»ƒ nháº­n diá»‡n lá»i nÃ³i vÃ  táº¡o báº£n dá»‹ch.\n"
                "4. Náº¿u cáº§n lá»“ng tiáº¿ng hoáº·c xuáº¥t video ngay, dÃ¹ng cÃ¡c quy trÃ¬nh nhanh cÃ²n láº¡i.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_translate_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("ASR vÃ  Dá»‹ch")
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._asr_summary = self._create_info_label("ChÆ°a cÃ³ káº¿t quáº£ ASR")
        self._asr_engine_combo = QComboBox()
        self._asr_engine_combo.addItems(["faster-whisper"])
        self._asr_model_combo = QComboBox()
        self._asr_model_combo.addItems(["tiny", "base", "small", "medium", "large-v3"])
        self._asr_model_combo.setCurrentText(self._settings.default_asr_model)
        self._asr_language_combo = QComboBox()
        self._asr_language_combo.addItems(["auto", "vi", "zh", "en"])
        self._vad_checkbox = QCheckBox("Báº­t lá»c VAD")
        self._vad_checkbox.setChecked(True)
        self._word_timestamps_checkbox = QCheckBox("Láº¥y má»‘c thá»i gian theo tá»«")
        self._word_timestamps_checkbox.setChecked(True)
        self._prompt_combo = QComboBox()
        self._translation_mode_info = self._create_info_label("legacy")
        self._translation_model_input = QLineEdit()
        self._translation_model_input.setPlaceholderText("gpt-4.1-mini")
        self._translation_summary = self._create_info_label("ChÆ°a cÃ³ káº¿t quáº£ dá»‹ch")
        self._review_summary = self._create_info_label("ChÆ°a cÃ³ hÃ ng review semantic")

        run_asr_button = QPushButton("Cháº¡y ASR")
        run_asr_button.clicked.connect(self._run_asr_job)
        reload_prompts_button = QPushButton("Náº¡p láº¡i prompt")
        reload_prompts_button.clicked.connect(self._reload_prompt_templates)
        run_translate_button = QPushButton("Cháº¡y dá»‹ch")
        run_translate_button.clicked.connect(self._run_translation_job)
        translate_buttons = QHBoxLayout()
        translate_buttons.addWidget(reload_prompts_button)
        translate_buttons.addWidget(run_translate_button)
        translate_buttons.addStretch(1)
        translate_container = QWidget()
        translate_container.setLayout(translate_buttons)
        review_group = QGroupBox("Review Ngá»¯ Cáº£nh")
        review_layout = QVBoxLayout(review_group)
        review_layout.addWidget(self._review_summary)
        self._review_table = QTableWidget(0, 7)
        self._review_table.setHorizontalHeaderLabels(
            ["#", "Scene", "Nguá»“n", "Speaker", "Listener", "XÆ°ng hÃ´", "LÃ½ do"]
        )
        self._review_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._review_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._review_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._review_table.setAlternatingRowColors(True)
        self._review_table.setWordWrap(True)
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
        review_form.addRow("Tá»± xÆ°ng", self._review_self_term_input)
        review_form.addRow("Gá»i ngÆ°á»i nghe", self._review_address_term_input)
        review_form.addRow("Phá»¥ Ä‘á» duyá»‡t", self._review_subtitle_input)
        review_form.addRow("Lá»i TTS duyá»‡t", self._review_tts_input)
        review_layout.addLayout(review_form)
        review_button_row = QHBoxLayout()
        reload_review_button = QPushButton("Náº¡p review")
        reload_review_button.clicked.connect(self._reload_review_queue)
        approve_line_button = QPushButton("KhÃ³a dÃ²ng")
        approve_line_button.clicked.connect(lambda checked=False: self._apply_review_resolution("line"))
        approve_scene_button = QPushButton("KhÃ³a scene")
        approve_scene_button.clicked.connect(lambda checked=False: self._apply_review_resolution("scene"))
        approve_relation_button = QPushButton("KhÃ³a quan há»‡")
        approve_relation_button.clicked.connect(
            lambda checked=False: self._apply_review_resolution("project-relationship")
        )
        select_scene_button = QPushButton("Chá»n cÃ¹ng scene")
        select_scene_button.clicked.connect(lambda checked=False: self._select_review_rows_by_scope("scene"))
        select_relation_button = QPushButton("Chá»n cÃ¹ng quan há»‡")
        select_relation_button.clicked.connect(lambda checked=False: self._select_review_rows_by_scope("relation"))
        approve_selected_button = QPushButton("Ãp cho dÃ²ng chá»n")
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
        form.addRow("MÃ´ hÃ¬nh", self._asr_model_combo)
        form.addRow("NgÃ´n ngá»¯ ASR", self._asr_language_combo)
        form.addRow("", self._vad_checkbox)
        form.addRow("", self._word_timestamps_checkbox)
        form.addRow("", run_asr_button)
        form.addRow("Cháº¿ Ä‘á»™ dá»‹ch", self._translation_mode_info)
        form.addRow("Máº«u prompt", self._prompt_combo)
        form.addRow("MÃ´ hÃ¬nh dá»‹ch", self._translation_model_input)
        form.addRow("", translate_container)

        layout.addWidget(self._asr_summary)
        layout.addWidget(self._translation_summary)
        layout.addWidget(group)
        layout.addWidget(review_group)
        layout.addWidget(
            self._build_placeholder_group(
                "HÆ°á»›ng dáº«n",
                "ASR sáº½ Ä‘á»c lá»i thoáº¡i tá»« Ã¢m thanh 16 kHz. BÆ°á»›c dá»‹ch dÃ¹ng OpenAI Responses API "
                "vÃ  Structured Outputs Ä‘á»ƒ táº¡o báº£n dá»‹ch á»•n Ä‘á»‹nh cho phá»¥ Ä‘á».",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_subtitle_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._subtitle_summary = self._create_info_label("ChÆ°a cÃ³ file phá»¥ Ä‘á» Ä‘áº§u ra")
        self._subtitle_editor_status = self._create_info_label("ChÆ°a náº¡p dá»¯ liá»‡u vÃ o trÃ¬nh biÃªn táº­p")
        self._subtitle_qc_summary = self._create_info_label("QC phá»¥ Ä‘á» chÆ°a Ä‘Æ°á»£c cháº¡y")
        self._subtitle_table = QTableWidget(0, 8)
        self._subtitle_table.setHorizontalHeaderLabels(
            ["#", "Báº¯t Ä‘áº§u", "Káº¿t thÃºc", "Nguá»“n", "Báº£n dá»‹ch", "Phá»¥ Ä‘á»", "Lá»i TTS", "Tráº¡ng thÃ¡i"]
        )
        self._subtitle_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._subtitle_table.setAlternatingRowColors(True)
        self._subtitle_table.setWordWrap(True)
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
        self._subtitle_qc_table.setHorizontalHeaderLabels(["DÃ²ng", "MÃ£ lá»—i", "Má»©c Ä‘á»™", "Chi tiáº¿t"])
        self._subtitle_qc_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._subtitle_qc_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._subtitle_qc_table.setAlternatingRowColors(True)
        self._subtitle_qc_table.setWordWrap(True)
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
        self._replace_target_combo.addItem("Phá»¥ Ä‘á»", "subtitle")
        self._replace_target_combo.addItem("Báº£n dá»‹ch", "translated")
        self._replace_target_combo.addItem("TTS", "tts")
        self._replace_target_combo.addItem("Táº¥t cáº£", "all")

        reload_button = QPushButton("Náº¡p láº¡i tá»« CSDL")
        reload_button.clicked.connect(lambda: self._reload_subtitle_editor_from_db(force=True))
        translated_to_subtitle_button = QPushButton("Báº£n dá»‹ch -> Phá»¥ Ä‘á»")
        translated_to_subtitle_button.clicked.connect(self._apply_translated_to_subtitle)
        subtitle_to_tts_button = QPushButton("Phá»¥ Ä‘á» -> Lá»i TTS")
        subtitle_to_tts_button.clicked.connect(self._apply_subtitle_to_tts)
        polish_tts_button = QPushButton("LÃ m mÆ°á»£t Lá»i TTS")
        polish_tts_button.clicked.connect(self._polish_tts_texts)
        split_button = QPushButton("TÃ¡ch dÃ²ng chá»n")
        split_button.clicked.connect(self._split_selected_subtitle_row)
        merge_button = QPushButton("Gá»™p vá»›i dÃ²ng sau")
        merge_button.clicked.connect(self._merge_selected_subtitle_row_with_next)
        save_button = QPushButton("LÆ°u chá»‰nh sá»­a")
        save_button.clicked.connect(self._save_subtitle_edits)
        shift_button = QPushButton("Dá»‹ch toÃ n bá»™")
        shift_button.clicked.connect(self._apply_shift_to_subtitle_rows)
        replace_button = QPushButton("TÃ¬m vÃ  thay tháº¿")
        replace_button.clicked.connect(self._apply_find_replace)
        qc_button = QPushButton("Cháº¡y QC")
        qc_button.clicked.connect(self._run_subtitle_qc)
        preview_from_start_button = QPushButton("Xem tá»« Ä‘áº§u")
        preview_from_start_button.clicked.connect(lambda: self._preview_subtitles(start_from_selected=False))
        preview_selected_button = QPushButton("Xem dÃ²ng chá»n")
        preview_selected_button.clicked.connect(lambda: self._preview_subtitles(start_from_selected=True))
        export_srt_button = QPushButton("Xuáº¥t SRT")
        export_srt_button.clicked.connect(lambda: self._run_export_subtitles_job("srt"))
        export_ass_button = QPushButton("Xuáº¥t ASS")
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
        action_row_bottom.addWidget(export_srt_button)
        action_row_bottom.addWidget(export_ass_button)
        action_row_bottom.addStretch(1)
        action_row = QVBoxLayout()
        action_row.addLayout(action_row_top)
        action_row.addLayout(action_row_bottom)
        action_container = QWidget()
        action_container.setLayout(action_row)

        shift_row = QHBoxLayout()
        shift_row.addWidget(QLabel("Dá»‹ch thá»i gian (ms)"))
        shift_row.addWidget(self._shift_input)
        shift_row.addWidget(shift_button)
        shift_row.addStretch(1)
        replace_row = QHBoxLayout()
        replace_row.addWidget(QLabel("TÃ¬m"))
        replace_row.addWidget(self._find_input)
        replace_row.addWidget(QLabel("Thay báº±ng"))
        replace_row.addWidget(self._replace_input)
        replace_row.addWidget(self._replace_target_combo)
        replace_row.addWidget(replace_button)
        replace_row.addStretch(1)
        tools_row = QVBoxLayout()
        tools_row.addLayout(shift_row)
        tools_row.addLayout(replace_row)
        tools_container = QWidget()
        tools_container.setLayout(tools_row)

        layout.addWidget(self._subtitle_summary)
        layout.addWidget(self._subtitle_editor_status)
        layout.addWidget(self._subtitle_table)
        layout.addWidget(tools_container)
        layout.addWidget(action_container)
        layout.addWidget(self._subtitle_qc_summary)
        layout.addWidget(self._subtitle_qc_table)
        layout.addWidget(
            self._build_placeholder_group(
                "HÆ°á»›ng dáº«n",
                "Báº¡n cÃ³ thá»ƒ sá»­a thá»i gian, báº£n dá»‹ch, phá»¥ Ä‘á» vÃ  ná»™i dung TTS ngay trong báº£ng. "
                "Náº¿u muá»‘n giá»ng Ä‘á»c tá»± nhiÃªn hÆ¡n, hÃ£y dÃ¹ng `Phá»¥ Ä‘á» -> Lá»i TTS` rá»“i `LÃ m mÆ°á»£t Lá»i TTS`, "
                "sau Ä‘Ã³ tinh chá»‰nh riÃªng nhá»¯ng cÃ¢u quan trá»ng. HÃ£y cháº¡y QC trÆ°á»›c khi xuáº¥t Ä‘á»ƒ kiá»ƒm tra lá»—i chá»“ng dÃ²ng, "
                "tá»‘c Ä‘á»™ Ä‘á»c vÃ  Ä‘á»™ dÃ i cÃ¢u.",
            )
        )
        return widget

    def _build_voiceover_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._voice_summary = self._create_info_label("ChÆ°a cÃ³ preset giá»ng hoáº·c káº¿t quáº£ TTS")
        self._mix_summary = self._create_info_label("ChÆ°a cÃ³ Ã¢m thanh Ä‘Ã£ trá»™n")
        self._voice_combo = QComboBox()
        self._voice_combo.currentIndexChanged.connect(self._handle_voice_preset_changed)
        self._voice_profile_name_input = QLineEdit()
        self._voice_profile_name_input.setPlaceholderText("TÃªn preset giá»ng")
        self._voice_profile_name_input.textChanged.connect(lambda _text: self._handle_voice_profile_form_changed())
        self._voice_engine_combo = QComboBox()
        self._voice_engine_combo.addItem("Windows SAPI", "sapi")
        self._voice_engine_combo.addItem("VieNeu", "vieneu")
        self._voice_engine_combo.currentIndexChanged.connect(self._handle_voice_profile_form_changed)
        self._voice_id_input = QLineEdit()
        self._voice_id_input.setPlaceholderText("default hoáº·c tÃªn giá»ng")
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
        self._voice_info = self._create_info_label("Giá»ng SAPI Ä‘Ã£ phÃ¡t hiá»‡n: chÆ°a náº¡p")
        self._voice_preset_notes = self._create_info_label("ChÆ°a cÃ³ ghi chÃº cho preset")
        self._voice_clone_status = self._create_info_label("Preset clone chÆ°a Ä‘Æ°á»£c cáº¥u hÃ¬nh")
        self._voice_profile_status = self._create_info_label("Quáº£n lÃ½ preset giá»ng Ä‘Ã£ sáºµn sÃ ng")
        self._speaker_binding_status = self._create_info_label("Speaker binding: chÆ°a cÃ³ dá»¯ liá»‡u")
        self._speaker_binding_hint = self._create_info_label(
            "Máº¹o: náº¿u Ä‘Ã£ lÆ°u Ã­t nháº¥t 1 speaker binding, má»i speaker nháº­n diá»‡n rÃµ trong track hiá»‡n táº¡i pháº£i Ä‘Æ°á»£c gÃ¡n preset. Speaker unknown váº«n dÃ¹ng preset máº·c Ä‘á»‹nh."
        )
        self._voice_policy_status = self._create_info_label("Voice policy: chÆ°a cÃ³ dá»¯ liá»‡u")
        self._voice_policy_hint = self._create_info_label(
            "Máº¹o: voice policy lÃ  fallback má»m. Quan há»‡ speaker->listener sáº½ Æ°u tiÃªn hÆ¡n policy theo nhÃ¢n váº­t, nhÆ°ng speaker binding váº«n lÃ  má»©c Æ°u tiÃªn cao nháº¥t."
        )
        self._voice_notes_input = QPlainTextEdit()
        self._voice_notes_input.setPlaceholderText("Ghi chÃº preset hoáº·c ghi chÃº vá» cáº¥u hÃ¬nh clone")
        self._voice_notes_input.setFixedHeight(56)
        self._voice_notes_input.textChanged.connect(self._handle_voice_profile_form_changed)
        self._vieneu_ref_audio_input = QLineEdit()
        self._vieneu_ref_audio_input.setPlaceholderText("assets/voices/reference.wav hoáº·c Ä‘Æ°á»ng dáº«n tuyá»‡t Ä‘á»‘i")
        self._vieneu_ref_audio_input.textChanged.connect(lambda _text: self._handle_voice_clone_form_changed())
        self._vieneu_ref_text_input = QPlainTextEdit()
        self._vieneu_ref_text_input.setPlaceholderText("Nháº­p cÃ¢u Ä‘á»c gá»‘c khá»›p vá»›i file audio máº«u")
        self._vieneu_ref_text_input.setFixedHeight(72)
        self._vieneu_ref_text_input.textChanged.connect(self._handle_voice_clone_form_changed)
        self._bgm_path_input = QLineEdit()
        self._bgm_path_input.setPlaceholderText("ÄÆ°á»ng dáº«n BGM tÃ¹y chá»n")
        self._original_volume_input = QLineEdit("0.35")
        self._voice_volume_input = QLineEdit("1.0")
        self._bgm_volume_input = QLineEdit("0.15")
        self._speaker_binding_table = QTableWidget(0, 4)
        self._speaker_binding_table.setHorizontalHeaderLabels(["Speaker", "Sá»‘ dÃ²ng", "Preset giá»ng", "Tráº¡ng thÃ¡i"])
        self._speaker_binding_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._speaker_binding_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._speaker_binding_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._speaker_binding_table.setAlternatingRowColors(True)
        self._speaker_binding_table.verticalHeader().setVisible(False)
        self._speaker_binding_table.setMinimumHeight(180)
        speaker_binding_header = self._speaker_binding_table.horizontalHeader()
        speaker_binding_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        speaker_binding_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        speaker_binding_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        speaker_binding_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._character_voice_policy_table = QTableWidget(0, 7)
        self._character_voice_policy_table.setHorizontalHeaderLabels(
            ["NhÃ¢n váº­t", "Sá»‘ dÃ²ng", "Preset máº·c Ä‘á»‹nh", "Tá»‘c Ä‘á»™", "Ã‚m lÆ°á»£ng", "Cao Ä‘á»™", "Tráº¡ng thÃ¡i"]
        )
        self._character_voice_policy_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._character_voice_policy_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._character_voice_policy_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._character_voice_policy_table.setAlternatingRowColors(True)
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
            ["Quan há»‡", "Sá»‘ dÃ²ng", "Preset override", "Tá»‘c Ä‘á»™", "Ã‚m lÆ°á»£ng", "Cao Ä‘á»™", "Tráº¡ng thÃ¡i"]
        )
        self._relationship_voice_policy_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._relationship_voice_policy_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._relationship_voice_policy_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._relationship_voice_policy_table.setAlternatingRowColors(True)
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

        reload_voices_button = QPushButton("Náº¡p láº¡i preset")
        reload_voices_button.clicked.connect(self._reload_voice_presets)
        run_tts_button = QPushButton("Cháº¡y TTS")
        run_tts_button.clicked.connect(self._run_tts_job)
        build_track_button = QPushButton("Táº¡o track giá»ng")
        build_track_button.clicked.connect(self._run_build_voice_track_job)
        self._choose_vieneu_ref_audio_button = QPushButton("Chá»n audio máº«u")
        self._choose_vieneu_ref_audio_button.clicked.connect(self._choose_vieneu_ref_audio_file)
        self._save_voice_preset_button = QPushButton("LÆ°u preset")
        self._save_voice_preset_button.clicked.connect(self._save_current_voice_preset)
        self._save_voice_preset_as_new_button = QPushButton("LÆ°u thÃ nh báº£n má»›i")
        self._save_voice_preset_as_new_button.clicked.connect(
            lambda checked=False: self._save_current_voice_preset(save_as_new=True)
        )
        self._delete_voice_preset_button = QPushButton("XÃ³a preset")
        self._delete_voice_preset_button.clicked.connect(self._delete_selected_voice_preset)
        self._batch_import_voice_profiles_button = QPushButton("Nháº­p hÃ ng loáº¡t tá»« thÆ° má»¥c máº«u")
        self._batch_import_voice_profiles_button.clicked.connect(self._batch_import_voice_profiles)
        self._reload_speaker_bindings_button = QPushButton("Náº¡p speaker")
        self._reload_speaker_bindings_button.clicked.connect(self._reload_speaker_bindings)
        self._save_speaker_bindings_button = QPushButton("LÆ°u binding")
        self._save_speaker_bindings_button.clicked.connect(self._save_speaker_bindings)
        self._fill_speaker_bindings_button = QPushButton("GÃ¡n preset Ä‘ang chá»n cho Ã´ trá»‘ng")
        self._fill_speaker_bindings_button.clicked.connect(self._fill_unbound_speakers_with_selected_preset)
        self._clear_speaker_bindings_button = QPushButton("XÃ³a gÃ¡n trÃªn form")
        self._clear_speaker_bindings_button.clicked.connect(self._clear_speaker_binding_form)
        self._fill_selected_speaker_bindings_button = QPushButton("Gan preset cho dong chon")
        self._fill_selected_speaker_bindings_button.clicked.connect(
            self._fill_selected_speaker_bindings_with_selected_preset
        )
        self._clear_selected_speaker_bindings_button = QPushButton("Xoa dong chon")
        self._clear_selected_speaker_bindings_button.clicked.connect(self._clear_selected_speaker_bindings)
        self._reload_voice_policies_button = QPushButton("Náº¡p voice policy")
        self._reload_voice_policies_button.clicked.connect(self._reload_voice_policies)
        self._save_voice_policies_button = QPushButton("LÆ°u voice policy")
        self._save_voice_policies_button.clicked.connect(self._save_voice_policies)
        self._fill_voice_policies_button = QPushButton("GÃ¡n preset Ä‘ang chá»n cho policy trá»‘ng")
        self._fill_voice_policies_button.clicked.connect(self._fill_unbound_voice_policies_with_selected_preset)
        self._clear_voice_policies_button = QPushButton("XÃ³a policy trÃªn form")
        self._clear_voice_policies_button.clicked.connect(self._clear_voice_policy_form)
        self._fill_voice_policy_styles_button = QPushButton("Điền style trống")
        self._fill_voice_policy_styles_button.clicked.connect(self._fill_unstyled_voice_policies_with_current_style)
        self._clear_voice_policy_styles_button = QPushButton("Xóa style form")
        self._clear_voice_policy_styles_button.clicked.connect(self._clear_voice_policy_form_styles)
        self._fill_selected_voice_policies_button = QPushButton("Gan preset cho dong chon")
        self._fill_selected_voice_policies_button.clicked.connect(
            self._fill_selected_voice_policy_rows_with_selected_preset
        )
        self._fill_selected_voice_policy_styles_button = QPushButton("Điền style dòng chọn")
        self._fill_selected_voice_policy_styles_button.clicked.connect(
            self._fill_selected_voice_policy_rows_with_current_style
        )
        self._clear_selected_voice_policies_button = QPushButton("Xoa dong chon")
        self._clear_selected_voice_policies_button.clicked.connect(self._clear_selected_voice_policy_rows)
        self._clear_selected_voice_policy_styles_button = QPushButton("Xóa style dòng chọn")
        self._clear_selected_voice_policy_styles_button.clicked.connect(self._clear_selected_voice_policy_row_styles)
        choose_bgm_button = QPushButton("Chá»n BGM")
        choose_bgm_button.clicked.connect(self._choose_bgm_file)
        mix_button = QPushButton("Trá»™n Ã¢m thanh")
        mix_button.clicked.connect(self._run_mixdown_job)

        group = QGroupBox("Preset giá»ng, TTS vÃ  trá»™n Ã¢m thanh")
        form = QFormLayout(group)
        self._configure_form_layout(form)
        action_row = QHBoxLayout()
        action_row.addWidget(reload_voices_button)
        action_row.addWidget(run_tts_button)
        action_row.addWidget(build_track_button)
        action_row.addStretch(1)
        action_container = QWidget()
        action_container.setLayout(action_row)

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
        voice_policy_actions = QVBoxLayout()
        voice_policy_actions.addLayout(voice_policy_actions_top)
        voice_policy_actions.addLayout(voice_policy_actions_bottom)
        voice_policy_actions_container = QWidget()
        voice_policy_actions_container.setLayout(voice_policy_actions)

        ref_audio_row = QHBoxLayout()
        ref_audio_row.addWidget(self._vieneu_ref_audio_input)
        ref_audio_row.addWidget(self._choose_vieneu_ref_audio_button)
        ref_audio_container = QWidget()
        ref_audio_container.setLayout(ref_audio_row)

        bgm_row = QHBoxLayout()
        bgm_row.addWidget(self._bgm_path_input)
        bgm_row.addWidget(choose_bgm_button)
        bgm_container = QWidget()
        bgm_container.setLayout(bgm_row)

        profile_numeric_row_top = QHBoxLayout()
        profile_numeric_row_top.addWidget(QLabel("Táº§n sá»‘ máº«u"))
        profile_numeric_row_top.addWidget(self._voice_sample_rate_input)
        profile_numeric_row_top.addWidget(QLabel("Tá»‘c Ä‘á»™"))
        profile_numeric_row_top.addWidget(self._voice_speed_profile_input)
        profile_numeric_row_top.addStretch(1)
        profile_numeric_row_bottom = QHBoxLayout()
        profile_numeric_row_bottom.addWidget(QLabel("Ã‚m lÆ°á»£ng"))
        profile_numeric_row_bottom.addWidget(self._voice_profile_volume_input)
        profile_numeric_row_bottom.addWidget(QLabel("Cao Ä‘á»™"))
        profile_numeric_row_bottom.addWidget(self._voice_pitch_input)
        profile_numeric_row_bottom.addStretch(1)
        profile_numeric_row = QVBoxLayout()
        profile_numeric_row.addLayout(profile_numeric_row_top)
        profile_numeric_row.addLayout(profile_numeric_row_bottom)
        profile_numeric_container = QWidget()
        profile_numeric_container.setLayout(profile_numeric_row)

        mix_row = QHBoxLayout()
        mix_row.addWidget(QLabel("Audio gá»‘c"))
        mix_row.addWidget(self._original_volume_input)
        mix_row.addWidget(QLabel("Giá»ng Ä‘á»c"))
        mix_row.addWidget(self._voice_volume_input)
        mix_row.addWidget(QLabel("BGM"))
        mix_row.addWidget(self._bgm_volume_input)
        mix_row.addWidget(mix_button)
        mix_row.addStretch(1)
        mix_container = QWidget()
        mix_container.setLayout(mix_row)

        form.addRow("Preset giá»ng", self._voice_combo)
        form.addRow("TÃªn preset", self._voice_profile_name_input)
        form.addRow("Bá»™ mÃ¡y TTS", self._voice_engine_combo)
        form.addRow("ID giá»ng", self._voice_id_input)
        form.addRow("NgÃ´n ngá»¯", self._voice_language_input)
        form.addRow("ThÃ´ng sá»‘ giá»ng", profile_numeric_container)
        form.addRow("Giá»ng SAPI phÃ¡t hiá»‡n", self._voice_info)
        form.addRow("Ghi chÃº preset", self._voice_preset_notes)
        form.addRow("Tráº¡ng thÃ¡i chá»‰nh sá»­a", self._voice_profile_status)
        form.addRow("Ghi chÃº chi tiáº¿t", self._voice_notes_input)
        form.addRow("Tráº¡ng thÃ¡i clone", self._voice_clone_status)
        form.addRow("Audio máº«u VieNeu", ref_audio_container)
        form.addRow("VÄƒn báº£n máº«u VieNeu", self._vieneu_ref_text_input)
        form.addRow("", profile_action_container)
        form.addRow("", action_container)
        form.addRow("Speaker binding", self._speaker_binding_status)
        form.addRow("", self._speaker_binding_hint)
        form.addRow("Báº£ng gÃ¡n speaker", self._speaker_binding_table)
        form.addRow("", speaker_binding_actions_container)
        form.addRow("Voice policy", self._voice_policy_status)
        form.addRow("", self._voice_policy_hint)
        form.addRow("Policy theo nhÃ¢n váº­t", self._character_voice_policy_table)
        form.addRow("Policy theo quan há»‡", self._relationship_voice_policy_table)
        form.addRow("", voice_policy_actions_container)
        form.addRow("BGM tÃ¹y chá»n", bgm_container)
        form.addRow("Má»©c Ã¢m khi trá»™n", mix_container)

        layout.addWidget(self._voice_summary)
        layout.addWidget(self._mix_summary)
        layout.addWidget(group)
        layout.addWidget(
            self._build_placeholder_group(
                "HÆ°á»›ng dáº«n",
                "VieNeu phÃ¹ há»£p nháº¥t cho giá»ng tiáº¿ng Viá»‡t. Náº¿u báº¡n cáº§n nhÃ¢n báº£n giá»ng, hÃ£y Ä‘iá»n audio máº«u "
                "vÃ  vÄƒn báº£n máº«u Ä‘Ãºng vá»›i audio Ä‘Ã³ trÆ°á»›c khi cháº¡y TTS hoáº·c nháº­p hÃ ng loáº¡t tá»« thÆ° má»¥c `assets/voices`. "
                "Äá»ƒ giá»ng Ä‘á»c tá»± nhiÃªn hÆ¡n, hÃ£y viáº¿t cá»™t `Lá»i TTS` theo vÄƒn nÃ³i ngáº¯n gá»n rá»“i má»›i cháº¡y TTS.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_export_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._export_summary = self._create_info_label("ChÆ°a cÃ³ video Ä‘áº§u ra")
        self._export_preset_combo = QComboBox()
        self._export_preset_combo.currentIndexChanged.connect(self._handle_export_preset_changed)
        self._burn_subtitles_checkbox = QCheckBox("Ghi cá»©ng phá»¥ Ä‘á» vÃ o video")
        self._burn_subtitles_checkbox.setChecked(True)
        self._burn_subtitles_checkbox.stateChanged.connect(self._handle_export_mode_changed)
        self._watermark_profile_combo = QComboBox()
        self._watermark_profile_combo.currentIndexChanged.connect(self._handle_watermark_profile_changed)
        self._watermark_profile_name_input = QLineEdit()
        self._watermark_profile_name_input.setPlaceholderText("TÃªn profile watermark hoáº·c logo")
        self._watermark_profile_name_input.textChanged.connect(
            lambda _text: self._handle_watermark_form_changed()
        )
        self._watermark_profile_status = self._create_info_label("ChÆ°a cÃ³ profile watermark")
        self._watermark_enabled_checkbox = QCheckBox("Báº­t watermark hoáº·c logo")
        self._watermark_enabled_checkbox.stateChanged.connect(self._handle_watermark_form_changed)
        self._watermark_path_input = QLineEdit()
        self._watermark_path_input.setPlaceholderText("assets/logos/logo.png hoáº·c Ä‘Æ°á»ng dáº«n tuyá»‡t Ä‘á»‘i")
        self._watermark_path_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_position_combo = QComboBox()
        self._watermark_position_combo.addItem("TrÃªn pháº£i", "top-right")
        self._watermark_position_combo.addItem("TrÃªn trÃ¡i", "top-left")
        self._watermark_position_combo.addItem("DÆ°á»›i pháº£i", "bottom-right")
        self._watermark_position_combo.addItem("DÆ°á»›i trÃ¡i", "bottom-left")
        self._watermark_position_combo.currentIndexChanged.connect(self._handle_watermark_form_changed)
        self._watermark_opacity_input = QLineEdit("0.85")
        self._watermark_opacity_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_scale_input = QLineEdit("0.16")
        self._watermark_scale_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        self._watermark_margin_input = QLineEdit("24")
        self._watermark_margin_input.textChanged.connect(lambda _text: self._handle_watermark_form_changed())
        choose_watermark_button = QPushButton("Chá»n logo")
        choose_watermark_button.clicked.connect(self._choose_watermark_file)
        reload_presets_button = QPushButton("Náº¡p láº¡i preset")
        reload_presets_button.clicked.connect(self._reload_export_presets)
        reload_watermarks_button = QPushButton("Náº¡p láº¡i profile watermark")
        reload_watermarks_button.clicked.connect(self._reload_watermark_profiles)
        save_watermark_button = QPushButton("LÆ°u profile")
        save_watermark_button.clicked.connect(self._save_current_watermark_profile)
        save_watermark_as_new_button = QPushButton("LÆ°u thÃ nh báº£n má»›i")
        save_watermark_as_new_button.clicked.connect(
            lambda checked=False: self._save_current_watermark_profile(save_as_new=True)
        )
        export_button = QPushButton("Xuáº¥t video")
        export_button.clicked.connect(self._run_video_export_job)
        form_group = QGroupBox("Preset xuáº¥t video")
        form = QFormLayout(form_group)
        self._configure_form_layout(form)
        watermark_row = QHBoxLayout()
        watermark_row.addWidget(self._watermark_path_input)
        watermark_row.addWidget(choose_watermark_button)
        watermark_container = QWidget()
        watermark_container.setLayout(watermark_row)
        watermark_actions = QHBoxLayout()
        watermark_actions.addWidget(reload_watermarks_button)
        watermark_actions.addWidget(save_watermark_button)
        watermark_actions.addWidget(save_watermark_as_new_button)
        watermark_actions.addStretch(1)
        watermark_actions_container = QWidget()
        watermark_actions_container.setLayout(watermark_actions)
        watermark_numeric_row = QHBoxLayout()
        watermark_numeric_row.addWidget(QLabel("Äá»™ má»"))
        watermark_numeric_row.addWidget(self._watermark_opacity_input)
        watermark_numeric_row.addWidget(QLabel("Tá»· lá»‡"))
        watermark_numeric_row.addWidget(self._watermark_scale_input)
        watermark_numeric_row.addWidget(QLabel("Lá»"))
        watermark_numeric_row.addWidget(self._watermark_margin_input)
        watermark_numeric_row.addStretch(1)
        watermark_numeric_container = QWidget()
        watermark_numeric_container.setLayout(watermark_numeric_row)
        buttons = QHBoxLayout()
        buttons.addWidget(reload_presets_button)
        buttons.addWidget(export_button)
        buttons.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(buttons)
        layout.addWidget(self._export_summary)
        form.addRow("Preset xuáº¥t", self._export_preset_combo)
        form.addRow("Cháº¿ Ä‘á»™ phá»¥ Ä‘á»", self._burn_subtitles_checkbox)
        form.addRow("Profile watermark", self._watermark_profile_combo)
        form.addRow("TÃªn profile", self._watermark_profile_name_input)
        form.addRow("Tráº¡ng thÃ¡i profile", self._watermark_profile_status)
        form.addRow("Báº­t watermark", self._watermark_enabled_checkbox)
        form.addRow("ÄÆ°á»ng dáº«n logo", watermark_container)
        form.addRow("Vá»‹ trÃ­", self._watermark_position_combo)
        form.addRow("Äá»™ má» / Tá»· lá»‡ / Lá»", watermark_numeric_container)
        form.addRow("", watermark_actions_container)
        layout.addWidget(form_group)
        layout.addWidget(button_container)
        layout.addWidget(
            self._build_placeholder_group(
                "HÆ°á»›ng dáº«n",
                "Chá»n preset xuáº¥t báº£n Ä‘á»ƒ quyáº¿t Ä‘á»‹nh tá»· lá»‡ khung hÃ¬nh vÃ  kiá»ƒu chÃ¨n phá»¥ Ä‘á». "
                "Náº¿u cáº§n logo, báº¡n cÃ³ thá»ƒ chá»‰nh nhanh trong form rá»“i lÆ°u láº¡i thÃ nh profile Ä‘á»ƒ dÃ¹ng cho cÃ¡c láº§n xuáº¥t sau.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_settings_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("ÄÆ°á»ng dáº«n cÃ´ng cá»¥ vÃ  cÃ i Ä‘áº·t á»©ng dá»¥ng")
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._ui_language_input = QLineEdit()
        self._ffmpeg_path_input = QLineEdit()
        self._ffprobe_path_input = QLineEdit()
        self._mpv_path_input = QLineEdit()
        self._model_cache_input = QLineEdit()
        self._openai_key_input = QLineEdit()
        self._openai_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._default_translation_model_input = QLineEdit()
        self._ffmpeg_status = self._create_info_label("ChÆ°a kiá»ƒm tra")

        save_button = QPushButton("LÆ°u cÃ i Ä‘áº·t")
        save_button.clicked.connect(self._save_settings)
        check_button = QPushButton("Kiá»ƒm tra FFmpeg")
        check_button.clicked.connect(self._check_ffmpeg)

        buttons = QHBoxLayout()
        buttons.addWidget(save_button)
        buttons.addWidget(check_button)
        buttons.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(buttons)

        form.addRow("NgÃ´n ngá»¯ giao diá»‡n", self._ui_language_input)
        form.addRow("ÄÆ°á»ng dáº«n ffmpeg", self._ffmpeg_path_input)
        form.addRow("ÄÆ°á»ng dáº«n ffprobe", self._ffprobe_path_input)
        form.addRow("ÄÆ°á»ng dáº«n mpv DLL", self._mpv_path_input)
        form.addRow("ThÆ° má»¥c cache model", self._model_cache_input)
        form.addRow("OpenAI API key", self._openai_key_input)
        form.addRow("MÃ´ hÃ¬nh dá»‹ch máº·c Ä‘á»‹nh", self._default_translation_model_input)
        form.addRow("", button_container)
        form.addRow("Tráº¡ng thÃ¡i kiá»ƒm tra", self._ffmpeg_status)

        layout.addWidget(group)
        layout.addWidget(
            self._build_placeholder_group(
                "Gá»£i Ã½",
                "OpenAI API key chá»‰ cáº§n cho bÆ°á»›c dá»‹ch. mpv DLL chá»‰ cáº§n khi báº¡n muá»‘n xem trÆ°á»›c phá»¥ Ä‘á» trong mpv.",
            )
        )
        layout.addStretch(1)
        return widget

    def _build_logs_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self._logs_info = self._create_info_label(f"Dá»¯ liá»‡u á»©ng dá»¥ng: {get_appdata_dir()}")
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
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
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
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        return label

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
        directory = QFileDialog.getExistingDirectory(self, "Chá»n thÆ° má»¥c dá»± Ã¡n")
        if directory:
            self._project_root_input.setText(directory)

    def _choose_source_video(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chá»n video nguá»“n",
            str(Path.cwd()),
            "Tá»‡p video (*.mp4 *.mkv *.mov *.avi *.webm);;Táº¥t cáº£ tá»‡p (*.*)",
        )
        if file_path:
            self._source_video_input.setText(file_path)

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
            raise ValueError("ChÆ°a cÃ³ dá»± Ã¡n")
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
            action_label = "lá»“ng tiáº¿ng"
            column_hint = "Lá»i TTS, Phá»¥ Ä‘á» hoáº·c Báº£n dá»‹ch"
        else:
            action_label = "xuáº¥t phá»¥ Ä‘á»/video"
            column_hint = "Phá»¥ Ä‘á» hoáº·c Báº£n dá»‹ch"
        QMessageBox.warning(
            self,
            dialog_title,
            (
                f"KhÃ´ng thá»ƒ {action_label} vÃ¬ cÃ²n {len(missing_indexes)} dÃ²ng chÆ°a cÃ³ ná»™i dung tiáº¿ng Ä‘Ã­ch.\n"
                f"- DÃ²ng: {self._format_row_number_list(missing_indexes)}\n"
                f"- HÃ£y hoÃ n táº¥t bÆ°á»›c dá»‹ch hoáº·c Ä‘iá»n trá»±c tiáº¿p vÃ o cá»™t {column_hint} trÆ°á»›c khi tiáº¿p tá»¥c."
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
            f"QC phá»¥ Ä‘á» trÆ°á»›c khi xuáº¥t: {report.error_count} lá»—i, {report.warning_count} cáº£nh bÃ¡o, {report.total_segments} dÃ²ng"
        )
        if report.error_count == 0:
            return True

        first_error = next((issue for issue in report.issues if issue.severity == "error"), report.issues[0])
        self._focus_subtitle_table_row(first_error.segment_index)
        QMessageBox.warning(
            self,
            dialog_title,
            (
                f"KhÃ´ng thá»ƒ xuáº¥t khi QC cÃ²n {report.error_count} lá»—i.\n"
                f"- DÃ²ng lá»—i Ä‘áº§u tiÃªn: {first_error.segment_index + 1}\n"
                f"- {first_error.message}\n"
                "- HÃ£y sá»­a trong tab Phá»¥ Ä‘á» rá»“i thá»­ láº¡i."
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
            original_volume = self._parse_volume_value(self._original_volume_input.text(), field_name="Ã‚m lÆ°á»£ng audio gá»‘c")
            voice_volume = self._parse_volume_value(self._voice_volume_input.text(), field_name="Ã‚m lÆ°á»£ng giá»ng")
            bgm_volume = self._parse_volume_value(self._bgm_volume_input.text(), field_name="Ã‚m lÆ°á»£ng BGM")
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
            self._parse_volume_value(self._original_volume_input.text(), field_name="Ã‚m lÆ°á»£ng audio gá»‘c")
            self._parse_volume_value(self._voice_volume_input.text(), field_name="Ã‚m lÆ°á»£ng giá»ng")
            self._parse_volume_value(self._bgm_volume_input.text(), field_name="Ã‚m lÆ°á»£ng BGM")
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

    def _export_live_preview_ass_from_editor(self) -> Path:
        if not self._current_workspace:
            raise ValueError("ChÆ°a cÃ³ dá»± Ã¡n")
        rows = self._collect_subtitle_table_rows()
        ass_path = export_preview_subtitles(
            self._current_workspace,
            segments=rows,
            format_name="ass",
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
            self._subtitle_editor_status.setText("mpv Ä‘ang chá» phá»¥ Ä‘á» há»£p lá»‡ trÆ°á»›c khi tá»± náº¡p láº¡i")
            return
        except (PreviewUnavailableError, FileNotFoundError, RuntimeError) as exc:
            self._cancel_preview_reload()
            self._live_preview_ass_path = None
            self._subtitle_editor_status.setText(f"ÄÃ£ dá»«ng tá»± náº¡p láº¡i preview mpv: {exc}")
            self._append_log_line(f"Tá»± náº¡p láº¡i preview mpv tháº¥t báº¡i: {exc}")
            return

        self._subtitle_editor_status.setText("ÄÃ£ náº¡p láº¡i preview mpv tá»« trÃ¬nh biÃªn táº­p phá»¥ Ä‘á»")

    def _ensure_editable_subtitle_track(
        self,
        database: ProjectDatabase,
        active_track: object,
    ) -> tuple[object, bool]:
        if str(active_track["kind"]) != CANONICAL_SUBTITLE_TRACK_KIND:
            return active_track, False
        if not self._current_workspace:
            raise ValueError("ChÆ°a cÃ³ dá»± Ã¡n")

        now = utc_now_iso()
        forked_track = database.create_subtitle_track(
            SubtitleTrackRecord(
                track_id=f"{self._current_workspace.project_id}:user:{uuid4()}",
                project_id=self._current_workspace.project_id,
                name="Báº£n phá»¥ Ä‘á» chá»‰nh sá»­a",
                kind=USER_SUBTITLE_TRACK_KIND,
                notes="ÄÆ°á»£c tÃ¡ch tá»« track phá»¥ Ä‘á» chuáº©n khi lÆ°u chá»‰nh sá»­a tá»« trÃ¬nh biÃªn táº­p.",
                created_at=now,
                updated_at=now,
            ),
            set_active=True,
        )
        sync_project_snapshot(self._current_workspace)
        return forked_track, True

    def _create_project(self) -> None:
        root_dir = Path(self._project_root_input.text()).expanduser()
        source_video_path = self._resolve_source_video_path()
        request = ProjectInitRequest(
            name=self._project_name_input.text().strip() or "Dá»± Ã¡n má»›i",
            root_dir=root_dir,
            source_language=self._source_lang_combo.currentText(),
            target_language=self._target_lang_combo.currentText(),
            source_video_path=source_video_path,
        )
        try:
            workspace = bootstrap_project(request)
        except FileExistsError as exc:
            QMessageBox.warning(self, "KhÃ´ng thá»ƒ táº¡o dá»± Ã¡n", str(exc))
            return
        self._set_current_workspace(workspace)
        QMessageBox.information(self, "ThÃ nh cÃ´ng", f"ÄÃ£ táº¡o dá»± Ã¡n táº¡i:\n{workspace.root_dir}")

    def _open_project(self) -> None:
        directory = QFileDialog.getExistingDirectory(self, "Má»Ÿ thÆ° má»¥c dá»± Ã¡n")
        if not directory:
            return
        try:
            workspace = open_project(Path(directory))
        except FileNotFoundError as exc:
            QMessageBox.warning(self, "KhÃ´ng thá»ƒ má»Ÿ dá»± Ã¡n", str(exc))
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
        self._restore_workspace_runtime_state(workspace)
        self._source_video_input.setText(str(workspace.source_video_path) if workspace.source_video_path else "")
        self._reload_prompt_templates()
        self._reload_voice_presets()
        self._reload_export_presets()
        self._reload_watermark_profiles()
        self._project_summary.setText(
            "Dá»± Ã¡n hiá»‡n táº¡i:\n"
            f"- TÃªn: {workspace.name}\n"
            f"- ThÆ° má»¥c: {workspace.root_dir}\n"
            f"- CSDL: {workspace.database_path}\n"
            f"- Cache: {workspace.cache_dir}\n"
            f"- Xuáº¥t báº£n: {workspace.exports_dir}"
        )
        self._refresh_workspace_views()
        self._logs_info.setText(
            f"Dá»¯ liá»‡u á»©ng dá»¥ng: {get_appdata_dir()}\nNháº­t kÃ½ dá»± Ã¡n: {workspace.logs_dir}"
        )
        self._append_log_line(f"Má»Ÿ workspace: {workspace.root_dir}")
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
            "probe_media": "Äá»c metadata",
            "extract_audio": "TÃ¡ch Ã¢m thanh",
            "asr": "ASR",
            "translate": "Dá»‹ch",
            "tts": "TTS",
            "voice_track": "Táº¡o track giá»ng",
            "mixdown": "Trá»™n Ã¢m thanh",
            "export_video": "Xuáº¥t video",
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
            tail = f"\n- CÃ²n láº¡i: {remaining}" if remaining else ""
            self._workflow_status.setText(
                f"Quy trÃ¬nh nhanh: Ä‘ang cháº¡y {self._workflow_name or '-'}\n"
                f"- BÆ°á»›c hiá»‡n táº¡i: {self._workflow_stage_label(self._workflow_current_stage)}"
                f"{tail}"
            )
            return
        self._workflow_status.setText("Quy trÃ¬nh nhanh: sáºµn sÃ ng")

    def _start_workflow(self, stages: list[str], *, workflow_name: str) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        if not stages:
            return
        self._workflow_queue = list(stages)
        self._workflow_current_stage = None
        self._workflow_current_job_id = None
        self._workflow_name = workflow_name
        self._append_log_line(f"Khá»Ÿi Ä‘á»™ng quy trÃ¬nh nhanh: {workflow_name}")
        self._run_next_workflow_stage()

    def _run_next_workflow_stage(self) -> None:
        if not self._workflow_queue:
            self._workflow_current_stage = None
            self._workflow_current_job_id = None
            self._update_workflow_status_label(
                f"Quy trÃ¬nh nhanh: Ä‘Ã£ hoÃ n táº¥t {self._workflow_name or 'quy trÃ¬nh'}"
            )
            self._workflow_name = None
            return

        next_stage = self._workflow_queue[0]
        runner = self._workflow_stage_runner(next_stage)
        if runner is None:
            self._stop_workflow(
                message=f"Quy trÃ¬nh nhanh Ä‘Ã£ dá»«ng: khÃ´ng tÃ¬m tháº¥y tÃ¡c vá»¥ xá»­ lÃ½ cho bÆ°á»›c {next_stage}",
            )
            return

        job_id = runner()
        if not job_id:
            self._stop_workflow(
                message=(
                    f"Quy trÃ¬nh nhanh Ä‘Ã£ dá»«ng á»Ÿ bÆ°á»›c {self._workflow_stage_label(next_stage)}. "
                    "HÃ£y bá»• sung dá»¯ liá»‡u hoáº·c cáº¥u hÃ¬nh rá»“i cháº¡y láº¡i."
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
                f"Quy trÃ¬nh nhanh: Ä‘Ã£ hoÃ n táº¥t {self._workflow_name or 'quy trÃ¬nh'}"
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
            self._update_workflow_status_label(message or "Quy trÃ¬nh nhanh: Ä‘Ã£ dá»«ng hÃ ng Ä‘á»£i hiá»‡n táº¡i")

    def _reload_subtitle_editor_from_db(self, *, force: bool = False) -> None:
        if not self._current_workspace:
            self._subtitle_table.setRowCount(0)
            self._subtitle_segment_snapshot = {}
            self._subtitle_editor_dirty = False
            self._subtitle_editor_status.setText("ChÆ°a má»Ÿ dá»± Ã¡n")
            return
        if self._subtitle_editor_dirty and not force:
            self._subtitle_editor_status.setText("TrÃ¬nh biÃªn táº­p cÃ³ thay Ä‘á»•i chÆ°a lÆ°u. HÃ£y lÆ°u hoáº·c náº¡p láº¡i thá»§ cÃ´ng.")
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
        self._subtitle_editor_status.setText(
            f"ÄÃ£ náº¡p {len(subtitle_rows)} dÃ²ng tá»« {self._subtitle_track_label(active_track)} vÃ o trÃ¬nh biÃªn táº­p"
        )
        if subtitle_rows:
            self._schedule_preview_reload()

    def _handle_subtitle_item_changed(self, _item: QTableWidgetItem) -> None:
        if self._subtitle_editor_loading:
            return
        self._mark_subtitle_editor_dirty("TrÃ¬nh biÃªn táº­p cÃ³ thay Ä‘á»•i chÆ°a lÆ°u")

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
                raise ValueError(f"DÃ²ng {row_index + 1}: thá»i gian káº¿t thÃºc pháº£i lá»›n hÆ¡n thá»i gian báº¯t Ä‘áº§u.")
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
        self._subtitle_qc_summary.setText("QC phá»¥ Ä‘á» chÆ°a Ä‘Æ°á»£c cháº¡y")
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
            "QC phá»¥ Ä‘á»:\n"
            f"- Tá»•ng sá»‘ dÃ²ng: {report.total_segments}\n"
            f"- Lá»—i: {report.error_count}\n"
            f"- Cáº£nh bÃ¡o: {report.warning_count}\n"
            f"- DÃ²ng Ä‘áº¡t chuáº©n: {report.ok_count}\n"
            "- Luáº­t máº·c Ä‘á»‹nh: tá»‘i Ä‘a 2 dÃ²ng, 42 CPL, 18 CPS, thá»i lÆ°á»£ng 800-7000 ms"
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
            QMessageBox.warning(self, "QC phá»¥ Ä‘á»", str(exc))
            return
        report = analyze_subtitle_rows(rows, config=SubtitleQcConfig())
        self._apply_qc_report_to_ui(report)
        self._append_log_line(
            f"QC phá»¥ Ä‘á»: {report.error_count} lá»—i, {report.warning_count} cáº£nh bÃ¡o, {report.total_segments} dÃ²ng"
        )

    def _apply_shift_to_subtitle_rows(self) -> None:
        if self._subtitle_table.rowCount() == 0:
            return
        try:
            shift_ms = int(self._shift_input.text().strip() or "0")
        except ValueError:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "Äá»™ dá»‹ch (ms) pháº£i lÃ  sá»‘ nguyÃªn.")
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
            f"ÄÃ£ dá»‹ch toÃ n bá»™ má»‘c thá»i gian {shift_ms} ms. HÃ£y lÆ°u Ä‘á»ƒ ghi vÃ o CSDL."
        )

    def _apply_find_replace(self) -> None:
        needle = self._find_input.text()
        if not needle:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "HÃ£y nháº­p chuá»—i cáº§n tÃ¬m.")
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
            self._subtitle_editor_status.setText("KhÃ´ng tÃ¬m tháº¥y chuá»—i cáº§n thay trong trÃ¬nh biÃªn táº­p")
            return
        self._mark_subtitle_editor_dirty(
            f"ÄÃ£ thay {replacement_count} lÆ°á»£t. HÃ£y lÆ°u Ä‘á»ƒ ghi vÃ o CSDL."
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
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi xem trÆ°á»›c.")
            return

        workspace = self._current_workspace
        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "ChÆ°a cÃ³ video", "HÃ£y chá»n video nguá»“n há»£p lá»‡.")
            return
        if not self._selected_export_preset():
            QMessageBox.warning(self, "Preset xuáº¥t", "KhÃ´ng tÃ¬m tháº¥y preset xuáº¥t trong dá»± Ã¡n.")
            return

        _database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR vÃ  dá»‹ch trÆ°á»›c.")
            return

        try:
            self._cancel_preview_reload()
            ass_path = export_preview_subtitles(workspace, segments=subtitle_rows, format_name="ass")
            self._live_preview_ass_path = ass_path
            self._last_subtitle_outputs["ass"] = ass_path
            self._preview_controller.preview(
                source_video_path=source_video_path,
                subtitle_path=ass_path,
                mpv_dll_path=self._settings.dependency_paths.mpv_dll_path,
                start_ms=self._selected_subtitle_start_ms() if start_from_selected else 0,
            )
        except (PreviewUnavailableError, FileNotFoundError, RuntimeError) as exc:
            QMessageBox.warning(self, "Xem trÆ°á»›c mpv", str(exc))
            return

        self._subtitle_editor_status.setText(
            "Äang má»Ÿ preview mpv cho "
            f"{self._subtitle_track_label(active_track)}"
            + (" tá»« dÃ²ng Ä‘ang chá»n" if start_from_selected else " tá»« Ä‘áº§u video")
        )
        self._refresh_workspace_views()
        self._append_log_line(f"Mo preview mpv voi ASS: {ass_path}")

    def _save_subtitle_edits(self, checked: bool = False, *, silent: bool = False) -> bool:
        del checked
        if not self._current_workspace:
            if not silent:
                QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return False
        if not self._subtitle_editor_dirty:
            self._subtitle_editor_status.setText("KhÃ´ng cÃ³ thay Ä‘á»•i cáº§n lÆ°u")
            return True

        try:
            rows = self._collect_subtitle_table_rows()
        except ValueError as exc:
            if not silent:
                QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", str(exc))
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
            self._append_log_line(f"ÄÃ£ tÃ¡ch track phá»¥ Ä‘á» chuáº©n sang track chá»‰nh sá»­a: {track_label}")
        self._append_log_line(f"ÄÃ£ lÆ°u track phá»¥ Ä‘á» {track_label} vá»›i {len(records)} dÃ²ng vÃ o CSDL")
        if downstream_artifacts_stale:
            self._append_log_line("ÄÃ£ xoÃ¡ tráº¡ng thÃ¡i TTS/track giá»ng/audio trá»™n cÅ© vÃ¬ track phá»¥ Ä‘á» Ä‘Ã£ thay Ä‘á»•i.")
        if not silent:
            QMessageBox.information(
                self,
                "BiÃªn táº­p phá»¥ Ä‘á»",
                (
                    f"ÄÃ£ lÆ°u track phá»¥ Ä‘á» {track_label} ({len(records)} dÃ²ng)."
                    + (" Track chuáº©n Ä‘Ã£ Ä‘Æ°á»£c tÃ¡ch thÃ nh track chá»‰nh sá»­a riÃªng." if forked_from_canonical else "")
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
        self._mark_subtitle_editor_dirty("ÄÃ£ chÃ©p báº£n dá»‹ch sang cá»™t phá»¥ Ä‘á». HÃ£y lÆ°u Ä‘á»ƒ ghi vÃ o CSDL.")

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
                f"ÄÃ£ táº¡o Lá»i TTS tá»« phá»¥ Ä‘á» cho {updated_count} dÃ²ng. HÃ£y lÆ°u Ä‘á»ƒ ghi vÃ o CSDL."
            )
        else:
            self._subtitle_editor_status.setText("Lá»i TTS hiá»‡n táº¡i Ä‘Ã£ khá»›p vá»›i phá»¥ Ä‘á» hoáº·c chÆ°a cÃ³ ná»™i dung Ä‘á»ƒ táº¡o.")

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
                f"ÄÃ£ lÃ m mÆ°á»£t Lá»i TTS cho {updated_count} dÃ²ng. HÃ£y lÆ°u Ä‘á»ƒ ghi vÃ o CSDL."
            )
        else:
            self._subtitle_editor_status.setText("KhÃ´ng cÃ³ Lá»i TTS nÃ o cáº§n lÃ m mÆ°á»£t thÃªm.")

    def _choose_bgm_file(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chá»n tá»‡p BGM",
            str(Path.cwd()),
            "Tá»‡p Ã¢m thanh (*.wav *.mp3 *.m4a *.aac *.flac *.ogg);;Táº¥t cáº£ tá»‡p (*.*)",
        )
        if file_path:
            self._bgm_path_input.setText(file_path)

    def _choose_vieneu_ref_audio_file(self) -> None:
        initial_dir = self._current_workspace.root_dir if self._current_workspace else Path.cwd()
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chá»n audio máº«u cho VieNeu clone",
            str(initial_dir),
            "Tá»‡p Ã¢m thanh (*.wav *.mp3 *.m4a *.aac *.flac *.ogg);;Táº¥t cáº£ tá»‡p (*.*)",
        )
        if file_path:
            self._vieneu_ref_audio_input.setText(file_path)
            self._refresh_workspace_views()

    def _choose_watermark_file(self) -> None:
        initial_dir = self._current_workspace.root_dir if self._current_workspace else Path.cwd()
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chá»n watermark hoáº·c logo",
            str(initial_dir),
            "Tá»‡p áº£nh (*.png *.webp *.jpg *.jpeg);;Táº¥t cáº£ tá»‡p (*.*)",
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
            self._voice_info.setText("Runtime giá»ng Ä‘á»c: chÆ°a má»Ÿ dá»± Ã¡n")
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
            voice_lines.append("Giá»ng SAPI Ä‘Ã£ phÃ¡t hiá»‡n:")
            voice_lines.extend(f"- {name}" for name in self._installed_sapi_voices)
        else:
            voice_lines.append("Giá»ng SAPI Ä‘Ã£ phÃ¡t hiá»‡n: khÃ´ng Ä‘á»c Ä‘Æ°á»£c hoáº·c há»‡ thá»‘ng chÆ°a cÃ³ giá»ng")
        version_suffix = (
            f" v{self._vieneu_environment.package_version}" if self._vieneu_environment.package_version else ""
        )
        if self._vieneu_environment.package_installed:
            voice_lines.append(f"VieNeu SDK{version_suffix}: Ä‘Ã£ cÃ i")
        else:
            voice_lines.append("VieNeu SDK: chÆ°a cÃ i package `vieneu`")
        if self._vieneu_environment.espeak_path:
            voice_lines.append(f"eSpeak NG: {self._vieneu_environment.espeak_path}")
        else:
            voice_lines.append("eSpeak NG: chÆ°a tÃ¬m tháº¥y, VieNeu local sáº½ chÆ°a cháº¡y Ä‘Æ°á»£c")
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
                    field_name="Táº§n sá»‘ máº«u",
                    minimum=8000,
                    default=preset.sample_rate or 24000,
                    strict=strict,
                ),
                "speed": self._parse_voice_float_value(
                    self._voice_speed_profile_input.text(),
                    field_name="Tá»‘c Ä‘á»™",
                    minimum=0.1,
                    maximum=4.0,
                    default=preset.speed or 1.0,
                    strict=strict,
                ),
                "volume": self._parse_voice_float_value(
                    self._voice_profile_volume_input.text(),
                    field_name="Ã‚m lÆ°á»£ng giá»ng",
                    minimum=0.0,
                    maximum=4.0,
                    default=preset.volume or 1.0,
                    strict=strict,
                ),
                "pitch": self._parse_voice_float_value(
                    self._voice_pitch_input.text(),
                    field_name="Cao Ä‘á»™",
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
            self._voice_profile_status.setText("ChÆ°a cÃ³ preset giá»ng Ä‘á»ƒ chá»‰nh sá»­a")
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
            self._voice_clone_status.setText("Preset nÃ y khÃ´ng dÃ¹ng cháº¿ Ä‘á»™ VieNeu clone")
        self._voice_profile_status.setText(
            f"Äang sá»­a preset {preset.name}. Báº¥m 'LÆ°u preset' hoáº·c 'LÆ°u thÃ nh báº£n má»›i' Ä‘á»ƒ Ã¡p dá»¥ng."
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
            self._voice_clone_status.setText(f"KhÃ´ng tÃ¬m tháº¥y audio máº«u clone: {resolved_audio_path} ({mode})")
        elif ref_audio_path and ref_text:
            transcript_size = len(ref_text.split())
            self._voice_clone_status.setText(
                f"Clone sáºµn sÃ ng ({mode}) - máº«u {transcript_size} tá»«"
            )
        elif ref_audio_path:
            self._voice_clone_status.setText(f"ÄÃ£ cÃ³ audio máº«u, cáº§n thÃªm vÄƒn báº£n máº«u Ä‘Ãºng 100% ({mode})")
        else:
            self._voice_clone_status.setText(
                f"Clone chÆ°a Ä‘Æ°á»£c cáº¥u hÃ¬nh ({mode}). Gá»£i Ã½: dÃ¹ng audio sáº¡ch 10-30 giÃ¢y vÃ  transcript khá»›p tuyá»‡t Ä‘á»‘i."
            )

    def _sync_voice_preset_form(self) -> None:
        preset = self._base_selected_voice_preset()
        if not preset:
            self._voice_preset_notes.setText("ChÆ°a cÃ³ preset giá»ng")
            self._voice_profile_status.setText("TrÃ¬nh quáº£n lÃ½ preset giá»ng chÆ°a cÃ³ preset")
            self._voice_clone_status.setText("Preset clone chÆ°a Ä‘Æ°á»£c cáº¥u hÃ¬nh")
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

        self._voice_preset_notes.setText(preset.notes or "KhÃ´ng cÃ³ ghi chÃº cho preset nÃ y")
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
            self._voice_clone_status.setText("Preset nÃ y khÃ´ng dÃ¹ng cháº¿ Ä‘á»™ VieNeu clone")
        else:
            self._update_vieneu_clone_status(mode=str(mode))
        self._voice_profile_status.setText(
            "TrÃ¬nh quáº£n lÃ½ preset giá»ng Ä‘Ã£ sáºµn sÃ ng. Báº¡n cÃ³ thá»ƒ sá»­a, nhÃ¢n báº£n, xÃ³a hoáº·c nháº­p hÃ ng loáº¡t."
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
            self._set_speaker_binding_row_status(row_index, status_text="ChÆ°a gÃ¡n", status_kind="unbound")
            return
        if preset_id and preset_id not in preset_ids:
            self._set_speaker_binding_row_status(
                row_index,
                status_text="Preset Ä‘Ã£ gÃ¡n khÃ´ng cÃ²n tá»“n táº¡i",
                status_kind="missing",
            )
            return
        self._set_speaker_binding_row_status(
            row_index,
            status_text="ÄÃ£ gÃ¡n preset riÃªng",
            status_kind="ok",
        )

    def _refresh_speaker_binding_status_summary(self) -> None:
        row_count = self._speaker_binding_table.rowCount()
        if not self._current_workspace:
            self._speaker_binding_status.setText("Speaker binding: chÆ°a má»Ÿ dá»± Ã¡n")
            return
        if row_count == 0:
            self._speaker_binding_status.setText("Speaker binding: chÆ°a cÃ³ speaker tá»« Contextual V2")
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
                f"Speaker binding: cÃ³ {row_count} speaker nháº­n diá»‡n. Náº¿u chÆ°a lÆ°u binding nÃ o, toÃ n bá»™ track sáº½ dÃ¹ng preset máº·c Ä‘á»‹nh."
            )
            return

        details: list[str] = [f"{bound_count}/{row_count} speaker Ä‘Ã£ cÃ³ preset riÃªng."]
        if missing_count:
            details.append(f"{missing_count} binding Ä‘ang trá» tá»›i preset khÃ´ng cÃ²n tá»“n táº¡i.")
        if unresolved_count:
            details.append("TTS sáº½ bá»‹ cháº·n cho Ä‘áº¿n khi gÃ¡n Ä‘á»§ cÃ¡c speaker Ä‘Ã£ nháº­n diá»‡n.")
        else:
            details.append("ÄÃ£ Ä‘á»§ binding cho cÃ¡c speaker Ä‘Ã£ nháº­n diá»‡n.")
        self._speaker_binding_status.setText("Speaker binding: " + " ".join(details))

    def _set_speaker_binding_form_dirty(self, is_dirty: bool) -> None:
        self._speaker_binding_dirty = is_dirty
        marker = " Form hiá»‡n cÃ³ thay Ä‘á»•i chÆ°a lÆ°u."
        status_text = self._speaker_binding_status.text().strip()
        if status_text:
            if is_dirty and marker not in status_text:
                self._speaker_binding_status.setText(status_text + marker)
            elif not is_dirty and marker in status_text:
                self._speaker_binding_status.setText(status_text.replace(marker, ""))
        if is_dirty:
            self._speaker_binding_hint.setText(
                "LÆ°u Ã½: báº¡n Ä‘ang cÃ³ thay Ä‘á»•i speaker binding trÃªn form nhÆ°ng chÆ°a lÆ°u. "
                "TTS/export chá»‰ dÃ¹ng mapping Ä‘Ã£ lÆ°u trong dá»± Ã¡n."
            )
        else:
            self._speaker_binding_hint.setText(
                "Máº¹o: náº¿u Ä‘Ã£ lÆ°u Ã­t nháº¥t 1 speaker binding, má»i speaker nháº­n diá»‡n rÃµ trong track hiá»‡n táº¡i "
                "pháº£i Ä‘Æ°á»£c gÃ¡n preset. Speaker unknown váº«n dÃ¹ng preset máº·c Ä‘á»‹nh."
            )
        self._sync_voice_summary_with_binding_form_state()

    def _sync_voice_summary_with_binding_form_state(self) -> None:
        summary_text = self._voice_summary.text().strip()
        if not summary_text:
            return
        lines = [
            line
            for line in summary_text.splitlines()
            if not line.startswith("- Speaker binding trÃªn form:")
            and not line.startswith("- Voice policy trÃªn form:")
        ]
        if self._speaker_binding_dirty and self._speaker_binding_table.rowCount() > 0:
            lines.append("- Speaker binding trÃªn form: cÃ³ thay Ä‘á»•i chÆ°a lÆ°u; hÃ£y báº¥m LÆ°u binding Ä‘á»ƒ Ã¡p dá»¥ng")
        if self._voice_policy_dirty and (
            self._character_voice_policy_table.rowCount() > 0 or self._relationship_voice_policy_table.rowCount() > 0
        ):
            lines.append("- Voice policy trÃªn form: cÃ³ thay Ä‘á»•i chÆ°a lÆ°u; hÃ£y báº¥m LÆ°u voice policy Ä‘á»ƒ Ã¡p dá»¥ng")
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
            QMessageBox.warning(self, "Speaker binding", "HÃ£y chá»n preset giá»ng máº·c Ä‘á»‹nh trÆ°á»›c.")
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
            self._append_log_line(f"ÄÃ£ gÃ¡n preset hiá»‡n táº¡i cho {changed} speaker chÆ°a cÃ³ binding")
        else:
            QMessageBox.information(self, "Speaker binding", "KhÃ´ng cÃ³ speaker trá»‘ng nÃ o Ä‘á»ƒ gÃ¡n nhanh.")

    def _fill_selected_speaker_bindings_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Speaker binding", "HÃ£y chá»n preset giá»ng máº·c Ä‘á»‹nh trÆ°á»›c.")
            return
        selected_rows = self._selected_table_row_indexes(self._speaker_binding_table)
        if not selected_rows:
            QMessageBox.warning(self, "Speaker binding", "HÃ£y chá»n Ã­t nháº¥t má»™t dÃ²ng speaker trÆ°á»›c.")
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
            self._append_log_line(f"ÄÃ£ gÃ¡n preset hiá»‡n táº¡i cho {changed}/{len(selected_rows)} speaker Ä‘Æ°á»£c chá»n")
        else:
            QMessageBox.information(self, "Speaker binding", "CÃ¡c speaker Ä‘Ã£ chá»n Ä‘Ã£ dÃ¹ng preset nÃ y hoáº·c khÃ´ng Ä‘á»•i Ä‘Æ°á»£c.")

    def _clear_selected_speaker_bindings(self) -> None:
        selected_rows = self._selected_table_row_indexes(self._speaker_binding_table)
        if not selected_rows:
            QMessageBox.warning(self, "Speaker binding", "HÃ£y chá»n Ã­t nháº¥t má»™t dÃ²ng speaker trÆ°á»›c.")
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
            self._append_log_line(f"ÄÃ£ xÃ³a preset trÃªn {changed}/{len(selected_rows)} speaker Ä‘Æ°á»£c chá»n")
        else:
            QMessageBox.information(self, "Speaker binding", "CÃ¡c speaker Ä‘Ã£ chá»n hiá»‡n Ä‘ang Ä‘á»ƒ trá»‘ng.")

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
            self._speaker_binding_status.setText("Speaker binding: chÆ°a má»Ÿ dá»± Ã¡n")
            self._speaker_binding_loading = False
            self._set_speaker_binding_form_dirty(False)
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        if not analysis_rows:
            self._speaker_binding_status.setText("Speaker binding: chÆ°a cÃ³ speaker tá»« Contextual V2")
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
            combo.addItem("ChÆ°a gÃ¡n", "")
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
            self._speaker_binding_status.setText("Speaker binding: chÆ°a cÃ³ speaker nháº­n diá»‡n Ä‘á»§ rÃµ Ä‘á»ƒ gÃ¡n")
            self._speaker_binding_loading = False
            self._set_speaker_binding_form_dirty(False)
            return
        self._speaker_binding_loading = False
        self._refresh_speaker_binding_status_summary()
        self._set_speaker_binding_form_dirty(False)

    def _save_speaker_bindings(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Speaker binding", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
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
        self._append_log_line(f"ÄÃ£ lÆ°u {len(bindings)} speaker binding")
        QMessageBox.information(
            self,
            "Speaker binding",
            (
                f"ÄÃ£ lÆ°u {len(bindings)} speaker binding.\n"
                "- Náº¿u Ä‘Ã£ thay Ä‘á»•i mapping giá»ng, hÃ£y cháº¡y láº¡i TTS rá»“i táº¡o láº¡i track giá»ng."
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
                    field_name="Tá»‘c Ä‘á»™ policy",
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
                    field_name="Ã‚m lÆ°á»£ng policy",
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
                    field_name="Cao Ä‘á»™ policy",
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
        marker = " Form hiá»‡n cÃ³ thay Ä‘á»•i chÆ°a lÆ°u."
        status_text = self._voice_policy_status.text().strip()
        if status_text:
            if is_dirty and marker not in status_text:
                self._voice_policy_status.setText(status_text + marker)
            elif not is_dirty and marker in status_text:
                self._voice_policy_status.setText(status_text.replace(marker, ""))
        if is_dirty:
            self._voice_policy_hint.setText(
                "LÆ°u Ã½: voice policy trÃªn form Ä‘ang cÃ³ thay Ä‘á»•i chÆ°a lÆ°u. Runtime chá»‰ dÃ¹ng policy Ä‘Ã£ lÆ°u trong dá»± Ã¡n."
            )
        else:
            self._voice_policy_hint.setText(
                "Máº¹o: voice policy lÃ  fallback má»m. Quan há»‡ speaker->listener sáº½ Æ°u tiÃªn hÆ¡n policy theo nhÃ¢n váº­t, nhÆ°ng speaker binding váº«n lÃ  má»©c Æ°u tiÃªn cao nháº¥t."
            )
        self._sync_voice_summary_with_binding_form_state()

    def _handle_voice_policy_selection_changed(self, table: QTableWidget, row_index: int) -> None:
        if self._voice_policy_loading:
            return
        self._update_voice_policy_row_status(table, row_index)
        self._refresh_voice_policy_status_summary()
        self._set_voice_policy_form_dirty(True)

    def _fill_unbound_voice_policies_with_selected_preset(self) -> None:
        preset_id = str(self._voice_combo.currentData() or "").strip()
        if not preset_id:
            QMessageBox.warning(self, "Voice policy", "HÃ£y chá»n preset giá»ng máº·c Ä‘á»‹nh trÆ°á»›c.")
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
            self._append_log_line(f"ÄÃ£ gÃ¡n preset hiá»‡n táº¡i cho {changed} hÃ ng voice policy cÃ²n trá»‘ng")
        else:
            QMessageBox.information(self, "Voice policy", "KhÃ´ng cÃ³ hÃ ng policy trá»‘ng nÃ o Ä‘á»ƒ gÃ¡n nhanh.")

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
            QMessageBox.warning(self, "Voice policy", "HÃ£y chá»n preset giá»ng máº·c Ä‘á»‹nh trÆ°á»›c.")
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
            QMessageBox.warning(self, "Voice policy", "HÃ£y chá»n Ã­t nháº¥t má»™t dÃ²ng policy trÆ°á»›c.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"ÄÃ£ gÃ¡n preset hiá»‡n táº¡i cho {changed}/{selected_count} dÃ²ng voice policy Ä‘Æ°á»£c chá»n")
        else:
            QMessageBox.information(self, "Voice policy", "CÃ¡c dÃ²ng Ä‘Ã£ chá»n Ä‘Ã£ dÃ¹ng preset nÃ y hoáº·c khÃ´ng Ä‘á»•i Ä‘Æ°á»£c.")

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
            QMessageBox.warning(self, "Voice policy", "HÃ£y chá»n Ã­t nháº¥t má»™t dÃ²ng policy trÆ°á»›c.")
            return
        self._refresh_voice_policy_status_summary()
        if changed:
            self._set_voice_policy_form_dirty(True)
            self._append_log_line(f"ÄÃ£ xÃ³a preset trÃªn {changed}/{selected_count} dÃ²ng voice policy Ä‘Æ°á»£c chá»n")
        else:
            QMessageBox.information(self, "Voice policy", "CÃ¡c dÃ²ng Ä‘Ã£ chá»n hiá»‡n Ä‘ang Ä‘á»ƒ trá»‘ng.")

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

    def _reload_voice_policies(self) -> None:
        self._voice_policy_loading = True
        self._character_voice_policy_table.setRowCount(0)
        self._relationship_voice_policy_table.setRowCount(0)
        if not self._current_workspace:
            self._voice_policy_status.setText("Voice policy: chÆ°a má»Ÿ dá»± Ã¡n")
            self._voice_policy_loading = False
            self._set_voice_policy_form_dirty(False)
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        analysis_rows = database.list_segment_analyses(self._current_workspace.project_id)
        if not analysis_rows:
            self._voice_policy_status.setText("Voice policy: chÆ°a cÃ³ dá»¯ liá»‡u Contextual V2")
            self._voice_policy_loading = False
            self._set_voice_policy_form_dirty(False)
            return

        character_name_map = self._character_name_map(database)
        relationship_rows = database.list_relationship_profiles(self._current_workspace.project_id)
        voice_policy_rows = database.list_voice_policies(self._current_workspace.project_id)
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
            combo.addItem("ChÆ°a gÃ¡n", "")
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
                    placeholder="Máº·c Ä‘á»‹nh",
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
                    placeholder="Máº·c Ä‘á»‹nh",
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
                    placeholder="Máº·c Ä‘á»‹nh",
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
            combo.addItem("ChÆ°a gÃ¡n", "")
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
                    placeholder="Máº·c Ä‘á»‹nh",
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
                    placeholder="Máº·c Ä‘á»‹nh",
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
                    placeholder="Máº·c Ä‘á»‹nh",
                ),
            )
            self._update_voice_policy_row_status(
                self._relationship_voice_policy_table,
                row_index,
                available_preset_ids=available_preset_ids,
            )

        self._voice_policy_loading = False
        self._refresh_voice_policy_status_summary()
        self._set_voice_policy_form_dirty(False)

    def _save_voice_policies(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Voice policy", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        now = utc_now_iso()
        policies: list[VoicePolicyRecord] = []
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
        database.replace_voice_policies(self._current_workspace.project_id, policies)
        self._set_voice_policy_form_dirty(False)
        self._invalidate_subtitle_pipeline_outputs(clear_tts_audio=True)
        self._reload_voice_policies()
        self._refresh_workspace_views()
        self._append_log_line(f"ÄÃ£ lÆ°u {len(policies)} voice policy")
        QMessageBox.information(
            self,
            "Voice policy",
            (
                f"ÄÃ£ lÆ°u {len(policies)} voice policy.\n"
                "- Náº¿u Ä‘Ã£ thay Ä‘á»•i policy giá»ng, hÃ£y cháº¡y láº¡i TTS rá»“i táº¡o láº¡i track giá»ng."
            ),
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
                QMessageBox.warning(self, dialog_title, "KhÃ´ng tÃ¬m tháº¥y preset giá»ng trong dá»± Ã¡n.")
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
        analysis_rows = (
            database.list_segment_analyses(self._current_workspace.project_id) if self._current_workspace else []
        )
        plan = build_speaker_binding_plan(
            subtitle_rows=voice_rows,
            analysis_rows=analysis_rows,
            binding_rows=binding_rows,
            voice_policy_rows=voice_policy_rows,
            available_preset_ids=set(available_presets),
        )
        if not plan.active_bindings and not getattr(plan, "active_voice_policies", False):
            return default_preset, None, plan.segment_speaker_keys or None, plan

        if plan.missing_preset_ids or plan.unresolved_speakers:
            if warn_on_unresolved:
                lines = ["Voice policy/binding hiá»‡n chÆ°a Ä‘áº§y Ä‘á»§, chÆ°a thá»ƒ cháº¡y TTS an toÃ n."]
                if plan.unresolved_speakers:
                    lines.append(f"- Speaker chÆ°a gÃ¡n preset: {', '.join(plan.unresolved_speakers)}")
                if plan.missing_preset_ids:
                    lines.append(f"- Preset khÃ´ng cÃ²n tá»“n táº¡i: {', '.join(plan.missing_preset_ids)}")
                lines.append("- HÃ£y vÃ o tab Lá»“ng tiáº¿ng, hoÃ n táº¥t speaker binding/voice policy rá»“i thá»­ láº¡i.")
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
            raise ValueError(f"{field_name} pháº£i lÃ  sá»‘")
        if minimum <= value <= maximum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} pháº£i náº±m trong khoáº£ng {minimum}..{maximum}")

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
            raise ValueError(f"{field_name} pháº£i lÃ  sá»‘ nguyÃªn")
        if value >= minimum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} pháº£i >= {minimum}")

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
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        base_preset = self._base_selected_voice_preset()
        name = self._voice_profile_name_input.text().strip() or (base_preset.name if base_preset else "")
        if not name:
            QMessageBox.warning(self, "Preset giá»ng", "HÃ£y nháº­p tÃªn preset giá»ng trÆ°á»›c khi lÆ°u.")
            return
        try:
            preset = self._selected_voice_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Preset giá»ng", str(exc))
            return
        if not preset:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y preset giá»ng Ä‘á»ƒ lÆ°u.")
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
        self._append_log_line(f"ÄÃ£ lÆ°u preset giá»ng: {output_path}")
        QMessageBox.information(self, "Preset giá»ng", f"ÄÃ£ lÆ°u preset táº¡i:\n{output_path}")

    def _delete_selected_voice_preset(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        preset = self._base_selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y preset Ä‘á»ƒ xÃ³a.")
            return
        if len(self._voice_presets) <= 1:
            QMessageBox.warning(self, "Preset giá»ng", "Dá»± Ã¡n pháº£i cÃ²n Ã­t nháº¥t 1 preset giá»ng.")
            return
        answer = QMessageBox.question(
            self,
            "XÃ³a preset giá»ng",
            f"XÃ³a preset '{preset.name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return

        deleted_path = delete_voice_preset(self._current_workspace.root_dir, preset.voice_preset_id)
        if not deleted_path:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y file preset Ä‘á»ƒ xÃ³a.")
            return
        self._reload_voice_presets()
        resolved_preset_id = self._set_voice_combo_to_preset(None)
        if resolved_preset_id:
            self._persist_active_voice_preset_id(resolved_preset_id)
        self._sync_voice_preset_form()
        self._refresh_workspace_views()
        self._append_log_line(f"ÄÃ£ xÃ³a preset giá»ng: {deleted_path}")
        QMessageBox.information(self, "Preset giá»ng", f"ÄÃ£ xÃ³a preset táº¡i:\n{deleted_path}")

    def _batch_import_voice_profiles(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        try:
            template_preset = self._selected_voice_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Nháº­p hÃ ng loáº¡t", str(exc))
            return
        if template_preset is None:
            QMessageBox.warning(self, "Nháº­p hÃ ng loáº¡t", "KhÃ´ng tÃ¬m tháº¥y preset máº«u.")
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
                "Nháº­p hÃ ng loáº¡t",
                "KhÃ´ng import Ä‘Æ°á»£c profile nÃ o. HÃ£y Ä‘áº·t file audio máº«u trong `assets/voices` kÃ¨m file `.txt` cÃ¹ng tÃªn.",
            )
            return

        self._reload_voice_presets()
        first_preset_id = report.imported_presets[0].voice_preset_id
        self._set_voice_combo_to_preset(first_preset_id)
        self._persist_active_voice_preset_id(first_preset_id)
        self._sync_voice_preset_form()
        self._refresh_workspace_views()
        self._voice_profile_status.setText(
            "ÄÃ£ nháº­p hÃ ng loáº¡t "
            f"{len(report.imported_presets)} preset; thiáº¿u `.txt`={len(report.skipped_missing_text)}; "
            f"`.txt` rá»—ng={len(report.skipped_empty_text)}"
        )
        self._append_log_line(
            "Nháº­p hÃ ng loáº¡t preset giá»ng: "
            f"da_nhap={len(report.imported_presets)} "
            f"thieu_txt={len(report.skipped_missing_text)} "
            f"txt_rong={len(report.skipped_empty_text)}"
        )
        QMessageBox.information(
            self,
            "Nháº­p hÃ ng loáº¡t",
            "ÄÃ£ nháº­p preset giá»ng:\n"
            f"- ÄÃ£ táº¡o: {len(report.imported_presets)}\n"
            f"- Thiáº¿u file `.txt`: {len(report.skipped_missing_text)}\n"
            f"- File `.txt` rá»—ng: {len(report.skipped_empty_text)}",
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
                notes="ChÆ°a cÃ³ profile watermark. Báº¡n cÃ³ thá»ƒ nháº­p thÃ´ng sá»‘ rá»“i lÆ°u thÃ nh profile má»›i.",
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
            self._watermark_profile_status.setText(profile.notes or "KhÃ´ng cÃ³ ghi chÃº cho profile watermark nÃ y")

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
                f"Äang sá»­a profile {base_profile.name}. Báº¥m 'LÆ°u profile' hoáº·c 'LÆ°u thÃ nh báº£n má»›i' Ä‘á»ƒ lÆ°u láº¡i."
            )
        else:
            self._watermark_profile_status.setText(
                "Äang sá»­a profile watermark táº¡m thá»i. Báº¥m 'LÆ°u thÃ nh báº£n má»›i' Ä‘á»ƒ tÃ¡i sá»­ dá»¥ng cho láº§n sau."
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
            raise ValueError(f"{field_name} pháº£i lÃ  sá»‘")
        if minimum <= value <= maximum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} pháº£i náº±m trong khoáº£ng {minimum}..{maximum}")

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
            raise ValueError(f"{field_name} pháº£i lÃ  sá»‘ nguyÃªn")
        if value >= minimum:
            return value
        if not strict:
            return default
        raise ValueError(f"{field_name} pháº£i >= {minimum}")

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
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return
        base_profile = self._base_selected_watermark_profile()
        name = self._watermark_profile_name_input.text().strip() or (
            base_profile.name if base_profile else ""
        )
        if not name:
            QMessageBox.warning(self, "Profile watermark", "HÃ£y nháº­p tÃªn profile trÆ°á»›c khi lÆ°u.")
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
            QMessageBox.warning(self, "Profile watermark", "KhÃ´ng cÃ³ dá»¯ liá»‡u Ä‘á»ƒ lÆ°u.")
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
        self._append_log_line(f"ÄÃ£ lÆ°u watermark profile: {output_path}")
        QMessageBox.information(self, "Profile watermark", f"ÄÃ£ lÆ°u profile táº¡i:\n{output_path}")

    @staticmethod
    def _parse_volume_value(raw_value: str, *, field_name: str) -> float:
        try:
            value = float(raw_value.strip() or "0")
        except ValueError as exc:
            raise ValueError(f"{field_name} pháº£i lÃ  sá»‘") from exc
        if value < 0:
            raise ValueError(f"{field_name} khÃ´ng Ä‘Æ°á»£c Ã¢m")
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
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "HÃ£y chá»n má»™t dÃ²ng Ä‘á»ƒ tÃ¡ch.")
            return
        try:
            rows = self._collect_subtitle_table_rows()
            first, second = split_editor_row(rows[row_index])
        except ValueError as exc:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", str(exc))
            return

        new_rows = rows[:row_index] + [first, second] + rows[row_index + 1 :]
        self._replace_subtitle_editor_rows(
            new_rows,
            status_message=f"ÄÃ£ tÃ¡ch dÃ²ng {row_index + 1} thÃ nh 2 Ä‘oáº¡n. HÃ£y lÆ°u Ä‘á»ƒ Ã¡p dá»¥ng.",
        )
        self._subtitle_table.selectRow(row_index)

    def _merge_selected_subtitle_row_with_next(self) -> None:
        row_index = self._selected_subtitle_row_index()
        if row_index < 0:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "HÃ£y chá»n má»™t dÃ²ng Ä‘á»ƒ gá»™p.")
            return
        try:
            rows = self._collect_subtitle_table_rows()
        except ValueError as exc:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", str(exc))
            return
        if row_index >= len(rows) - 1:
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "KhÃ´ng cÃ³ dÃ²ng tiáº¿p theo Ä‘á»ƒ gá»™p.")
            return

        merged = merge_editor_rows(rows[row_index], rows[row_index + 1])
        new_rows = rows[:row_index] + [merged] + rows[row_index + 2 :]
        self._replace_subtitle_editor_rows(
            new_rows,
            status_message=f"ÄÃ£ gá»™p dÃ²ng {row_index + 1} vá»›i dÃ²ng káº¿ tiáº¿p. HÃ£y lÆ°u Ä‘á»ƒ Ã¡p dá»¥ng.",
        )
        self._subtitle_table.selectRow(row_index)

    def _sync_settings_to_form(self) -> None:
        dependency_paths = self._settings.dependency_paths
        self._ui_language_input.setText(self._settings.ui_language)
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
        dependency_paths.ffmpeg_path = self._ffmpeg_path_input.text().strip() or None
        dependency_paths.ffprobe_path = self._ffprobe_path_input.text().strip() or None
        dependency_paths.mpv_dll_path = self._mpv_path_input.text().strip() or None
        self._settings.model_cache_dir = self._model_cache_input.text().strip() or None
        self._settings.openai_api_key = self._openai_key_input.text().strip() or None
        self._settings.default_translation_model = (
            self._default_translation_model_input.text().strip() or "gpt-4.1-mini"
        )
        save_settings(self._settings)
        self._append_log_line("ÄÃ£ lÆ°u cÃ i Ä‘áº·t")
        QMessageBox.information(self, "ÄÃ£ lÆ°u", "CÃ i Ä‘áº·t á»©ng dá»¥ng Ä‘Ã£ Ä‘Æ°á»£c lÆ°u.")

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
        self._append_log_line("Kiá»ƒm tra FFmpeg:\n" + text)

    def _run_probe_media_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None

        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "ChÆ°a cÃ³ video", "HÃ£y chá»n video nguá»“n há»£p lá»‡.")
            return None

        workspace = self._current_workspace
        ffprobe_path = self._settings.dependency_paths.ffprobe_path

        def handler(context: JobContext) -> JobResult:
            context.report_progress(5, "Äang Ä‘á»c metadata báº±ng ffprobe")
            metadata = probe_media(source_video_path, ffprobe_path=ffprobe_path)
            asset = attach_source_video_to_project(workspace, metadata)
            context.report_progress(100, "ÄÃ£ cáº­p nháº­t video nguá»“n")
            return JobResult(
                message="ÄÃ£ Ä‘á»c metadata video",
                extra={"metadata": metadata, "asset": asset},
            )

        return self._job_manager.submit_job(
            stage="probe_media",
            description="Äá»c metadata video nguá»“n vÃ  cáº­p nháº­t MediaAsset",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_extract_audio_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None

        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "ChÆ°a cÃ³ video", "HÃ£y chá»n video nguá»“n há»£p lá»‡.")
            return None

        workspace = self._current_workspace
        ffprobe_path = self._settings.dependency_paths.ffprobe_path
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path
        metadata_hint = self._media_metadata

        def handler(context: JobContext) -> JobResult:
            metadata = metadata_hint
            if metadata is None or metadata.source_path != source_video_path.resolve():
                context.report_progress(2, "Äang Ä‘á»c thÃ´ng tin video nguá»“n")
                metadata = probe_media(source_video_path, ffprobe_path=ffprobe_path)
                attach_source_video_to_project(workspace, metadata)
            if not metadata.primary_audio_stream:
                raise RuntimeError("Video khÃ´ng cÃ³ audio stream Ä‘á»ƒ tÃ¡ch.")

            artifacts = extract_audio_artifacts(
                context,
                workspace=workspace,
                metadata=metadata,
                ffmpeg_path=ffmpeg_path,
            )
            return JobResult(
                message="ÄÃ£ tÃ¡ch Ã¢m thanh 16 kHz vÃ  48 kHz",
                output_paths=[artifacts.audio_16k_path, artifacts.audio_48k_path],
                extra={"metadata": metadata, "artifacts": artifacts},
            )

        return self._job_manager.submit_job(
            stage="extract_audio",
            description="TÃ¡ch audio 16 kHz vÃ  48 kHz vÃ o bá»™ nhá»› Ä‘á»‡m",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_asr_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None

        workspace = self._current_workspace
        artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
        if not artifacts or not artifacts.audio_16k_path.exists():
            QMessageBox.warning(
                self,
                "ChÆ°a cÃ³ audio cho ASR",
                "HÃ£y tÃ¡ch audio trÆ°á»›c khi cháº¡y ASR.",
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
                message=f"ÄÃ£ lÆ°u {persisted.segment_count} phÃ¢n Ä‘oáº¡n ASR",
                output_paths=[persisted.segments_json_path],
                extra={"result": result, "persisted": persisted},
            )

        return self._job_manager.submit_job(
            stage="asr",
            description="Cháº¡y faster-whisper vÃ  lÆ°u phÃ¢n Ä‘oáº¡n vÃ o CSDL",
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
            self._review_summary.setText("ChÆ°a cÃ³ dá»± Ã¡n")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        if self._current_translation_mode(database.get_project()) != "contextual_v2":
            self._review_summary.setText("Dá»± Ã¡n nÃ y Ä‘ang dÃ¹ng cháº¿ Ä‘á»™ dá»‹ch legacy")
            return
        review_rows = database.list_review_queue_items(self._current_workspace.project_id)
        self._review_summary.setText(
            f"HÃ ng review semantic: {len(review_rows)} dÃ²ng cáº§n duyá»‡t trÆ°á»›c TTS/export"
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
                    f"TÃ³m táº¯t scene: {self._review_table.item(current_row, 1).text() if self._review_table.item(current_row, 1) else ''}",
                    f"LÃ½ do: {', '.join(str(item) for item in review_reason_codes)}",
                    f"CÃ¢u há»i review: {review_question}",
                    "",
                    "Ngá»¯ cáº£nh:",
                    *context_lines,
                ]
            )
        )
        self._review_context_text.setPlainText(
            "\n".join(
                [
                    f"Scene: {analysis_row['scene_id']}",
                    f"TÃ³m táº¯t scene: {scene_summary}",
                    f"LÃ½ do: {', '.join(str(item) for item in review_reason_codes)}",
                    f"CÃ¢u há»i review: {review_question}",
                    "",
                    "Ngá»¯ cáº£nh:",
                    *context_lines,
                ]
            )
        )
        review_reason_text = ", ".join(str(item) for item in review_reason_codes) or "Khong co"
        review_question_text = review_question or "Khong co"
        self._review_context_text.setPlainText(
            "\n".join(
                [
                    f"Scene: {analysis_row['scene_id']}",
                    f"TÃ³m táº¯t scene: {scene_summary}",
                    f"LÃ½ do review: {review_reason_text}",
                    f"CÃ¢u há»i review: {review_question_text}",
                    "",
                    "Ngá»¯ cáº£nh:",
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
                "ChÆ°a cÃ³ káº¿t quáº£ Contextual V2. HÃ£y cháº¡y dá»‹ch trÆ°á»›c khi tiáº¿p tá»¥c.",
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
                f"KhÃ´ng thá»ƒ tiáº¿p tá»¥c vÃ¬ cÃ²n {len(pending_rows)} dÃ²ng chÆ°a qua semantic review/QC.\n"
                "- HÃ£y xá»­ lÃ½ cÃ¡c dÃ²ng trong báº£ng Review Ngá»¯ Cáº£nh trÆ°á»›c khi cháº¡y TTS hoáº·c export."
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
            QMessageBox.warning(self, "Review", "HÃ£y chá»n má»™t dÃ²ng review trÆ°á»›c.")
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
            QMessageBox.information(self, "Review", "KhÃ´ng tÃ¬m tháº¥y dÃ²ng nÃ o phÃ¹ há»£p Ä‘á»ƒ chá»n.")
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
            QMessageBox.warning(self, "Review", "HÃ£y chá»n Ã­t nháº¥t má»™t dÃ²ng review trÆ°á»›c.")
            return
        self._run_review_resolution(scope="line", explicit_segment_ids=selected_segment_ids)

    def _run_review_resolution(self, *, scope: str, explicit_segment_ids: list[str] | None = None) -> None:
        if not self._current_workspace:
            return
        segment_id = self._pending_review_segment_id
        if not segment_id:
            QMessageBox.warning(self, "Review", "HÃ£y chá»n má»™t dÃ²ng review trÆ°á»›c.")
            return
        database = ProjectDatabase(self._current_workspace.database_path)
        project_row = database.get_project()
        if project_row is None:
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
        resolution_label = "dong chon" if explicit_segment_ids else scope
        self._append_log_line(f"Da ap review resolution cho {updated_count} dong ({resolution_label})")

    def _apply_review_resolution(self, scope: str) -> None:
        self._run_review_resolution(scope=scope)
        return
        if not self._current_workspace:
            return
        segment_id = self._pending_review_segment_id
        if not segment_id:
            QMessageBox.warning(self, "Review", "HÃ£y chá»n má»™t dÃ²ng review trÆ°á»›c.")
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
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        workspace = self._current_workspace
        database = ProjectDatabase(workspace.database_path)
        project_row = database.get_project()
        segments = database.list_segments(workspace.project_id)
        template = self._selected_prompt_template()
        if not project_row or not segments:
            QMessageBox.warning(self, "ChÆ°a cÃ³ phÃ¢n Ä‘oáº¡n", "HÃ£y cháº¡y ASR trÆ°á»›c khi dá»‹ch.")
            return None
        if not template:
            QMessageBox.warning(self, "ChÆ°a cÃ³ prompt", "KhÃ´ng tÃ¬m tháº¥y prompt template trong dá»± Ã¡n.")
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
                    context.report_progress(100, "DÃ¹ng láº¡i cache Contextual V2")
                    return JobResult(
                        message="DÃ¹ng cache Contextual V2",
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
                    message=f"ÄÃ£ cháº¡y Contextual V2 cho {len(contextual_result['segment_analyses'])} phÃ¢n Ä‘oáº¡n",
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
                context.report_progress(100, "DÃ¹ng láº¡i cache báº£n dá»‹ch")
                cache_path = workspace.cache_dir / "translate" / stage_hash / "segments_translated.json"
                return JobResult(
                    message=f"DÃ¹ng cache báº£n dá»‹ch cho {len(cached)} phÃ¢n Ä‘oáº¡n",
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
                message=f"ÄÃ£ dá»‹ch {len(translated_items)} phÃ¢n Ä‘oáº¡n",
                output_paths=[cache_path],
                extra={
                    "translated_count": len(translated_items),
                    "cache_path": cache_path,
                    "translation_mode": translation_mode,
                },
            )

        return self._job_manager.submit_job(
            stage="translate",
            description="Dá»‹ch phÃ¢n Ä‘oáº¡n báº±ng OpenAI Structured Outputs",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_tts_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "TTS", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi cháº¡y TTS.")
            return None
        workspace = self._current_workspace
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y preset giá»ng trong dá»± Ã¡n.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR vÃ  dá»‹ch trÆ°á»›c.")
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
                context.report_progress(100, "DÃ¹ng láº¡i cache clip TTS")
                return JobResult(
                    message=f"DÃ¹ng cache TTS cho {len(cached.artifacts)} phÃ¢n Ä‘oáº¡n",
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
                message=f"ÄÃ£ táº¡o {len(synthesized.artifacts)} clip TTS",
                output_paths=[synthesized.manifest_path],
                extra={
                    "manifest_path": synthesized.manifest_path,
                    "artifact_count": len(synthesized.artifacts),
                    "voice_engine": preset.engine.lower(),
                },
            )

        return self._job_manager.submit_job(
            stage="tts",
            description="Táº¡o clip TTS tá»« ná»™i dung phá»¥ Ä‘á» hoáº·c lá»i Ä‘á»c",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_build_voice_track_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Track giá»ng", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi táº¡o track giá»ng.")
            return None
        workspace = self._current_workspace
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y preset giá»ng trong dá»± Ã¡n.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR vÃ  dá»‹ch trÆ°á»›c.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="tts",
            dialog_title="Track giá»ng",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Track giá»ng"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Track giá»ng",
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
            QMessageBox.warning(self, "TTS", "HÃ£y cháº¡y TTS trÆ°á»›c khi táº¡o track giá»ng.")
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
                "Track giá»ng",
                (
                    "Cache TTS hiá»‡n táº¡i chÆ°a Ä‘á»§ cho toÃ n bá»™ dÃ²ng cáº§n Ä‘á»c.\n"
                    f"- DÃ²ng thiáº¿u clip: {self._format_row_number_list(missing_artifact_indexes)}\n"
                    "- HÃ£y cháº¡y láº¡i TTS sau khi hoÃ n táº¥t ná»™i dung tiáº¿ng Ä‘Ã­ch."
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
                message="ÄÃ£ táº¡o track giá»ng",
                output_paths=[result.manifest_path, result.voice_track_path],
                extra={
                    "voice_track_path": result.voice_track_path,
                    "manifest_path": result.manifest_path,
                    "fitted_count": len(result.fitted_clips),
                },
            )

        return self._job_manager.submit_job(
            stage="voice_track",
            description="CÄƒn chá»‰nh clip TTS theo timeline vÃ  táº¡o track giá»ng",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_mixdown_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "Trá»™n Ã¢m thanh", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi trá»™n Ã¢m thanh.")
            return None
        workspace = self._current_workspace
        artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
        if not artifacts or not artifacts.audio_48k_path.exists():
            QMessageBox.warning(self, "ChÆ°a cÃ³ audio 48 kHz", "HÃ£y tÃ¡ch Ã¢m thanh trÆ°á»›c khi trá»™n.")
            return None
        database, _active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR vÃ  dá»‹ch trÆ°á»›c.")
            return None
        preset = self._selected_voice_preset()
        if not preset:
            QMessageBox.warning(self, "Preset giá»ng", "KhÃ´ng tÃ¬m tháº¥y preset giá»ng trong dá»± Ã¡n.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="tts",
            dialog_title="Trá»™n Ã¢m thanh",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Trá»™n Ã¢m thanh"):
            return None
        if not self._last_voice_track_output or not self._last_voice_track_output.exists():
            QMessageBox.warning(self, "ChÆ°a cÃ³ track giá»ng", "HÃ£y táº¡o track giá»ng trÆ°á»›c khi trá»™n Ã¢m thanh.")
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Trá»™n Ã¢m thanh",
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
            QMessageBox.warning(self, "Trá»™n Ã¢m thanh", "KhÃ´ng tÃ¬m tháº¥y cache TTS hiá»‡n táº¡i. HÃ£y cháº¡y láº¡i TTS.")
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
                "Trá»™n Ã¢m thanh",
                (
                    "Track giá»ng hiá»‡n táº¡i khÃ´ng cÃ²n khá»›p vá»›i ná»™i dung TTS.\n"
                    f"- DÃ²ng thiáº¿u clip: {self._format_row_number_list(missing_artifact_indexes)}\n"
                    "- HÃ£y cháº¡y láº¡i TTS rá»“i táº¡o láº¡i track giá»ng trÆ°á»›c khi trá»™n."
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
                "Trá»™n Ã¢m thanh",
                (
                    "Track giá»ng hiá»‡n táº¡i khÃ´ng cÃ²n Ä‘á»“ng bá»™ vá»›i phá»¥ Ä‘á» hoáº·c preset giá»ng.\n"
                    "- HÃ£y táº¡o láº¡i track giá»ng trÆ°á»›c khi trá»™n Ã¢m thanh."
                ),
            )
            return None
        mixdown_inputs = self._current_mixdown_inputs_or_warn(dialog_title="Trá»™n Ã¢m thanh")
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
                message="ÄÃ£ trá»™n Ã¢m thanh",
                output_paths=[result.manifest_path, result.mixed_audio_path],
                extra={"mixed_audio_path": result.mixed_audio_path, "manifest_path": result.manifest_path},
            )

        return self._job_manager.submit_job(
            stage="mixdown",
            description="Trá»™n track giá»ng vá»›i Ã¢m thanh gá»‘c vÃ  BGM tÃ¹y chá»n",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_export_subtitles_job(self, format_name: str) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi xuáº¥t file.")
            return None
        workspace = self._current_workspace
        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR hoáº·c dá»‹ch trÆ°á»›c.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="subtitle",
            dialog_title=f"Xuáº¥t {format_name.upper()}",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(
            database,
            dialog_title=f"Xuáº¥t {format_name.upper()}",
        ):
            return None
        if not self._ensure_qc_passed_for_export(subtitle_rows, dialog_title=f"Xuáº¥t {format_name.upper()}"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)

        def handler(context: JobContext) -> JobResult:
            context.report_progress(
                10,
                f"Äang táº¡o {format_name.upper()} tá»« {self._subtitle_track_label(active_track)}",
            )
            output_path = export_subtitles(
                workspace,
                segments=subtitle_rows,
                format_name=format_name,
                allow_source_fallback=not require_localized,
            )
            context.report_progress(100, f"ÄÃ£ táº¡o {format_name.upper()}")
            return JobResult(
                message=f"ÄÃ£ xuáº¥t {format_name.upper()}",
                output_paths=[output_path],
                extra={"format_name": format_name, "output_path": output_path},
            )

        return self._job_manager.submit_job(
            stage=f"export_{format_name}",
            description=f"Xuáº¥t phá»¥ Ä‘á» {format_name.upper()}",
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_video_export_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return None
        if not self._save_subtitle_edits(silent=True):
            QMessageBox.warning(self, "BiÃªn táº­p phá»¥ Ä‘á»", "KhÃ´ng thá»ƒ lÆ°u chá»‰nh sá»­a phá»¥ Ä‘á» trÆ°á»›c khi xuáº¥t video.")
            return None

        workspace = self._current_workspace
        source_video_path = self._resolve_source_video_path()
        if not source_video_path:
            QMessageBox.warning(self, "ChÆ°a cÃ³ video", "HÃ£y chá»n video nguá»“n há»£p lá»‡.")
            return None
        try:
            export_preset = self._selected_export_preset(strict=True)
        except ValueError as exc:
            QMessageBox.warning(self, "Preset xuáº¥t", str(exc))
            return None
        if not export_preset:
            QMessageBox.warning(self, "Preset xuáº¥t", "KhÃ´ng tÃ¬m tháº¥y preset xuáº¥t trong dá»± Ã¡n.")
            return None

        database, active_track, subtitle_rows = self._load_active_subtitle_track_rows()
        if not subtitle_rows:
            QMessageBox.warning(self, "ChÆ°a cÃ³ track phá»¥ Ä‘á»", "HÃ£y cháº¡y ASR vÃ  dá»‹ch trÆ°á»›c.")
            return None
        if not self._ensure_localized_rows_ready(
            database,
            subtitle_rows,
            purpose="subtitle",
            dialog_title="Xuáº¥t video",
        ):
            return None
        if not self._ensure_contextual_semantic_ready(database, dialog_title="Xuáº¥t video"):
            return None
        if not self._ensure_qc_passed_for_export(subtitle_rows, dialog_title="Xuáº¥t video"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        preset, segment_voice_presets, _segment_speaker_keys, _voice_plan = self._resolve_tts_voice_plan(
            database,
            subtitle_rows,
            require_localized=require_localized,
            dialog_title="Xuáº¥t video",
            warn_on_unresolved=True,
        )
        if preset is None:
            return None

        video_row = database.get_primary_video_asset(workspace.project_id)
        duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else None
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path
        replacement_audio_path: Path | None = None
        if self._last_voice_track_output and self._last_voice_track_output.exists():
            if not preset:
                QMessageBox.warning(
                    self,
                    "Xuáº¥t video",
                    "KhÃ´ng tÃ¬m tháº¥y preset giá»ng hiá»‡n táº¡i. HÃ£y chá»n láº¡i preset rá»“i táº¡o láº¡i track giá»ng trÆ°á»›c khi xuáº¥t video.",
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
                    "Xuáº¥t video",
                    (
                        "Track giá»ng hiá»‡n táº¡i khÃ´ng cÃ²n khá»›p vá»›i track phá»¥ Ä‘á» Ä‘ang active.\n"
                        f"- DÃ²ng thiáº¿u clip TTS: {self._format_row_number_list(missing_tts_indexes)}\n"
                        "- HÃ£y cháº¡y láº¡i TTS, táº¡o track giá»ng vÃ  trá»™n Ã¢m thanh trÆ°á»›c khi xuáº¥t video."
                    ),
                )
                return None
            if expected_voice_track_path is None or expected_voice_track_path.resolve() != self._last_voice_track_output.resolve():
                QMessageBox.warning(
                    self,
                    "Xuáº¥t video",
                    (
                        "Track giá»ng hiá»‡n táº¡i khÃ´ng cÃ²n Ä‘á»“ng bá»™ vá»›i phá»¥ Ä‘á» hoáº·c preset giá»ng.\n"
                        "- HÃ£y táº¡o láº¡i track giá»ng rá»“i trá»™n Ã¢m thanh trÆ°á»›c khi xuáº¥t video."
                    ),
                )
                return None
            artifacts = self._audio_artifacts or load_cached_audio_artifacts(workspace)
            if not artifacts or not artifacts.audio_48k_path.exists():
                QMessageBox.warning(
                    self,
                    "Xuáº¥t video",
                    "ÄÃ£ cÃ³ track giá»ng nhÆ°ng chÆ°a cÃ³ audio 48 kHz Ä‘á»ƒ trá»™n. HÃ£y cháº¡y Chuáº©n bá»‹ media rá»“i trá»™n Ã¢m thanh trÆ°á»›c.",
                )
                return None
            mixdown_inputs = self._current_mixdown_inputs_or_warn(dialog_title="Xuáº¥t video")
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
                    "Xuáº¥t video",
                    "ÄÃ£ cÃ³ track giá»ng nhÆ°ng chÆ°a cÃ³ Ã¢m thanh Ä‘Ã£ trá»™n. HÃ£y cháº¡y Trá»™n Ã¢m thanh trÆ°á»›c khi xuáº¥t video.",
                )
                return None
            if expected_mixed_audio_path.resolve() != self._last_mixed_audio_output.resolve():
                QMessageBox.warning(
                    self,
                    "Xuáº¥t video",
                    (
                        "Ã‚m thanh Ä‘Ã£ trá»™n hiá»‡n táº¡i khÃ´ng cÃ²n Ä‘á»“ng bá»™ vá»›i track giá»ng, má»©c Ã¢m lÆ°á»£ng hoáº·c BGM.\n"
                        "- HÃ£y cháº¡y láº¡i Trá»™n Ã¢m thanh trÆ°á»›c khi xuáº¥t video."
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
                f"Äang táº¡o {subtitle_format.upper()} cho {self._subtitle_track_label(active_track)}",
            )
            subtitle_path = export_subtitles(
                workspace,
                segments=subtitle_rows,
                format_name=subtitle_format,
                allow_source_fallback=not require_localized,
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
                message=f"ÄÃ£ xuáº¥t video {export_mode_label}",
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
                "Ghi cá»©ng ASS vÃ o video nguá»“n báº±ng FFmpeg"
                if export_preset.burn_subtitles
                else "Gáº¯n track phá»¥ Ä‘á» vÃ o video nguá»“n báº±ng FFmpeg"
            ),
            handler=handler,
            project_id=workspace.project_id,
            project_db_path=workspace.database_path,
        )

    def _run_hardsub_export_job(self) -> None:
        self._run_video_export_job()

    def _run_smoke_job(self) -> None:
        if not self._current_workspace:
            QMessageBox.warning(self, "ChÆ°a cÃ³ dá»± Ã¡n", "HÃ£y táº¡o hoáº·c má»Ÿ dá»± Ã¡n trÆ°á»›c.")
            return

        def smoke_handler(context: JobContext) -> JobResult:
            for progress in range(0, 101, 10):
                context.cancellation_token.raise_if_canceled()
                context.report_progress(progress, f"Kiá»ƒm tra tiáº¿n trÃ¬nh {progress}%")
                context.sleep_with_cancel(0.15)
            return JobResult(message="TÃ¡c vá»¥ kiá»ƒm tra Ä‘Ã£ hoÃ n táº¥t")

        self._job_manager.submit_job(
            stage="smoke",
            description="Kiá»ƒm tra hÃ ng Ä‘á»£i tÃ¡c vá»¥, tiáº¿n trÃ¬nh vÃ  thao tÃ¡c há»§y",
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
        status_label = "tháº¥t báº¡i" if status == JobStatus.FAILED.value else "Ä‘Ã£ bá»‹ há»§y"
        self._stop_workflow(
            message=(
                f"Quy trÃ¬nh nhanh dá»«ng láº¡i: bÆ°á»›c {self._workflow_stage_label(stage)} {status_label}.\n"
                f"- ThÃ´ng bÃ¡o: {message or '-'}"
            ),
        )

    def _handle_retry_requested(self, job_id: str) -> None:
        new_job_id = self._job_manager.retry_job(job_id)
        if not new_job_id:
            QMessageBox.warning(self, "KhÃ´ng thá»ƒ cháº¡y láº¡i", "KhÃ´ng tÃ¬m tháº¥y job gá»‘c.")
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
                    "ASR hoÃ n táº¥t:\n"
                    f"- Sá»‘ phÃ¢n Ä‘oáº¡n: {persisted.segment_count}\n"
                    f"- Cache: {persisted.cache_dir}\n"
                    f"- JSON: {persisted.segments_json_path}"
                )
            else:
                database = ProjectDatabase(self._current_workspace.database_path)
                self._asr_summary.setText(
                    f"ASR hoÃ n táº¥t, sá»‘ phÃ¢n Ä‘oáº¡n trong CSDL: {database.count_segments(self._current_workspace.project_id)}"
                )
            self._reload_subtitle_editor_from_db(force=True)
        elif stage == "translate" and self._current_workspace:
            translated_count = extra.get("translated_count", 0)
            cache_path = extra.get("cache_path")
            translation_mode = extra.get("translation_mode", self._current_translation_mode())
            pending_review_count = extra.get("pending_review_count")
            semantic_qc = extra.get("semantic_qc") or {}
            summary_lines = [
                "Dá»‹ch hoÃ n táº¥t:",
                f"- Cháº¿ Ä‘á»™: {translation_mode}",
                f"- Sá»‘ dÃ²ng Ä‘Ã£ dá»‹ch: {translated_count}",
                f"- Cache: {cache_path}",
            ]
            if pending_review_count is not None:
                summary_lines.append(f"- DÃ²ng cáº§n review: {pending_review_count}")
            if semantic_qc:
                summary_lines.append(
                    f"- Semantic QC: {semantic_qc.get('error_count', 0)} lá»—i, {semantic_qc.get('warning_count', 0)} cáº£nh bÃ¡o"
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
        self._reload_speaker_bindings()
        self._reload_voice_policies()
        video_row = database.get_primary_video_asset(self._current_workspace.project_id)
        if video_row:
            self._media_summary.setText(
                "Video nguá»“n:\n"
                f"- ÄÆ°á»ng dáº«n: {video_row['path']}\n"
                f"- Thá»i lÆ°á»£ng: {video_row['duration_ms']} ms\n"
                f"- Äá»™ phÃ¢n giáº£i: {video_row['width']}x{video_row['height']}\n"
                f"- FPS: {video_row['fps']}\n"
                f"- Ã‚m thanh: {video_row['audio_channels']} kÃªnh @ {video_row['sample_rate']} Hz"
            )
        else:
            self._media_summary.setText("ChÆ°a cÃ³ video nguá»“n")

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
                "Bá»™ nhá»› Ä‘á»‡m Ã¢m thanh sáºµn sÃ ng:\n"
                f"- ASR 16 kHz: {self._audio_artifacts.audio_16k_path}\n"
                f"- Mix 48 kHz: {self._audio_artifacts.audio_48k_path}\n"
                f"- PhÃ¢n Ä‘oáº¡n trong CSDL: {segment_count}"
            )
        else:
            self._asr_summary.setText(f"ChÆ°a cÃ³ cache Ã¢m thanh. PhÃ¢n Ä‘oáº¡n trong CSDL: {segment_count}")
        target_language = project_row["target_language"] if project_row else "-"
        translation_mode = self._current_translation_mode(project_row)
        pending_review_count = (
            database.count_pending_segment_reviews(self._current_workspace.project_id)
            if translation_mode == "contextual_v2"
            else 0
        )
        self._translation_summary.setText(
            "Tráº¡ng thÃ¡i dá»‹ch:\n"
            f"- Cháº¿ Ä‘á»™: {translation_mode}\n"
            f"- NgÃ´n ngá»¯ Ä‘Ã­ch: {target_language}\n"
            f"- Máº«u prompt: {len(self._prompt_templates)}\n"
            f"- ÄÃ£ dá»‹ch: {translated_count}/{segment_count}\n"
            f"- DÃ²ng cáº§n review: {pending_review_count}"
        )
        if translation_mode == "contextual_v2":
            self._review_summary.setText(
                f"HÃ ng review semantic: {pending_review_count} dÃ²ng cáº§n duyá»‡t trÆ°á»›c TTS/export"
            )
        else:
            self._review_summary.setText("Dá»± Ã¡n nÃ y Ä‘ang dÃ¹ng cháº¿ Ä‘á»™ dá»‹ch legacy")

        current_preset = self._selected_voice_preset()
        resolved_preset = current_preset
        segment_voice_presets: dict[str, object] | None = None
        speaker_binding_lines: list[str] = []
        voice_binding_ready = True
        if database and subtitle_rows:
            resolved_preset, segment_voice_presets, _segment_speaker_keys, voice_plan = self._resolve_tts_voice_plan(
                database,
                subtitle_rows,
                require_localized=require_localized,
                dialog_title="LÃ¡Â»â€œng tiÃ¡ÂºÂ¿ng",
                warn_on_unresolved=False,
            )
            if voice_plan is not None:
                if getattr(voice_plan, "active_bindings", False):
                    if getattr(voice_plan, "unresolved_speakers", None):
                        voice_binding_ready = False
                        speaker_binding_lines.append(
                            "- Speaker binding: chưa đủ, còn speaker chưa gán preset "
                            + f"({', '.join(voice_plan.unresolved_speakers)})"
                        )
                    elif getattr(voice_plan, "missing_preset_ids", None):
                        voice_binding_ready = False
                        speaker_binding_lines.append(
                            "- Speaker binding: có binding trỏ tới preset không còn tồn tại "
                            + f"({', '.join(voice_plan.missing_preset_ids)})"
                        )
                    else:
                        speaker_binding_lines.append(
                            f"- Speaker binding: đã gán theo speaker cho {len(voice_plan.segment_voice_preset_ids)} dòng"
                        )
                else:
                    speaker_binding_lines.append(
                        "- Speaker binding: chưa bật, toàn bộ sẽ dùng preset mặc định"
                    )
        if database and subtitle_rows and voice_plan is not None:
            if getattr(voice_plan, "active_voice_policies", False):
                relationship_hits = int(getattr(voice_plan, "relationship_policy_hits", 0))
                character_hits = int(getattr(voice_plan, "character_policy_hits", 0))
                relationship_style_hits = int(getattr(voice_plan, "relationship_style_hits", 0))
                character_style_hits = int(getattr(voice_plan, "character_style_hits", 0))
                if relationship_hits or character_hits or relationship_style_hits or character_style_hits:
                    speaker_binding_lines.append(
                        f"- Voice policy: relationship={relationship_hits} dòng, character={character_hits} dòng"
                    )
                    if relationship_style_hits or character_style_hits:
                        speaker_binding_lines.append(
                            f"- Voice style: relationship={relationship_style_hits} dòng, character={character_style_hits} dòng"
                        )
                else:
                    speaker_binding_lines.append(
                        "- Voice policy: đã bật nhưng chưa khớp dòng nào; runtime sẽ rơi về speaker binding hoặc preset mặc định"
                    )
            else:
                speaker_binding_lines.append("- Voice policy: chưa bật")
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
            "Lá»“ng tiáº¿ng:",
            f"- Sá»‘ preset giá»ng: {len(self._voice_presets)}",
            f"- Track phá»¥ Ä‘á» Ä‘ang dÃ¹ng: {self._subtitle_track_label(active_track)}",
            f"- Ná»™i dung sáºµn sÃ ng Ä‘á»ƒ lá»“ng tiáº¿ng: {tts_text_ready_count}/{len(subtitle_rows)}",
            f"- DÃ²ng Ä‘Ã£ cÃ³ audio TTS: {tts_ready_count}/{len(subtitle_rows)}",
            f"- Preset Ä‘ang chá»n: {current_preset.name if current_preset else '-'}",
        ]
        if self._installed_sapi_voices:
            voice_lines.append(f"- Giá»ng SAPI phÃ¡t hiá»‡n: {len(self._installed_sapi_voices)}")
        version_suffix = f" v{self._vieneu_environment.package_version}" if self._vieneu_environment.package_version else ""
        if self._vieneu_environment.package_installed:
            voice_lines.append(f"- VieNeu SDK{version_suffix}: Ä‘Ã£ cÃ i")
            if self._vieneu_environment.espeak_path:
                voice_lines.append(f"- eSpeak NG: {self._vieneu_environment.espeak_path}")
            else:
                voice_lines.append("- eSpeak NG: chÆ°a tÃ¬m tháº¥y cho VieNeu local")
        else:
            voice_lines.append("- VieNeu SDK: chÆ°a cÃ i")
        voice_lines.extend(speaker_binding_lines)
        if not voice_binding_ready:
            voice_lines.append("- Tráº¡ng thÃ¡i binding: chÆ°a an toÃ n Ä‘á»ƒ cháº¡y TTS hoáº·c xuáº¥t video")
        if current_preset and current_preset.engine.lower() == "vieneu":
            try:
                voice_lines.append(f"- Cháº¿ Ä‘á»™ VieNeu: {get_vieneu_mode(current_preset)}")
            except ValueError as exc:
                voice_lines.append(f"- Cáº¥u hÃ¬nh VieNeu lá»—i: {exc}")
            ref_audio_path = str(current_preset.engine_options.get("ref_audio_path", "")).strip()
            ref_text = str(current_preset.engine_options.get("ref_text", "")).strip()
            if ref_audio_path:
                voice_lines.append(f"- Audio máº«u clone: {ref_audio_path}")
            if ref_text:
                preview = ref_text if len(ref_text) <= 72 else ref_text[:69] + "..."
                voice_lines.append(f"- VÄƒn báº£n máº«u clone: {preview}")
        if self._last_tts_manifest:
            voice_lines.append(f"- Manifest TTS: {self._last_tts_manifest}")
        if self._last_voice_track_output:
            voice_lines.append(f"- Track giá»ng: {self._last_voice_track_output}")
        voice_lines.append(
            "- Äá»“ng bá»™ track giá»ng: "
            + (
                "CÃ³"
                if voice_track_ready
                else "Cáº§n táº¡o láº¡i"
                if self._last_voice_track_output
                else "ChÆ°a cÃ³"
            )
        )
        self._voice_summary.setText("\n".join(voice_lines))

        mix_lines = ["Trá»™n Ã¢m thanh:"]
        if self._audio_artifacts:
            mix_lines.append(f"- Audio gá»‘c 48 kHz: {self._audio_artifacts.audio_48k_path}")
        else:
            mix_lines.append("- Audio gá»‘c 48 kHz: chÆ°a cÃ³")
        mix_lines.append(f"- BGM: {self._resolve_bgm_path() or '-'}")
        mix_lines.append(
            f"- Ã‚m lÆ°á»£ng: gá»‘c={self._original_volume_input.text()} "
            f"giá»ng={self._voice_volume_input.text()} bgm={self._bgm_volume_input.text()}"
        )
        mixed_audio_ready = False
        if voice_track_ready and self._last_voice_track_output and self._audio_artifacts:
            mixdown_inputs = self._current_mixdown_inputs()
            if mixdown_inputs is None:
                mix_lines.append("- Tráº¡ng thÃ¡i: thÃ´ng sá»‘ mix chÆ°a há»£p lá»‡")
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
                    f"- Tráº¡ng thÃ¡i: {'Äá»“ng bá»™ vá»›i track giá»ng hiá»‡n táº¡i' if mixed_audio_ready else 'Cáº§n trá»™n láº¡i'}"
                )
        elif self._last_voice_track_output and self._last_voice_track_output.exists():
            if self._audio_artifacts:
                mix_lines.append("- Tráº¡ng thÃ¡i: track giá»ng hiá»‡n táº¡i Ä‘Ã£ cÅ©, hÃ£y táº¡o láº¡i trÆ°á»›c khi trá»™n")
            else:
                mix_lines.append("- Tráº¡ng thÃ¡i: thiáº¿u audio 48 kHz Ä‘á»ƒ trá»™n")
        if self._last_mixed_audio_output:
            mix_lines.append(f"- Ã‚m thanh Ä‘Ã£ trá»™n: {self._last_mixed_audio_output}")
        self._mix_summary.setText("\n".join(mix_lines))

        subtitle_lines = [
            "BiÃªn táº­p phá»¥ Ä‘á»:",
            f"- Track Ä‘ang dÃ¹ng: {self._subtitle_track_label(active_track)}",
            f"- DÃ²ng sáºµn sÃ ng Ä‘á»ƒ xuáº¥t: {subtitle_ready_count}/{len(subtitle_rows)}",
            f"- QC: {qc_report.error_count} lá»—i, {qc_report.warning_count} cáº£nh bÃ¡o",
            f"- CÃ³ thay Ä‘á»•i chÆ°a lÆ°u: {'CÃ³' if self._subtitle_editor_dirty else 'KhÃ´ng'}",
        ]
        if self._last_subtitle_outputs:
            subtitle_lines.append("- Tá»‡p Ä‘áº§u ra:")
            for format_name, output_path in sorted(self._last_subtitle_outputs.items()):
                subtitle_lines.append(f"- {format_name.upper()}: {output_path}")
        self._subtitle_summary.setText("\n".join(subtitle_lines))

        source_video_path = self._resolve_source_video_path()
        selected_export_preset = self._selected_export_preset(strict=False)
        selected_watermark_profile = self._selected_watermark_profile(strict=False)
        export_mode = "Ghi cá»©ng ASS" if selected_export_preset and selected_export_preset.burn_subtitles else "Mux soft-sub"
        watermark_mode = (
            "Báº­t" if selected_export_preset and selected_export_preset.watermark_enabled else "Táº¯t"
        )
        watermark_profile_label = (
            selected_watermark_profile.name
            if selected_watermark_profile
            else "Theo preset xuáº¥t"
        )
        watermark_path_label = self._resolve_watermark_path() or (
            selected_export_preset.watermark_path if selected_export_preset and selected_export_preset.watermark_path else "-"
        )
        if self._last_export_output:
            self._export_summary.setText(
                "Video Ä‘áº§u ra:\n"
                f"- Nguá»“n: {source_video_path or '-'}\n"
                f"- Preset xuáº¥t: {selected_export_preset.name if selected_export_preset else '-'}\n"
                f"- Cháº¿ Ä‘á»™: {export_mode if selected_export_preset else '-'}\n"
                f"- Profile watermark: {watermark_profile_label}\n"
                f"- Watermark: {watermark_mode} / {watermark_path_label}\n"
                f"- Ã‚m thanh Ä‘Ã£ trá»™n: {self._last_mixed_audio_output or '-'}\n"
                f"- Tá»‡p Ä‘áº§u ra: {self._last_export_output}"
            )
        elif source_video_path:
            self._export_summary.setText(
                "Sáºµn sÃ ng xuáº¥t video:\n"
                f"- Nguá»“n: {source_video_path}\n"
                f"- Track phá»¥ Ä‘á»: {self._subtitle_track_label(active_track)}\n"
                f"- Sá»‘ dÃ²ng phá»¥ Ä‘á»: {len(subtitle_rows)}\n"
                f"- Preset xuáº¥t: {selected_export_preset.name if selected_export_preset else '-'}\n"
                f"- Cháº¿ Ä‘á»™: {export_mode if selected_export_preset else '-'}\n"
                f"- Profile watermark: {watermark_profile_label}\n"
                f"- Watermark: {watermark_mode} / {watermark_path_label}\n"
                f"- Ã‚m thanh Ä‘Ã£ trá»™n: {self._last_mixed_audio_output or '-'}\n"
                f"- ThÆ° má»¥c xuáº¥t: {self._current_workspace.exports_dir}"
            )
        else:
            self._export_summary.setText("ChÆ°a cÃ³ video nguá»“n Ä‘á»ƒ xuáº¥t")

        pipeline_lines = [
            "Checklist quy trÃ¬nh:",
            f"- Metadata video: {'Sáºµn sÃ ng' if video_row else 'Thiáº¿u'}",
            f"- Bá»™ Ä‘á»‡m audio 16 kHz/48 kHz: {'Sáºµn sÃ ng' if self._audio_artifacts else 'Thiáº¿u'}",
            f"- PhÃ¢n Ä‘oáº¡n ASR: {segment_count}",
            f"- Dá»‹ch: {translated_count}/{segment_count}",
            f"- DÃ²ng phá»¥ Ä‘á» tiáº¿ng Ä‘Ã­ch: {subtitle_ready_count}/{len(subtitle_rows)}",
            f"- QC phá»¥ Ä‘á»: {'Äáº¡t' if qc_report.error_count == 0 else f'{qc_report.error_count} lá»—i'}"
            + (f", {qc_report.warning_count} cáº£nh bÃ¡o" if qc_report.warning_count else ""),
            f"- Ná»™i dung lá»“ng tiáº¿ng tiáº¿ng Ä‘Ã­ch: {tts_text_ready_count}/{len(subtitle_rows)}",
            f"- Clip TTS: {tts_ready_count}/{len(subtitle_rows)}",
            f"- Track giá»ng: {'Sáºµn sÃ ng' if voice_track_ready else 'Thiáº¿u hoáº·c cáº§n táº¡o láº¡i'}",
            f"- Ã‚m thanh Ä‘Ã£ trá»™n: {'Sáºµn sÃ ng' if mixed_audio_ready else 'Thiáº¿u hoáº·c cáº§n trá»™n láº¡i'}",
            (
                "- Sáºµn sÃ ng xuáº¥t: CÃ³"
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
                else "- Sáºµn sÃ ng xuáº¥t: KhÃ´ng"
            ),
            f"- Video Ä‘áº§u ra: {'Sáºµn sÃ ng' if self._last_export_output and self._last_export_output.exists() else 'ChÆ°a cÃ³'}",
        ]
        self._pipeline_summary.setText("\n".join(pipeline_lines))
        if not self._workflow_current_stage:
            self._update_workflow_status_label()

    def closeEvent(self, event) -> None:  # noqa: N802
        self._cancel_preview_reload()
        self._preview_controller.close()
        super().closeEvent(event)

    def _append_log_line(self, message: str) -> None:
        self._logs_console.appendPlainText(message)

