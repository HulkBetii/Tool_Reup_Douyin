from __future__ import annotations

import json
import re
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QTimer, Qt
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
from app.project.models import ProjectInitRequest, ProjectWorkspace, SubtitleTrackRecord
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
from app.tts.presets import (
    batch_import_voice_clone_presets,
    delete_voice_preset,
    list_voice_presets,
    save_voice_preset,
)
from app.tts.sapi_engine import list_installed_sapi_voices
from app.tts.vieneu_engine import detect_vieneu_installation, get_vieneu_mode
from app.translate.openai_engine import OpenAITranslationEngine
from app.translate.persistence import (
    build_translation_stage_hash,
    load_cached_translations,
    persist_translations,
)
from app.translate.presets import list_prompt_templates
from app.ui.status_panel import StatusPanel
from app.version import APP_NAME, APP_VERSION


class MainWindow(QMainWindow):
    def __init__(self, settings: AppSettings, job_manager: JobManager) -> None:
        super().__init__()
        self._settings = settings
        self._job_manager = job_manager
        self._current_workspace: ProjectWorkspace | None = None
        self._media_metadata: MediaMetadata | None = None
        self._audio_artifacts: ExtractedAudioArtifacts | None = None
        self._prompt_templates = []
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

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(splitter)
        self.setCentralWidget(container)

        self._sync_settings_to_form()
        self._append_log_line("Khởi tạo giao diện hoàn tất")

    def _build_project_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self._project_summary = self._create_info_label("Chưa mở dự án")
        self._media_summary = self._create_info_label("Chưa có video nguồn")
        self._pipeline_summary = self._create_info_label("Checklist quy trình chưa có dữ liệu")
        self._workflow_status = self._create_info_label("Quy trình nhanh: sẵn sàng")

        group = QGroupBox("Khởi tạo / mở dự án")
        form = QFormLayout(group)
        self._configure_form_layout(form)

        self._project_name_input = QLineEdit("Dự án mới")
        self._project_root_input = QLineEdit(str(Path.cwd() / "workspace"))
        self._source_video_input = QLineEdit()
        self._source_lang_combo = QComboBox()
        self._source_lang_combo.addItems(["auto", "vi", "zh", "en"])
        self._target_lang_combo = QComboBox()
        self._target_lang_combo.addItems(["vi", "zh", "en"])

        browse_button = QPushButton("Chọn thư mục")
        browse_button.clicked.connect(self._choose_project_root)
        create_button = QPushButton("Tạo dự án")
        create_button.clicked.connect(self._create_project)
        open_button = QPushButton("Mở dự án")
        open_button.clicked.connect(self._open_project)
        smoke_button = QPushButton("Chạy tác vụ thử")
        smoke_button.clicked.connect(self._run_smoke_job)
        choose_video_button = QPushButton("Chọn video")
        choose_video_button.clicked.connect(self._choose_source_video)
        probe_button = QPushButton("Đọc metadata")
        probe_button.clicked.connect(self._run_probe_media_job)
        extract_button = QPushButton("Tách âm thanh")
        extract_button.clicked.connect(self._run_extract_audio_job)
        prepare_media_button = QPushButton("Chuẩn bị media")
        prepare_media_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio"],
                workflow_name="Chuẩn bị media",
            )
        )
        asr_translate_button = QPushButton("ASR -> Dịch")
        asr_translate_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["asr", "translate"],
                workflow_name="ASR -> Dịch",
            )
        )
        dub_button = QPushButton("Lồng tiếng nhanh")
        dub_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["tts", "voice_track", "mixdown"],
                workflow_name="Lồng tiếng nhanh",
            )
        )
        full_pipeline_button = QPushButton("Chạy toàn bộ quy trình")
        full_pipeline_button.clicked.connect(
            lambda checked=False: self._start_workflow(
                ["probe_media", "extract_audio", "asr", "translate", "tts", "voice_track", "mixdown", "export_video"],
                workflow_name="Toàn bộ quy trình",
            )
        )
        stop_workflow_button = QPushButton("Dừng quy trình")
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

        workflow_group = QGroupBox("Quy trình nhanh")
        workflow_form = QFormLayout(workflow_group)
        self._configure_form_layout(workflow_form)
        workflow_form.addRow("Trạng thái", self._workflow_status)
        workflow_form.addRow("", workflow_container)

        form.addRow("Tên dự án", self._project_name_input)
        form.addRow("Thư mục dự án", self._project_root_input)
        form.addRow("Video nguồn", self._source_video_input)
        form.addRow("", source_container)
        form.addRow("Ngôn ngữ nguồn", self._source_lang_combo)
        form.addRow("Dịch sang", self._target_lang_combo)
        form.addRow("", button_container)

        layout.addWidget(self._project_summary)
        layout.addWidget(self._media_summary)
        layout.addWidget(self._pipeline_summary)
        layout.addWidget(group)
        layout.addWidget(workflow_group)
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

    def _build_translate_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("ASR và Dịch")
        form = QFormLayout(group)
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
        self._translation_model_input = QLineEdit()
        self._translation_model_input.setPlaceholderText("gpt-4.1-mini")
        self._translation_summary = self._create_info_label("Chưa có kết quả dịch")

        run_asr_button = QPushButton("Chạy ASR")
        run_asr_button.clicked.connect(self._run_asr_job)
        reload_prompts_button = QPushButton("Nạp lại prompt")
        reload_prompts_button.clicked.connect(self._reload_prompt_templates)
        run_translate_button = QPushButton("Chạy dịch")
        run_translate_button.clicked.connect(self._run_translation_job)
        translate_buttons = QHBoxLayout()
        translate_buttons.addWidget(reload_prompts_button)
        translate_buttons.addWidget(run_translate_button)
        translate_buttons.addStretch(1)
        translate_container = QWidget()
        translate_container.setLayout(translate_buttons)

        form.addRow("Engine ASR", self._asr_engine_combo)
        form.addRow("Mô hình", self._asr_model_combo)
        form.addRow("Ngôn ngữ ASR", self._asr_language_combo)
        form.addRow("", self._vad_checkbox)
        form.addRow("", self._word_timestamps_checkbox)
        form.addRow("", run_asr_button)
        form.addRow("Mẫu prompt", self._prompt_combo)
        form.addRow("Mô hình dịch", self._translation_model_input)
        form.addRow("", translate_container)

        layout.addWidget(self._asr_summary)
        layout.addWidget(self._translation_summary)
        layout.addWidget(group)
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
        self._subtitle_qc_table.setHorizontalHeaderLabels(["Dòng", "Mã lỗi", "Mức độ", "Chi tiết"])
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
        self._replace_target_combo.addItem("Phụ đề", "subtitle")
        self._replace_target_combo.addItem("Bản dịch", "translated")
        self._replace_target_combo.addItem("TTS", "tts")
        self._replace_target_combo.addItem("Tất cả", "all")

        reload_button = QPushButton("Nạp lại từ CSDL")
        reload_button.clicked.connect(lambda: self._reload_subtitle_editor_from_db(force=True))
        translated_to_subtitle_button = QPushButton("Bản dịch -> Phụ đề")
        translated_to_subtitle_button.clicked.connect(self._apply_translated_to_subtitle)
        subtitle_to_tts_button = QPushButton("Phụ đề -> Lời TTS")
        subtitle_to_tts_button.clicked.connect(self._apply_subtitle_to_tts)
        polish_tts_button = QPushButton("Làm mượt Lời TTS")
        polish_tts_button.clicked.connect(self._polish_tts_texts)
        split_button = QPushButton("Tách dòng chọn")
        split_button.clicked.connect(self._split_selected_subtitle_row)
        merge_button = QPushButton("Gộp với dòng sau")
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
        choose_bgm_button = QPushButton("Chọn BGM")
        choose_bgm_button.clicked.connect(self._choose_bgm_file)
        mix_button = QPushButton("Trộn âm thanh")
        mix_button.clicked.connect(self._run_mixdown_job)

        group = QGroupBox("Preset giọng, TTS và trộn âm thanh")
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
        watermark_numeric_row.addWidget(QLabel("Độ mờ"))
        watermark_numeric_row.addWidget(self._watermark_opacity_input)
        watermark_numeric_row.addWidget(QLabel("Tỷ lệ"))
        watermark_numeric_row.addWidget(self._watermark_scale_input)
        watermark_numeric_row.addWidget(QLabel("Lề"))
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
        self._ffmpeg_path_input = QLineEdit()
        self._ffprobe_path_input = QLineEdit()
        self._mpv_path_input = QLineEdit()
        self._model_cache_input = QLineEdit()
        self._openai_key_input = QLineEdit()
        self._openai_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._default_translation_model_input = QLineEdit()
        self._ffmpeg_status = self._create_info_label("Chưa kiểm tra")

        save_button = QPushButton("Lưu cài đặt")
        save_button.clicked.connect(self._save_settings)
        check_button = QPushButton("Kiểm tra FFmpeg")
        check_button.clicked.connect(self._check_ffmpeg)

        buttons = QHBoxLayout()
        buttons.addWidget(save_button)
        buttons.addWidget(check_button)
        buttons.addStretch(1)
        button_container = QWidget()
        button_container.setLayout(buttons)

        form.addRow("Ngôn ngữ giao diện", self._ui_language_input)
        form.addRow("Đường dẫn ffmpeg", self._ffmpeg_path_input)
        form.addRow("Đường dẫn ffprobe", self._ffprobe_path_input)
        form.addRow("Đường dẫn mpv DLL", self._mpv_path_input)
        form.addRow("Thư mục cache model", self._model_cache_input)
        form.addRow("OpenAI API key", self._openai_key_input)
        form.addRow("Mô hình dịch mặc định", self._default_translation_model_input)
        form.addRow("", button_container)
        form.addRow("Trạng thái kiểm tra", self._ffmpeg_status)

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
        directory = QFileDialog.getExistingDirectory(self, "Chọn thư mục dự án")
        if directory:
            self._project_root_input.setText(directory)

    def _choose_source_video(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn video nguồn",
            str(Path.cwd()),
            "Tệp video (*.mp4 *.mkv *.mov *.avi *.webm);;Tất cả tệp (*.*)",
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
    ) -> tuple[Path | None, list[int]]:
        tts_stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
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
            raise ValueError("Chưa có dự án")
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
        root_dir = Path(self._project_root_input.text()).expanduser()
        source_video_path = self._resolve_source_video_path()
        request = ProjectInitRequest(
            name=self._project_name_input.text().strip() or "Dự án mới",
            root_dir=root_dir,
            source_language=self._source_lang_combo.currentText(),
            target_language=self._target_lang_combo.currentText(),
            source_video_path=source_video_path,
        )
        try:
            workspace = bootstrap_project(request)
        except FileExistsError as exc:
            QMessageBox.warning(self, "Không thể tạo dự án", str(exc))
            return
        self._set_current_workspace(workspace)
        QMessageBox.information(self, "Thành công", f"Đã tạo dự án tại:\n{workspace.root_dir}")

    def _open_project(self) -> None:
        directory = QFileDialog.getExistingDirectory(self, "Mở thư mục dự án")
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
        self._restore_workspace_runtime_state(workspace)
        self._source_video_input.setText(str(workspace.source_video_path) if workspace.source_video_path else "")
        self._reload_prompt_templates()
        self._reload_voice_presets()
        self._reload_export_presets()
        self._reload_watermark_profiles()
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
        for row_index in range(self._subtitle_table.rowCount()):
            for column_index in range(self._subtitle_table.columnCount()):
                item = self._subtitle_table.item(row_index, column_index)
                if item is not None:
                    item.setBackground(QColor())

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
            str(Path.cwd()),
            "Tệp âm thanh (*.wav *.mp3 *.m4a *.aac *.flac *.ogg);;Tất cả tệp (*.*)",
        )
        if file_path:
            self._bgm_path_input.setText(file_path)

    def _choose_vieneu_ref_audio_file(self) -> None:
        initial_dir = self._current_workspace.root_dir if self._current_workspace else Path.cwd()
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
        initial_dir = self._current_workspace.root_dir if self._current_workspace else Path.cwd()
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
            f"da_nhap={len(report.imported_presets)} "
            f"thieu_txt={len(report.skipped_missing_text)} "
            f"txt_rong={len(report.skipped_empty_text)}"
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

    def _run_probe_media_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
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
            return
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

    def _run_translation_job(self) -> str | None:
        if not self._current_workspace:
            QMessageBox.warning(self, "Chưa có dự án", "Hãy tạo hoặc mở dự án trước.")
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

        source_language = segments[0]["source_lang"] or project_row["source_language"] or "auto"
        target_language = project_row["target_language"]
        model = self._translation_model_input.text().strip() or self._settings.default_translation_model
        stage_hash = build_translation_stage_hash(
            segments=segments,
            template=template,
            model=model,
            source_language=source_language,
            target_language=target_language,
        )
        settings = self._settings

        def handler(context: JobContext) -> JobResult:
            cached = load_cached_translations(workspace, stage_hash)
            if cached:
                database.apply_segment_translations(workspace.project_id, cached)
                context.report_progress(100, "Dùng lại cache bản dịch")
                cache_path = workspace.cache_dir / "translate" / stage_hash / "segments_translated.json"
                return JobResult(
                    message=f"Dùng cache bản dịch cho {len(cached)} phân đoạn",
                    output_paths=[cache_path],
                    extra={"translated_count": len(cached), "cache_path": cache_path},
                )

            engine = OpenAITranslationEngine(settings)
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
                extra={"translated_count": len(translated_items), "cache_path": cache_path},
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
        require_localized = self._requires_localized_output(database, subtitle_rows)
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
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
        require_localized = self._requires_localized_output(database, subtitle_rows)
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
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
        if not self._last_voice_track_output or not self._last_voice_track_output.exists():
            QMessageBox.warning(self, "Chưa có track giọng", "Hãy tạo track giọng trước khi trộn âm thanh.")
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)
        stage_hash = build_tts_stage_hash(
            subtitle_rows,
            preset,
            allow_source_fallback=not require_localized,
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
        if not self._ensure_qc_passed_for_export(subtitle_rows, dialog_title="Xuất video"):
            return None
        require_localized = self._requires_localized_output(database, subtitle_rows)

        video_row = database.get_primary_video_asset(workspace.project_id)
        duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else None
        ffmpeg_path = self._settings.dependency_paths.ffmpeg_path
        replacement_audio_path: Path | None = None
        if self._last_voice_track_output and self._last_voice_track_output.exists():
            preset = self._selected_voice_preset()
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
            self._translation_summary.setText(
                "Dịch hoàn tất:\n"
                f"- Số dòng đã dịch: {translated_count}\n"
                f"- Cache: {cache_path}"
            )
            self._reload_subtitle_editor_from_db(force=True)
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
        self._translation_summary.setText(
            "Trạng thái dịch:\n"
            f"- Ngôn ngữ đích: {target_language}\n"
            f"- Mẫu prompt: {len(self._prompt_templates)}\n"
            f"- Đã dịch: {translated_count}/{segment_count}"
        )

        current_preset = self._selected_voice_preset()
        tts_ready_count = sum(1 for row in subtitle_rows if row["audio_path"])
        total_duration_ms = int(video_row["duration_ms"]) if video_row and video_row["duration_ms"] else (
            max((int(row["end_ms"]) for row in subtitle_rows), default=0)
        )
        expected_voice_track_path = None
        voice_track_ready = False
        if current_preset and subtitle_rows and total_duration_ms > 0:
            expected_voice_track_path, missing_tts_indexes = self._expected_voice_track_path_for_rows(
                workspace=self._current_workspace,
                subtitle_rows=subtitle_rows,
                preset=current_preset,
                total_duration_ms=total_duration_ms,
                require_localized=require_localized,
            )
            voice_track_ready = bool(
                expected_voice_track_path
                and not missing_tts_indexes
                and self._last_voice_track_output
                and self._last_voice_track_output.exists()
                and expected_voice_track_path.resolve() == self._last_voice_track_output.resolve()
            )
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
        if not self._workflow_current_stage:
            self._update_workflow_status_label()

    def closeEvent(self, event) -> None:  # noqa: N802
        self._cancel_preview_reload()
        self._preview_controller.close()
        super().closeEvent(event)

    def _append_log_line(self, message: str) -> None:
        self._logs_console.appendPlainText(message)



