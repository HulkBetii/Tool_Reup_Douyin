from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication, QFileDialog

from app.core.jobs import JobManager
from app.core.settings import build_default_settings
from app.ui import main_window as main_window_module
from app.ui.main_window import MainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def test_main_window_defaults_to_simple_v2_mode(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    app.processEvents()

    assert window._current_ui_mode() == "simple_v2"  # type: ignore[attr-defined]
    assert window._project_profile_combo.currentData() == "zh-vi-narration-fast-v2-vieneu"  # type: ignore[attr-defined]
    assert window._project_root_browse_button.text() == "Chọn"  # type: ignore[attr-defined]
    assert window._project_ops_group.isHidden() is True  # type: ignore[attr-defined]
    assert window._reload_prompts_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._voice_profile_name_input.isHidden() is True  # type: ignore[attr-defined]
    assert window._prepare_media_button.text() == "1. Chuẩn bị video"  # type: ignore[attr-defined]
    assert window._asr_translate_button.text() == "2. Tạo phụ đề"  # type: ignore[attr-defined]
    assert window._project_review_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._project_finish_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._open_export_video_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._open_export_video_button.isEnabled() is False  # type: ignore[attr-defined]
    assert window._neutral_narration_review_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._review_speaker_input.isEnabled() is False  # type: ignore[attr-defined]
    assert window._review_listener_input.isEnabled() is False  # type: ignore[attr-defined]
    assert window._review_self_term_input.isEnabled() is False  # type: ignore[attr-defined]
    assert window._review_address_term_input.isEnabled() is False  # type: ignore[attr-defined]
    assert window._review_fix_suggestion_combo.isEnabled() is False  # type: ignore[attr-defined]
    assert window._approve_relation_review_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._select_relation_review_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._dub_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._full_pipeline_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._source_lang_combo.currentText() == "zh"  # type: ignore[attr-defined]
    assert window._target_lang_combo.currentText() == "vi"  # type: ignore[attr-defined]
    assert window._asr_language_combo.currentText() == "zh"  # type: ignore[attr-defined]
    assert window._project_name_input.text() == ""  # type: ignore[attr-defined]
    assert Path(window._project_root_input.text()).parent == workspace_root  # type: ignore[attr-defined]
    assert window._original_volume_input.text() == "0.07"  # type: ignore[attr-defined]
    assert window._voice_volume_input.text() == "1.0"  # type: ignore[attr-defined]
    assert window._bgm_volume_input.text() == "0.0"  # type: ignore[attr-defined]
    logs_index = window._tabs.count() - 1  # type: ignore[attr-defined]
    assert window._tabs.tabBar().isTabVisible(logs_index) is False  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_open_export_video_button_uses_last_export_output(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    output_path = tmp_path / "exports" / "final.mp4"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"video")

    opened_urls: list[str] = []
    monkeypatch.setattr(
        main_window_module.QDesktopServices,
        "openUrl",
        lambda url: opened_urls.append(url.toLocalFile()) or True,
    )

    window._last_export_output = output_path  # type: ignore[attr-defined]
    window._refresh_export_access_actions()  # type: ignore[attr-defined]
    app.processEvents()

    assert window._open_export_video_button.isEnabled() is True  # type: ignore[attr-defined]

    window._open_last_export_video()  # type: ignore[attr-defined]

    assert [Path(path).resolve() for path in opened_urls] == [output_path.resolve()]

    window.close()
    app.processEvents()


def test_neutral_narration_review_button_resets_review_fields(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())

    window._pending_review_segment_id = "seg-1"  # type: ignore[attr-defined]
    window._review_speaker_input.setText("host")  # type: ignore[attr-defined]
    window._review_listener_input.setText("viewer")  # type: ignore[attr-defined]
    window._review_self_term_input.setText("toi")  # type: ignore[attr-defined]
    window._review_address_term_input.setText("ban")  # type: ignore[attr-defined]
    window._review_subtitle_input.setText("Ban da san sang chua? Bay gio bat dau nhe.")  # type: ignore[attr-defined]
    window._review_tts_input.setText("Ban da chuan bi chua? Gio chung ta bat dau nhe.")  # type: ignore[attr-defined]

    window._apply_neutral_narration_review_preset()  # type: ignore[attr-defined]

    assert window._review_speaker_input.text() == "narrator"  # type: ignore[attr-defined]
    assert window._review_listener_input.text() == "audience"  # type: ignore[attr-defined]
    assert window._review_self_term_input.text() == ""  # type: ignore[attr-defined]
    assert window._review_address_term_input.text() == ""  # type: ignore[attr-defined]
    assert window._review_subtitle_input.text() == "Gio bat dau."  # type: ignore[attr-defined]
    assert window._review_tts_input.text() == "Gio bat dau."  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_review_fix_suggestions_focus_simple_v2_on_text_fields(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())

    suggestions = window._review_fix_suggestions_for_reason_codes(  # type: ignore[attr-defined]
        ["sub_tts_pronoun_divergence", "ambiguous_term"]
    )

    assert suggestions[0] == "Ưu tiên sửa: Phụ đề duyệt + Lời TTS duyệt"
    assert "Giữ nguyên: Speaker = narrator, Listener = audience" in suggestions
    assert "Không sửa: Tự xưng / Gọi người nghe (để trống)" in suggestions
    assert "Sửa: Phụ đề duyệt + Lời TTS duyệt thành cùng một câu trung tính" in suggestions
    assert "Sửa: thuật ngữ / thực thể trong cả hai ô Phụ đề duyệt + Lời TTS duyệt" in suggestions

    window.close()
    app.processEvents()


def test_simple_v2_review_selection_locks_narration_fields(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    class FakeProjectDatabase:
        def __init__(self, _database_path):
            pass

        def get_project(self):
            return {"translation_mode": "contextual_v2"}

        def list_review_queue_items(self, _project_id):
            return [
                {
                    "segment_id": "seg-1",
                    "segment_index": 0,
                    "scene_index": 0,
                    "source_text": "准备好了吗",
                    "review_question": "",
                    "short_scene_summary": "narration scene",
                    "speaker_json": {"character_id": "host"},
                    "listeners_json": [{"character_id": "viewer"}],
                    "honorific_policy_json": {"self_term": "toi", "address_term": "ban"},
                    "review_reason_codes_json": ["sub_tts_pronoun_divergence"],
                }
            ]

        def get_segment_analysis(self, _project_id, _segment_id):
            return {
                "segment_id": "seg-1",
                "segment_index": 0,
                "scene_id": "scene_0001",
                "speaker_json": {"character_id": "host"},
                "listeners_json": [{"character_id": "viewer"}],
                "honorific_policy_json": {"self_term": "toi", "address_term": "ban"},
                "approved_subtitle_text": "Ban da san sang chua?",
                "approved_tts_text": "Gio chung ta bat dau nhe.",
                "review_reason_codes_json": ["sub_tts_pronoun_divergence"],
                "review_question": "",
            }

        def list_segment_analyses(self, _project_id):
            return [self.get_segment_analysis(_project_id, "seg-1")]

        def list_segments(self, _project_id):
            return [
                {
                    "segment_index": 0,
                    "source_text": "准备好了吗",
                    "subtitle_text": "Ban da san sang chua?",
                    "tts_text": "Gio chung ta bat dau nhe.",
                }
            ]

    monkeypatch.setattr(main_window_module, "ProjectDatabase", FakeProjectDatabase)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._current_workspace = SimpleNamespace(database_path=tmp_path / "project.db", project_id="project-1")  # type: ignore[attr-defined]

    window._reload_review_queue()  # type: ignore[attr-defined]
    app.processEvents()

    assert window._pending_review_segment_id == "seg-1"  # type: ignore[attr-defined]
    assert window._review_speaker_input.text() == "narrator"  # type: ignore[attr-defined]
    assert window._review_listener_input.text() == "audience"  # type: ignore[attr-defined]
    assert window._review_self_term_input.text() == ""  # type: ignore[attr-defined]
    assert window._review_address_term_input.text() == ""  # type: ignore[attr-defined]
    assert window._review_fix_suggestion_combo.isEnabled() is True  # type: ignore[attr-defined]
    assert window._review_fix_suggestion_combo.itemText(0) == "Ưu tiên sửa: Phụ đề duyệt + Lời TTS duyệt"  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_review_reopen_message_surfaces_reason_and_narration_hint(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())

    message = window._format_review_reopen_message(  # type: ignore[attr-defined]
        [
            {
                "segment_id": "seg-1",
                "segment_index": 26,
                "speaker_json": {"character_id": "narrator"},
                "listeners_json": [{"character_id": "audience"}],
                "review_reason_codes_json": ["pronoun_without_evidence", "addressee_mismatch"],
            }
        ]
    )

    assert "Dòng 27" in message
    assert "pronoun_without_evidence" in message
    assert "Narration trung tính" in message

    window.close()
    app.processEvents()


def test_main_window_can_switch_back_to_advanced_mode(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_ui_mode("advanced", persist=False)  # type: ignore[attr-defined]
    app.processEvents()

    assert window._current_ui_mode() == "advanced"  # type: ignore[attr-defined]
    assert window._project_profile_combo.count() >= 3  # type: ignore[attr-defined]
    assert window._project_ops_group.isHidden() is False  # type: ignore[attr-defined]
    assert window._reload_prompts_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._voice_profile_name_input.isHidden() is False  # type: ignore[attr-defined]
    assert window._review_speaker_input.isEnabled() is True  # type: ignore[attr-defined]
    assert window._review_listener_input.isEnabled() is True  # type: ignore[attr-defined]
    assert window._review_self_term_input.isEnabled() is True  # type: ignore[attr-defined]
    assert window._review_address_term_input.isEnabled() is True  # type: ignore[attr-defined]
    assert window._approve_relation_review_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._select_relation_review_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._prepare_media_button.text() == "Chuẩn bị media"  # type: ignore[attr-defined]
    assert window._asr_translate_button.text() == "ASR -> Dịch"  # type: ignore[attr-defined]
    assert window._project_review_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._project_finish_button.isHidden() is True  # type: ignore[attr-defined]
    assert window._dub_button.isHidden() is False  # type: ignore[attr-defined]
    assert window._full_pipeline_button.isHidden() is False  # type: ignore[attr-defined]
    logs_index = window._tabs.count() - 1  # type: ignore[attr-defined]
    assert window._tabs.tabBar().isTabVisible(logs_index) is True  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_choose_source_video_autofills_project_name_and_root(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    downloads_dir = tmp_path / "Downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    video_path = downloads_dir / "earth-depth.mp4"
    video_path.write_bytes(b"video")

    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)
    monkeypatch.setattr(main_window_module, "get_user_downloads_dir", lambda: downloads_dir)
    monkeypatch.setattr(
        QFileDialog,
        "getOpenFileName",
        lambda *args, **kwargs: (str(video_path), ""),
    )

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._choose_source_video()  # type: ignore[attr-defined]
    app.processEvents()

    assert window._source_video_input.text() == str(video_path)  # type: ignore[attr-defined]
    assert "earth-depth" in window._project_name_input.text().lower()  # type: ignore[attr-defined]
    assert Path(window._project_root_input.text()).parent == workspace_root  # type: ignore[attr-defined]
    assert Path(window._project_root_input.text()).name != ""  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_choose_source_video_preserves_manual_project_fields(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    downloads_dir = tmp_path / "Downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    video_path = downloads_dir / "human-longevity.mp4"
    video_path.write_bytes(b"video")
    custom_root = tmp_path / "custom-project-root"

    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)
    monkeypatch.setattr(main_window_module, "get_user_downloads_dir", lambda: downloads_dir)
    monkeypatch.setattr(
        QFileDialog,
        "getOpenFileName",
        lambda *args, **kwargs: (str(video_path), ""),
    )

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._project_name_input.setText("ten-tu-dat")  # type: ignore[attr-defined]
    window._handle_project_name_edited("ten-tu-dat")  # type: ignore[attr-defined]
    window._project_root_input.setText(str(custom_root))  # type: ignore[attr-defined]
    window._handle_project_root_edited(str(custom_root))  # type: ignore[attr-defined]

    window._choose_source_video()  # type: ignore[attr-defined]
    app.processEvents()

    assert window._project_name_input.text() == "ten-tu-dat"  # type: ignore[attr-defined]
    assert Path(window._project_root_input.text()) == custom_root  # type: ignore[attr-defined]

    window.close()
    app.processEvents()


def test_simple_finish_workflow_routes_to_review_when_pending(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._current_workspace = object()  # type: ignore[attr-defined]

    review_calls: list[str] = []
    warning_calls: list[str] = []
    monkeypatch.setattr(window, "_pending_review_count", lambda: 3)  # type: ignore[attr-defined]
    monkeypatch.setattr(window, "_open_review_queue_tab", lambda: review_calls.append("opened"))  # type: ignore[attr-defined]
    monkeypatch.setattr(main_window_module.QMessageBox, "warning", lambda *args: warning_calls.append("warn"))

    workflow_calls: list[tuple[list[str], str]] = []
    monkeypatch.setattr(
        window,
        "_start_workflow",
        lambda stages, *, workflow_name: workflow_calls.append((list(stages), workflow_name)),
    )  # type: ignore[attr-defined]

    window._start_simple_finish_workflow()  # type: ignore[attr-defined]

    assert review_calls == ["opened"]
    assert warning_calls == ["warn"]
    assert workflow_calls == []

    window.close()
    app.processEvents()


def test_simple_finish_workflow_runs_downstream_when_review_clear(tmp_path, monkeypatch):
    workspace_root = tmp_path / "workspace"
    monkeypatch.setattr(main_window_module, "get_default_workspace_dir", lambda: workspace_root)

    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._current_workspace = object()  # type: ignore[attr-defined]

    monkeypatch.setattr(window, "_pending_review_count", lambda: 0)  # type: ignore[attr-defined]
    workflow_calls: list[tuple[list[str], str]] = []
    monkeypatch.setattr(
        window,
        "_start_workflow",
        lambda stages, *, workflow_name: workflow_calls.append((list(stages), workflow_name)),
    )  # type: ignore[attr-defined]

    window._start_simple_finish_workflow()  # type: ignore[attr-defined]

    assert workflow_calls == [
        (["tts", "voice_track", "mixdown", "export_video"], "Hoàn thiện video")
    ]

    window.close()
    app.processEvents()
