from __future__ import annotations

import os
from pathlib import Path

from PySide6.QtWidgets import QApplication, QAbstractScrollArea, QSizePolicy

from app.core.jobs import JobManager
from app.core.settings import build_default_settings
from app.ui.main_window import MainWindow
from app.ui.status_panel import StatusPanel

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def test_main_window_uses_readable_vietnamese_ui_strings(tmp_path):
    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    app.processEvents()

    tab_titles = [window._tabs.tabText(index) for index in range(window._tabs.count())]  # type: ignore[attr-defined]
    assert tab_titles == [
        "Dự án",
        "ASR & Dịch",
        "Phụ đề",
        "Lồng tiếng",
        "Xuất bản",
        "Cài đặt",
        "Nhật ký",
    ]
    assert window._project_summary.text() == "Chưa mở dự án"  # type: ignore[attr-defined]
    assert window._doctor_summary.text() == "Doctor: chưa chạy"  # type: ignore[attr-defined]
    assert window._settings_doctor_status.text() == "Doctor: chưa chạy"  # type: ignore[attr-defined]
    assert window._fill_voice_policy_styles_button.text() == "Điền style trống"  # type: ignore[attr-defined]
    assert window._clear_selected_register_voice_styles_button.text() == "Xóa style register dòng chọn"  # type: ignore[attr-defined]
    assert (
        window._effective_voice_plan_preview.placeholderText()  # type: ignore[attr-defined]
        == "Effective voice plan của track hiện tại sẽ hiện ở đây."
    )

    window.close()
    app.processEvents()


def test_main_window_uses_responsive_label_table_and_scroll_policies(tmp_path):
    app = QApplication.instance() or QApplication([])
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    app.processEvents()

    project_scroll = window._tabs.widget(0)  # type: ignore[attr-defined]
    assert project_scroll is not None
    assert project_scroll.widgetResizable() is True
    assert project_scroll.sizePolicy().horizontalPolicy() == QSizePolicy.Policy.Expanding

    assert window._project_summary.wordWrap() is True  # type: ignore[attr-defined]
    assert window._project_summary.sizePolicy().horizontalPolicy() == QSizePolicy.Policy.Expanding  # type: ignore[attr-defined]
    assert window._project_summary.minimumWidth() == 0  # type: ignore[attr-defined]

    for table in (
        window._review_table,  # type: ignore[attr-defined]
        window._subtitle_table,  # type: ignore[attr-defined]
        window._speaker_binding_table,  # type: ignore[attr-defined]
        window._character_voice_policy_table,  # type: ignore[attr-defined]
        window._relationship_voice_policy_table,  # type: ignore[attr-defined]
        window._register_voice_style_table,  # type: ignore[attr-defined]
    ):
        assert table.sizeAdjustPolicy() == QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents
        assert table.sizePolicy().horizontalPolicy() == QSizePolicy.Policy.Expanding

    window.close()
    app.processEvents()


def test_status_panel_uses_readable_vietnamese_and_responsive_table():
    app = QApplication.instance() or QApplication([])
    panel = StatusPanel()
    app.processEvents()

    assert panel._title.text() == "Tiến trình tác vụ"  # type: ignore[attr-defined]
    assert panel._cancel_button.text() == "Hủy"  # type: ignore[attr-defined]
    assert panel._retry_button.text() == "Chạy lại"  # type: ignore[attr-defined]
    expected_headers = ["Mã job", "Công đoạn", "Trạng thái", "Tiến độ", "Thông báo"]
    actual_headers = [
        panel._table.horizontalHeaderItem(index).text()  # type: ignore[attr-defined]
        for index in range(panel._table.columnCount())  # type: ignore[attr-defined]
    ]
    assert actual_headers == expected_headers
    assert panel._table.sizeAdjustPolicy() == QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents  # type: ignore[attr-defined]
    assert panel._table.sizePolicy().horizontalPolicy() == QSizePolicy.Policy.Expanding  # type: ignore[attr-defined]

    panel.close()
    app.processEvents()


def test_ui_source_files_do_not_contain_common_mojibake_markers():
    repo_root = Path(__file__).resolve().parents[2]
    ui_root = repo_root / "src" / "app" / "ui"
    broken_markers = ["�", "Ã", "á»", "áº", "Ä‘", "Æ°", "Há»", "ChÆ°", "Dá»±"]
    broken_phrases = ["da_nhap=", "thieu_txt=", "txt_rong=", "Repair xong", "Hay mo du an truoc", "Du an hien tai"]

    for path in ui_root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for marker in broken_markers:
            assert marker not in text, f"{path} still contains mojibake marker {marker!r}"
        for phrase in broken_phrases:
            assert phrase not in text, f"{path} still contains unaccented UI phrase {phrase!r}"
