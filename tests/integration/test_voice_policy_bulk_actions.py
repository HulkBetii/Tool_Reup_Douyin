from __future__ import annotations

import os
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMessageBox
import pytest

from app.core.jobs import JobManager
from app.core.settings import build_default_settings
from app.project.bootstrap import bootstrap_project
from app.project.database import ProjectDatabase
from app.project.models import ProjectInitRequest, SceneMemoryRecord, SegmentAnalysisRecord, SegmentRecord
from app.ui.main_window import MainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _seed_voice_policy_project(tmp_path: Path):
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Voice Policy Bulk Actions",
            root_dir=tmp_path / "project",
            source_language="zh",
            target_language="vi",
            translation_mode="contextual_v2",
        )
    )
    database = ProjectDatabase(workspace.database_path)
    now = "2026-03-20T00:00:00+00:00"
    database.replace_segments(
        workspace.project_id,
        [
            SegmentRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                segment_index=0,
                start_ms=0,
                end_ms=1000,
                source_lang="zh",
                source_text="走吧。",
                source_text_norm="走吧。",
            ),
            SegmentRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                segment_index=1,
                start_ms=1000,
                end_ms=2000,
                source_lang="zh",
                source_text="快点。",
                source_text_norm="快点。",
            ),
        ],
    )
    database.replace_contextual_translation_state(
        workspace.project_id,
        scenes=[
            SceneMemoryRecord(
                scene_id="scene-1",
                project_id=workspace.project_id,
                scene_index=0,
                start_segment_index=0,
                end_segment_index=1,
                start_ms=0,
                end_ms=2000,
                short_scene_summary="A và B đối thoại",
                created_at=now,
                updated_at=now,
            )
        ],
        analyses=[
            SegmentAnalysisRecord(
                segment_id="seg-1",
                project_id=workspace.project_id,
                scene_id="scene-1",
                segment_index=0,
                speaker_json={"character_id": "char_a", "confidence": 1.0},
                semantic_translation="Đi thôi.",
                approved_subtitle_text="Đi thôi.",
                approved_tts_text="Đi thôi.",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                scene_id="scene-1",
                segment_index=1,
                speaker_json={"character_id": "char_b", "confidence": 1.0},
                semantic_translation="Nhanh lên.",
                approved_subtitle_text="Nhanh lên.",
                approved_tts_text="Nhanh lên.",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
        ],
    )
    return workspace


def _find_character_policy_row(window: MainWindow, speaker_key: str) -> int:
    table = window._character_voice_policy_table  # type: ignore[attr-defined]
    for row_index in range(table.rowCount()):
        item = table.item(row_index, 0)
        payload = item.data(Qt.ItemDataRole.UserRole) if item is not None else {}
        if isinstance(payload, dict) and payload.get("speaker_character_id") == speaker_key:
            return row_index
    raise AssertionError(f"Không tìm thấy row cho {speaker_key}")


def _style_texts(window: MainWindow, row_index: int) -> tuple[str, str, str]:
    table = window._character_voice_policy_table  # type: ignore[attr-defined]
    return (
        window._voice_policy_override_input(table, row_index, 3).text(),  # type: ignore[attr-defined]
        window._voice_policy_override_input(table, row_index, 4).text(),  # type: ignore[attr-defined]
        window._voice_policy_override_input(table, row_index, 5).text(),  # type: ignore[attr-defined]
    )


def test_voice_policy_bulk_fill_unstyled_rows_from_current_preset_form(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(QMessageBox, "warning", staticmethod(lambda *args, **kwargs: QMessageBox.StandardButton.Ok))
    monkeypatch.setattr(
        QMessageBox,
        "information",
        staticmethod(lambda *args, **kwargs: QMessageBox.StandardButton.Ok),
    )
    app = QApplication.instance() or QApplication([])
    workspace = _seed_voice_policy_project(tmp_path)
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    app.processEvents()

    row_a = _find_character_policy_row(window, "char_a")
    row_b = _find_character_policy_row(window, "char_b")
    assert _style_texts(window, row_a) == ("", "", "")
    assert _style_texts(window, row_b) == ("", "", "")

    window._voice_speed_profile_input.setText("1.11")  # type: ignore[attr-defined]
    window._voice_profile_volume_input.setText("0.97")  # type: ignore[attr-defined]
    window._voice_pitch_input.setText("2.5")  # type: ignore[attr-defined]
    window._fill_unstyled_voice_policies_with_current_style()  # type: ignore[attr-defined]
    app.processEvents()

    assert _style_texts(window, row_a) == ("1.11", "0.97", "2.5")
    assert _style_texts(window, row_b) == ("1.11", "0.97", "2.5")

    window.close()
    app.processEvents()


def test_voice_policy_bulk_fill_and_clear_selected_row_styles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(QMessageBox, "warning", staticmethod(lambda *args, **kwargs: QMessageBox.StandardButton.Ok))
    monkeypatch.setattr(
        QMessageBox,
        "information",
        staticmethod(lambda *args, **kwargs: QMessageBox.StandardButton.Ok),
    )
    app = QApplication.instance() or QApplication([])
    workspace = _seed_voice_policy_project(tmp_path)
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    app.processEvents()

    row_a = _find_character_policy_row(window, "char_a")
    row_b = _find_character_policy_row(window, "char_b")
    table = window._character_voice_policy_table  # type: ignore[attr-defined]

    monkeypatch.setattr(
        window,
        "_selected_table_row_indexes",
        lambda current_table: [row_a] if current_table is table else [],
    )
    window._voice_speed_profile_input.setText("0.89")  # type: ignore[attr-defined]
    window._voice_profile_volume_input.setText("1.2")  # type: ignore[attr-defined]
    window._voice_pitch_input.setText("-1.5")  # type: ignore[attr-defined]
    window._fill_selected_voice_policy_rows_with_current_style()  # type: ignore[attr-defined]
    app.processEvents()

    assert _style_texts(window, row_a) == ("0.89", "1.2", "-1.5")
    assert _style_texts(window, row_b) == ("", "", "")

    window._clear_selected_voice_policy_row_styles()  # type: ignore[attr-defined]
    app.processEvents()

    assert _style_texts(window, row_a) == ("", "", "")
    assert _style_texts(window, row_b) == ("", "", "")

    window.close()
    app.processEvents()
