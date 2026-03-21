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
from app.project.models import (
    ProjectInitRequest,
    RegisterVoiceStylePolicyRecord,
    RelationshipProfileRecord,
    SceneMemoryRecord,
    SegmentAnalysisRecord,
    SegmentRecord,
)
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


def _seed_register_style_project(tmp_path: Path):
    workspace = bootstrap_project(
        ProjectInitRequest(
            name="Register Style Preview",
            root_dir=tmp_path / "register-project",
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
                source_text="快点走。",
                source_text_norm="快点走。",
            ),
            SegmentRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                segment_index=1,
                start_ms=1000,
                end_ms=2000,
                source_lang="zh",
                source_text="别闹了。",
                source_text_norm="别闹了。",
            ),
        ],
    )
    database.upsert_relationship_profiles(
        [
            RelationshipProfileRecord(
                relationship_id="rel:char_a->char_b",
                project_id=workspace.project_id,
                from_character_id="char_a",
                to_character_id="char_b",
                relation_type="teacher_student",
                created_at=now,
                updated_at=now,
            ),
            RelationshipProfileRecord(
                relationship_id="rel:char_b->char_a",
                project_id=workspace.project_id,
                from_character_id="char_b",
                to_character_id="char_a",
                relation_type="friend",
                created_at=now,
                updated_at=now,
            ),
        ]
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
                short_scene_summary="A ra lenh, B phan ung",
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
                speaker_json={"character_id": "char_a", "confidence": 0.95},
                listeners_json=[{"character_id": "char_b", "confidence": 0.92, "role": "primary"}],
                register_json={
                    "politeness": "formal",
                    "power_direction": "down",
                    "emotional_tone": "serious",
                },
                turn_function="command",
                confidence_json={"speaker": 0.95, "relation": 0.91},
                semantic_translation="Nhanh lên.",
                approved_subtitle_text="Nhanh lên.",
                approved_tts_text="Nhanh lên.",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
            SegmentAnalysisRecord(
                segment_id="seg-2",
                project_id=workspace.project_id,
                scene_id="scene-1",
                segment_index=1,
                speaker_json={"character_id": "char_b", "confidence": 0.94},
                listeners_json=[{"character_id": "char_a", "confidence": 0.9, "role": "primary"}],
                register_json={
                    "politeness": "casual",
                    "power_direction": "even",
                    "emotional_tone": "playful",
                },
                turn_function="tease",
                confidence_json={"speaker": 0.94, "relation": 0.88},
                semantic_translation="Đừng la nữa.",
                approved_subtitle_text="Đừng la nữa.",
                approved_tts_text="Đừng la nữa.",
                semantic_qc_passed=True,
                created_at=now,
                updated_at=now,
            ),
        ],
    )
    database.replace_register_voice_style_policies(
        workspace.project_id,
        [
            RegisterVoiceStylePolicyRecord(
                policy_id="registerstyle:formal:down:serious:command:teacher_student",
                project_id=workspace.project_id,
                politeness="formal",
                power_direction="down",
                emotional_tone="serious",
                turn_function="command",
                relation_type="teacher_student",
                speed_override=0.88,
                volume_override=1.05,
                pitch_override=-0.6,
                created_at=now,
                updated_at=now,
            )
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


def _find_register_style_row(
    window: MainWindow,
    *,
    politeness: str,
    power_direction: str,
    emotional_tone: str,
    turn_function: str,
    relation_type: str,
) -> int:
    table = window._register_voice_style_table  # type: ignore[attr-defined]
    expected = {
        "politeness": politeness,
        "power_direction": power_direction,
        "emotional_tone": emotional_tone,
        "turn_function": turn_function,
        "relation_type": relation_type,
    }
    for row_index in range(table.rowCount()):
        item = table.item(row_index, 0)
        payload = item.data(Qt.ItemDataRole.UserRole) if item is not None else {}
        if isinstance(payload, dict) and payload == expected:
            return row_index
    raise AssertionError(f"Khong tim thay row register style {expected}")


def _register_style_texts(window: MainWindow, row_index: int) -> tuple[str, str, str]:
    return (
        window._register_voice_style_override_input(row_index, 7).text(),  # type: ignore[attr-defined]
        window._register_voice_style_override_input(row_index, 8).text(),  # type: ignore[attr-defined]
        window._register_voice_style_override_input(row_index, 9).text(),  # type: ignore[attr-defined]
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


def test_register_voice_style_bulk_fill_and_clear_selected_row_styles(
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
    workspace = _seed_register_style_project(tmp_path)
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    app.processEvents()

    row_formal = _find_register_style_row(
        window,
        politeness="formal",
        power_direction="down",
        emotional_tone="serious",
        turn_function="command",
        relation_type="teacher_student",
    )
    row_casual = _find_register_style_row(
        window,
        politeness="casual",
        power_direction="even",
        emotional_tone="playful",
        turn_function="tease",
        relation_type="friend",
    )
    table = window._register_voice_style_table  # type: ignore[attr-defined]

    assert _register_style_texts(window, row_formal) == ("0.88", "1.05", "-0.6")
    assert _register_style_texts(window, row_casual) == ("", "", "")

    monkeypatch.setattr(
        window,
        "_selected_table_row_indexes",
        lambda current_table: [row_casual] if current_table is table else [],
    )
    window._voice_speed_profile_input.setText("1.08")  # type: ignore[attr-defined]
    window._voice_profile_volume_input.setText("0.95")  # type: ignore[attr-defined]
    window._voice_pitch_input.setText("1.4")  # type: ignore[attr-defined]
    window._fill_selected_register_voice_rows_with_current_style()  # type: ignore[attr-defined]
    app.processEvents()

    assert _register_style_texts(window, row_formal) == ("0.88", "1.05", "-0.6")
    assert _register_style_texts(window, row_casual) == ("1.08", "0.95", "1.4")
    assert "2/2" in window._register_voice_style_status.text()  # type: ignore[attr-defined]

    window._clear_selected_register_voice_row_styles()  # type: ignore[attr-defined]
    app.processEvents()

    assert _register_style_texts(window, row_formal) == ("0.88", "1.05", "-0.6")
    assert _register_style_texts(window, row_casual) == ("", "", "")

    window.close()
    app.processEvents()


def test_voice_plan_preview_reports_register_style_sources_and_rerun_button_uses_downstream_stages(
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
    workspace = _seed_register_style_project(tmp_path)
    settings = build_default_settings(tmp_path / "appdata")
    window = MainWindow(settings=settings, job_manager=JobManager())
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    app.processEvents()

    database = ProjectDatabase(workspace.database_path)
    subtitle_rows = [
        {"segment_id": "evt-1", "source_segment_id": "seg-1", "tts_text": "Nhanh lên."},
        {"segment_id": "evt-2", "source_segment_id": "seg-2", "tts_text": "Đừng la nữa."},
    ]
    default_preset, segment_voice_presets, _segment_speaker_keys, voice_plan = window._resolve_tts_voice_plan(  # type: ignore[attr-defined]
        database,
        subtitle_rows,
        require_localized=False,
        dialog_title="Voice plan preview",
        warn_on_unresolved=False,
    )
    window._refresh_effective_voice_plan_preview(  # type: ignore[attr-defined]
        subtitle_rows=subtitle_rows,
        require_localized=False,
        default_preset=default_preset,
        segment_voice_presets=segment_voice_presets,
        voice_plan=voice_plan,
    )

    preview_text = window._effective_voice_plan_preview.toPlainText()  # type: ignore[attr-defined]
    assert "Effective voice plan:" in preview_text
    assert "Nguồn style:" in preview_text
    assert "register_policy=1" in preview_text
    assert "field_sources=pitch:register_policy,speed:register_policy,volume:register_policy" in preview_text

    captured: dict[str, object] = {}

    def _capture_start_workflow(stages, *, workflow_name):
        captured["stages"] = list(stages)
        captured["workflow_name"] = workflow_name

    monkeypatch.setattr(window, "_start_workflow", _capture_start_workflow)
    window._rerun_downstream_only_button.click()  # type: ignore[attr-defined]

    assert captured == {
        "stages": ["tts", "voice_track", "mixdown", "export_video"],
        "workflow_name": "Chạy lại downstream",
    }

    window.close()
    app.processEvents()
