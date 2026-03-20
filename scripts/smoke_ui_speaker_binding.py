from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from PySide6.QtCore import QItemSelectionModel, QSize
from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.settings import load_settings
from app.project.bootstrap import open_project
from app.ui.main_window import MainWindow


def _select_voice_preset(window: MainWindow, voice_preset_id: str) -> None:
    for index in range(window._voice_combo.count()):  # type: ignore[attr-defined]
        if str(window._voice_combo.itemData(index) or "").strip() == voice_preset_id:  # type: ignore[attr-defined]
            window._voice_combo.setCurrentIndex(index)  # type: ignore[attr-defined]
            return
    raise ValueError(f"Voice preset not found: {voice_preset_id}")


def _speaker_binding_table_rows(window: MainWindow) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    table = window._speaker_binding_table  # type: ignore[attr-defined]
    for row_index in range(table.rowCount()):
        speaker_item = table.item(row_index, 0)
        count_item = table.item(row_index, 1)
        status_item = table.item(row_index, 3)
        combo = table.cellWidget(row_index, 2)
        rows.append(
            {
                "speaker": speaker_item.text() if speaker_item else "",
                "line_count": count_item.text() if count_item else "",
                "voice_preset_id": str(combo.currentData() or "").strip() if combo is not None else "",
                "status": status_item.text() if status_item else "",
            }
        )
    return rows


def _save_window_snapshot(window: MainWindow, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    window.grab().save(str(path))


def _save_widget_snapshot(widget, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    widget.grab().save(str(path))


def _select_rows(table, row_indexes: list[int]) -> None:
    selection_model = table.selectionModel()
    if selection_model is None:
        return
    table.clearSelection()
    for row_index in row_indexes:
        selection_model.select(
            table.model().index(row_index, 0),
            QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows,
        )
    if row_indexes:
        table.setCurrentCell(row_indexes[0], 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test speaker binding UI for a project.")
    parser.add_argument("--project-root", required=True, help="Path to the project root directory.")
    parser.add_argument(
        "--voice-preset-id",
        default="default-sapi",
        help="Preset id used for the quick-fill smoke action.",
    )
    parser.add_argument(
        "--output-dir",
        default="ui_smoke/speaker_binding",
        help="Relative output directory under the project root.",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    output_dir = project_root / args.output_dir

    app = QApplication(sys.argv)
    settings = load_settings()
    window = MainWindow(settings=settings, job_manager=JobManager())
    window.resize(QSize(1440, 960))
    window.show()
    app.processEvents()

    workspace = open_project(project_root)
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    window._tabs.setCurrentIndex(3)  # type: ignore[attr-defined]
    app.processEvents()

    initial_snapshot = output_dir / "speaker_binding_initial.png"
    _save_window_snapshot(window, initial_snapshot)
    initial_table_snapshot = output_dir / "speaker_binding_table_initial.png"
    _save_widget_snapshot(window._speaker_binding_table, initial_table_snapshot)  # type: ignore[attr-defined]
    initial_status_snapshot = output_dir / "speaker_binding_status_initial.png"
    _save_widget_snapshot(window._speaker_binding_status, initial_status_snapshot)  # type: ignore[attr-defined]

    initial_summary = {
        "status_text": window._speaker_binding_status.text(),  # type: ignore[attr-defined]
        "voice_summary": window._voice_summary.text(),  # type: ignore[attr-defined]
        "rows": _speaker_binding_table_rows(window),
    }

    _select_voice_preset(window, args.voice_preset_id)
    table = window._speaker_binding_table  # type: ignore[attr-defined]
    selected_rows = list(range(min(2, table.rowCount())))
    _select_rows(table, selected_rows)
    window._fill_selected_speaker_bindings_with_selected_preset()  # type: ignore[attr-defined]
    app.processEvents()

    selected_fill_snapshot = output_dir / "speaker_binding_selected_fill.png"
    _save_window_snapshot(window, selected_fill_snapshot)
    selected_fill_table_snapshot = output_dir / "speaker_binding_table_selected_fill.png"
    _save_widget_snapshot(table, selected_fill_table_snapshot)
    selected_fill_status_snapshot = output_dir / "speaker_binding_status_selected_fill.png"
    _save_widget_snapshot(window._speaker_binding_status, selected_fill_status_snapshot)  # type: ignore[attr-defined]

    selected_fill_summary = {
        "status_text": window._speaker_binding_status.text(),  # type: ignore[attr-defined]
        "voice_summary": window._voice_summary.text(),  # type: ignore[attr-defined]
        "rows": _speaker_binding_table_rows(window),
        "selected_rows": selected_rows,
    }

    _select_rows(table, selected_rows)
    window._clear_selected_speaker_bindings()  # type: ignore[attr-defined]
    app.processEvents()

    selected_clear_snapshot = output_dir / "speaker_binding_selected_clear.png"
    _save_window_snapshot(window, selected_clear_snapshot)
    selected_clear_table_snapshot = output_dir / "speaker_binding_table_selected_clear.png"
    _save_widget_snapshot(table, selected_clear_table_snapshot)
    selected_clear_status_snapshot = output_dir / "speaker_binding_status_selected_clear.png"
    _save_widget_snapshot(window._speaker_binding_status, selected_clear_status_snapshot)  # type: ignore[attr-defined]

    selected_clear_summary = {
        "status_text": window._speaker_binding_status.text(),  # type: ignore[attr-defined]
        "voice_summary": window._voice_summary.text(),  # type: ignore[attr-defined]
        "rows": _speaker_binding_table_rows(window),
        "selected_rows": selected_rows,
    }

    window._fill_unbound_speakers_with_selected_preset()  # type: ignore[attr-defined]
    app.processEvents()

    filled_snapshot = output_dir / "speaker_binding_filled.png"
    _save_window_snapshot(window, filled_snapshot)
    filled_table_snapshot = output_dir / "speaker_binding_table_filled.png"
    _save_widget_snapshot(window._speaker_binding_table, filled_table_snapshot)  # type: ignore[attr-defined]
    filled_status_snapshot = output_dir / "speaker_binding_status_filled.png"
    _save_widget_snapshot(window._speaker_binding_status, filled_status_snapshot)  # type: ignore[attr-defined]

    filled_summary = {
        "status_text": window._speaker_binding_status.text(),  # type: ignore[attr-defined]
        "voice_summary": window._voice_summary.text(),  # type: ignore[attr-defined]
        "rows": _speaker_binding_table_rows(window),
    }

    summary_path = output_dir / "speaker_binding_ui_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "project_root": str(project_root),
                "voice_preset_id": args.voice_preset_id,
                "initial_snapshot": str(initial_snapshot),
                "initial_table_snapshot": str(initial_table_snapshot),
                "initial_status_snapshot": str(initial_status_snapshot),
                "filled_snapshot": str(filled_snapshot),
                "filled_table_snapshot": str(filled_table_snapshot),
                "filled_status_snapshot": str(filled_status_snapshot),
                "selected_fill_snapshot": str(selected_fill_snapshot),
                "selected_fill_table_snapshot": str(selected_fill_table_snapshot),
                "selected_fill_status_snapshot": str(selected_fill_status_snapshot),
                "selected_clear_snapshot": str(selected_clear_snapshot),
                "selected_clear_table_snapshot": str(selected_clear_table_snapshot),
                "selected_clear_status_snapshot": str(selected_clear_status_snapshot),
                "initial": initial_summary,
                "selected_fill": selected_fill_summary,
                "selected_clear": selected_clear_summary,
                "filled": filled_summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(summary_path)
    window.close()
    app.processEvents()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
