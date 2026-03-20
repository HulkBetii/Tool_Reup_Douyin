from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from PySide6.QtCore import QItemSelectionModel, QSize, Qt
from PySide6.QtWidgets import QApplication

from app.core.jobs import JobManager
from app.core.settings import load_settings
from app.project.bootstrap import open_project, utc_now_iso
from app.project.database import ProjectDatabase
from app.ui.main_window import MainWindow


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


def _pair_key(row: object) -> tuple[str, str]:
    speaker = _analysis_json_field(row, "speaker_json", {})
    listeners = _analysis_json_field(row, "listeners_json", [])
    speaker_id = str(speaker.get("character_id", "unknown")) if isinstance(speaker, dict) else "unknown"
    if isinstance(listeners, list) and listeners:
        first_listener = listeners[0] or {}
        if isinstance(first_listener, dict):
            listener_id = str(first_listener.get("character_id", "unknown"))
        else:
            listener_id = "unknown"
    else:
        listener_id = "unknown"
    return speaker_id, listener_id


def _prepare_project_copy(source_root: Path, target_root: Path) -> Path:
    if target_root.exists():
        shutil.rmtree(target_root)
    shutil.copytree(source_root, target_root)

    database_path = target_root / "project.db"
    now = utc_now_iso()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE projects SET root_dir = ?, name = ?, updated_at = ?",
            (str(target_root), f"{target_root.name} (Bulk Review Smoke)", now),
        )
        connection.commit()
    return target_root


def _choose_seed_rows(database: ProjectDatabase, project_id: str) -> dict[str, object]:
    analysis_rows = database.list_segment_analyses(project_id)
    grouped: dict[tuple[str, str], dict[str, list[object]]] = defaultdict(lambda: defaultdict(list))
    for row in analysis_rows:
        pair = _pair_key(row)
        if not pair[0] or not pair[1]:
            continue
        if pair[0].lower().startswith("unknown") or pair[1].lower().startswith("unknown"):
            continue
        grouped[pair][str(row["scene_id"])].append(row)

    ranked_pairs = sorted(
        grouped.items(),
        key=lambda item: (
            -sum(len(scene_rows) for scene_rows in item[1].values()),
            -len(item[1]),
            item[0],
        ),
    )
    if not ranked_pairs:
        raise RuntimeError("Khong tim thay cap speaker/listener phu hop de smoke test bulk review.")

    pair, scenes = ranked_pairs[0]
    ranked_scenes = sorted(
        scenes.items(),
        key=lambda item: (-len(item[1]), min(int(row["segment_index"]) for row in item[1])),
    )
    primary_scene_id, primary_rows = ranked_scenes[0]
    primary_rows = sorted(primary_rows, key=lambda row: int(row["segment_index"]))

    selected_rows = primary_rows[:2]
    secondary_row = None
    for scene_id, scene_rows in ranked_scenes[1:]:
        if scene_id == primary_scene_id:
            continue
        secondary_row = sorted(scene_rows, key=lambda row: int(row["segment_index"]))[0]
        break
    if secondary_row is None and len(primary_rows) >= 3:
        secondary_row = primary_rows[2]
    if secondary_row is None:
        raise RuntimeError("Khong du du lieu de tao kịch bản bulk review gom scene va cross-scene.")

    seeded_rows = [selected_rows[0], selected_rows[1], secondary_row]
    return {
        "pair": {"speaker": pair[0], "listener": pair[1]},
        "primary_scene_id": primary_scene_id,
        "seed_segment_ids": [str(row["segment_id"]) for row in seeded_rows],
        "selected_segment_ids": [str(selected_rows[0]["segment_id"]), str(secondary_row["segment_id"])],
    }


def _seed_review_queue(database: ProjectDatabase, project_id: str, *, seed_segment_ids: list[str]) -> None:
    now = utc_now_iso()
    for segment_id in seed_segment_ids:
        row = database.get_segment_analysis(project_id, segment_id)
        if row is None:
            continue
        database.update_segment_analysis_review(
            project_id,
            segment_id,
            needs_human_review=True,
            review_status="needs_review",
            review_scope="line",
            review_reason_codes_json=["uncertain_speaker"],
            review_question="Bulk review smoke test: xac nhan nguoi noi va nguoi nghe.",
            semantic_qc_passed=True,
            semantic_qc_issues_json=[],
            updated_at=now,
        )


def _selected_review_segment_ids(window: MainWindow) -> list[str]:
    table = window._review_table  # type: ignore[attr-defined]
    rows: list[str] = []
    selection_model = table.selectionModel()
    if selection_model is None:
        return rows
    for index in selection_model.selectedRows():
        item = table.item(index.row(), 0)
        if item is None:
            continue
        segment_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if segment_id:
            rows.append(segment_id)
    return rows


def _review_table_rows(window: MainWindow) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    table = window._review_table  # type: ignore[attr-defined]
    for row_index in range(table.rowCount()):
        id_item = table.item(row_index, 0)
        rows.append(
            {
                "segment_id": str(id_item.data(Qt.ItemDataRole.UserRole) or "").strip() if id_item else "",
                "line": id_item.text() if id_item else "",
                "scene": table.item(row_index, 1).text() if table.item(row_index, 1) else "",
                "source_text": table.item(row_index, 2).text() if table.item(row_index, 2) else "",
                "speaker": table.item(row_index, 3).text() if table.item(row_index, 3) else "",
                "listener": table.item(row_index, 4).text() if table.item(row_index, 4) else "",
                "policy": table.item(row_index, 5).text() if table.item(row_index, 5) else "",
                "reason": table.item(row_index, 6).text() if table.item(row_index, 6) else "",
            }
        )
    return rows


def _save_widget_snapshot(widget, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    widget.grab().save(str(path))


def _select_explicit_review_rows(window: MainWindow, segment_ids: list[str]) -> list[int]:
    table = window._review_table  # type: ignore[attr-defined]
    selection_model = table.selectionModel()
    if selection_model is None:
        return []
    row_indexes: list[int] = []
    table.clearSelection()
    wanted = set(segment_ids)
    for row_index in range(table.rowCount()):
        item = table.item(row_index, 0)
        if item is None:
            continue
        row_segment_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if row_segment_id not in wanted:
            continue
        row_indexes.append(row_index)
        selection_model.select(
            table.model().index(row_index, 0),
            QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows,
        )
    if row_indexes:
        selection_model.setCurrentIndex(
            table.model().index(row_indexes[0], 0),
            QItemSelectionModel.SelectionFlag.NoUpdate,
        )
        window._handle_review_selection_changed()  # type: ignore[attr-defined]
    return row_indexes


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test bulk review UI on a copied contextual project.")
    parser.add_argument("--source-project-root", required=True, help="Path to an existing project root.")
    parser.add_argument(
        "--target-project-root",
        default="workspace/bulk-review-smoke-20260320",
        help="Path to the copied smoke project root.",
    )
    parser.add_argument(
        "--output-dir",
        default="ui_smoke/bulk_review",
        help="Relative output directory under the copied project root.",
    )
    args = parser.parse_args()

    source_root = Path(args.source_project_root).expanduser().resolve()
    target_root = Path(args.target_project_root).expanduser().resolve()
    if not target_root.is_absolute():
        target_root = Path.cwd() / target_root
    target_root = target_root.resolve()
    copied_root = _prepare_project_copy(source_root, target_root)
    database = ProjectDatabase(copied_root / "project.db")
    project_row = database.get_project()
    if project_row is None:
        raise RuntimeError("Khong tim thay project row trong project copy.")
    project_id = str(project_row["project_id"])

    seed_info = _choose_seed_rows(database, project_id)
    _seed_review_queue(database, project_id, seed_segment_ids=list(seed_info["seed_segment_ids"]))

    output_dir = copied_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    app = QApplication(sys.argv)
    settings = load_settings()
    window = MainWindow(settings=settings, job_manager=JobManager())
    window.resize(QSize(1440, 960))
    window.show()
    app.processEvents()

    workspace = open_project(copied_root)
    window._set_current_workspace(workspace)  # type: ignore[attr-defined]
    window._tabs.setCurrentIndex(1)  # type: ignore[attr-defined]
    window._reload_review_queue()  # type: ignore[attr-defined]
    app.processEvents()

    review_table = window._review_table  # type: ignore[attr-defined]
    review_summary = {
        "initial_pending_count": review_table.rowCount(),
        "initial_rows": _review_table_rows(window),
        "initial_selected": _selected_review_segment_ids(window),
    }
    _save_widget_snapshot(review_table, output_dir / "review_table_initial.png")
    _save_widget_snapshot(window._review_context_text, output_dir / "review_context_initial.png")  # type: ignore[attr-defined]

    window._select_review_rows_by_scope("scene")  # type: ignore[attr-defined]
    app.processEvents()
    scene_selected_ids = _selected_review_segment_ids(window)
    _save_widget_snapshot(review_table, output_dir / "review_table_scene_selected.png")

    window._select_review_rows_by_scope("relation")  # type: ignore[attr-defined]
    app.processEvents()
    relation_selected_ids = _selected_review_segment_ids(window)
    _save_widget_snapshot(review_table, output_dir / "review_table_relation_selected.png")

    explicit_row_indexes = _select_explicit_review_rows(window, list(seed_info["selected_segment_ids"]))
    app.processEvents()
    explicit_selected_ids = _selected_review_segment_ids(window)
    _save_widget_snapshot(review_table, output_dir / "review_table_explicit_selected.png")

    window._apply_review_resolution_to_selected_rows()  # type: ignore[attr-defined]
    app.processEvents()
    after_selected_pending = database.count_pending_segment_reviews(project_id)
    after_selected_rows = _review_table_rows(window)
    _save_widget_snapshot(review_table, output_dir / "review_table_after_selected_apply.png")

    if review_table.rowCount() > 0:
        review_table.setCurrentCell(0, 0)
        window._handle_review_selection_changed()  # type: ignore[attr-defined]
        window._apply_review_resolution("line")  # type: ignore[attr-defined]
        app.processEvents()

    final_pending = database.count_pending_segment_reviews(project_id)
    final_rows = _review_table_rows(window)
    _save_widget_snapshot(review_table, output_dir / "review_table_final.png")

    summary = {
        "source_project_root": str(source_root),
        "copied_project_root": str(copied_root),
        "seed_info": seed_info,
        "initial": review_summary,
        "scene_selection": {
            "selected_segment_ids": scene_selected_ids,
            "selected_count": len(scene_selected_ids),
        },
        "relation_selection": {
            "selected_segment_ids": relation_selected_ids,
            "selected_count": len(relation_selected_ids),
        },
        "explicit_selection": {
            "selected_segment_ids": explicit_selected_ids,
            "selected_row_indexes": explicit_row_indexes,
            "selected_count": len(explicit_selected_ids),
        },
        "after_selected_apply": {
            "pending_count": after_selected_pending,
            "rows": after_selected_rows,
        },
        "final": {
            "pending_count": final_pending,
            "rows": final_rows,
        },
    }
    summary_path = output_dir / "bulk_review_ui_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(summary_path)
    window.close()
    app.processEvents()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
