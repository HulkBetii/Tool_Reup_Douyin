from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.project.bootstrap import open_project, sync_project_snapshot
from app.project.database import ProjectDatabase
from app.translate.contextual_pipeline import recompute_semantic_qc

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


FIT_FIXUPS: dict[int, str] = {
    0: "Cho đến nay, lòng đất vẫn đóng kín với con người.",
    1: "Nhiều người nghĩ lòng đất ở rất gần, nhưng thực tế lại ngược hẳn.",
    2: "Ngày nay, tàu thăm dò của con người đã bay xa hàng tỷ cây số.",
    5: "Dù là vực Challenger ở Mariana hay các mũi khoan sâu nhất,",
    6: "Dường như có một rào cản ở mốc 12.000 mét dưới lòng đất.",
    8: "Xin chào, GUS đây.",
    10: "Đường còn dài, nhớ bấm thích nhé. Sẵn sàng chưa? Bắt đầu thôi.",
    17: "Liệu con người có thể khoan sâu vào Trái Đất, thậm chí chạm tới lớp phủ hay không?",
    20: "Năm 1909, Andrija Mohorovičić phát hiện ranh giới này qua tốc độ sóng địa chấn.",
    21: "Vì vậy ranh giới này mang tên ông.",
    22: "Tên Mohole ghép từ Moho và hole, thể hiện quyết tâm khoan tới ranh giới Moho.",
    23: "Dự án được Quỹ Khoa học Quốc gia Mỹ hỗ trợ, và đã khoan xuống 183 mét dưới đáy biển.",
    24: "Nhưng độ khó tăng, chi phí đội lên, lại thêm tranh chấp chính trị, nên dự án dần bế tắc.",
}


def _apply_fit_fixups(database: ProjectDatabase, project_id: str) -> list[int]:
    analyses_by_index = {
        int(row["segment_index"]): row for row in database.list_segment_analyses(project_id)
    }
    touched: list[int] = []
    for segment_index, text in FIT_FIXUPS.items():
        row = analyses_by_index.get(segment_index)
        if row is None:
            raise RuntimeError(f"Khong tim thay segment analysis index={segment_index}")
        database.update_segment_analysis_review(
            project_id,
            str(row["segment_id"]),
            approved_subtitle_text=text,
            approved_tts_text=text,
            needs_human_review=False,
            review_status="approved",
            review_scope="line",
            review_reason_codes_json=[],
            review_question="",
            semantic_qc_issues_json=[],
        )
        touched.append(segment_index)
    return sorted(touched)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Shorten high slot-pressure lines for the Earth-depth 120s sample."
    )
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--target-language", default="vi")
    args = parser.parse_args()

    workspace = open_project(args.project_root.expanduser().resolve())
    database = ProjectDatabase(workspace.database_path)

    touched = _apply_fit_fixups(database, workspace.project_id)
    semantic_qc = recompute_semantic_qc(
        database,
        project_id=workspace.project_id,
        target_language=args.target_language,
    )
    pending_review_count = database.count_pending_segment_reviews(workspace.project_id)
    sync_project_snapshot(workspace)

    summary = {
        "project_root": str(workspace.root_dir),
        "touched_segment_indexes": touched,
        "pending_review_count": pending_review_count,
        "semantic_qc": semantic_qc,
    }
    summary_path = workspace.root_dir / "polish_fit_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
