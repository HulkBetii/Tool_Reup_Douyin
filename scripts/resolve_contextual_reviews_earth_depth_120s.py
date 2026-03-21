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


EMPTY_POLICY = {
    "policy_id": "",
    "self_term": "",
    "address_term": "",
    "locked": False,
    "confidence": 0.0,
}


REVIEW_FIXUPS: dict[int, dict[str, object]] = {
    1: {
        "subtitle": "Nhiều người nghĩ bên trong Trái Đất ở rất gần, nhưng thực tế lại ngược hẳn.",
        "tts": "Nhiều người nghĩ bên trong Trái Đất ở rất gần, nhưng thực tế lại ngược hẳn.",
    },
    3: {
        "subtitle": "Nhưng hiểu biết của ta về lòng đất vẫn rất ít.",
        "tts": "Nhưng hiểu biết của ta về lòng đất vẫn rất ít.",
    },
    5: {
        "subtitle": "Dù là vực Challenger ở Mariana hay các mũi khoan sâu nhất của con người,",
        "tts": "Dù là vực Challenger ở Mariana hay các mũi khoan sâu nhất của con người,",
    },
    7: {
        "subtitle": "Còn dưới độ sâu ấy, những gì ta biết lại càng ít.",
        "tts": "Còn dưới độ sâu ấy, những gì ta biết lại càng ít.",
    },
    8: {
        "subtitle": "Xin chào, đây là kênh GUS.",
        "tts": "Xin chào, đây là kênh GUS.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    9: {
        "subtitle": "Lần này, hãy cùng xem điều gì đang ngăn con người khoan sâu hơn.",
        "tts": "Lần này, hãy cùng xem điều gì đang ngăn con người khoan sâu hơn.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    10: {
        "subtitle": "Đường còn dài, nhớ bấm thích nhé. Sẵn sàng chưa? Bắt đầu thôi.",
        "tts": "Đường còn dài, nhớ bấm thích nhé. Sẵn sàng chưa? Bắt đầu thôi.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    11: {
        "subtitle": "Lần khoan sâu nhất trong lịch sử nhân loại cũng chỉ tới khoảng 12.000 mét.",
        "tts": "Lần khoan sâu nhất trong lịch sử nhân loại cũng chỉ tới khoảng 12.000 mét.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    12: {
        "subtitle": "Nghe thì lớn, nhưng so với Trái Đất thì gần như không đáng kể.",
        "tts": "Nghe thì lớn, nhưng so với Trái Đất thì gần như không đáng kể.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    13: {
        "subtitle": "Cuộc khoan sâu vào Trái Đất bắt đầu từ cuộc đua công nghệ thời Chiến tranh Lạnh.",
        "tts": "Cuộc khoan sâu vào Trái Đất bắt đầu từ cuộc đua công nghệ thời Chiến tranh Lạnh.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    14: {
        "subtitle": "Thập niên 1960, Mỹ và Liên Xô lao vào cuộc đua không gian.",
        "tts": "Thập niên 1960, Mỹ và Liên Xô lao vào cuộc đua không gian.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    15: {
        "subtitle": "Hai bên thi nhau đưa con người vào quỹ đạo rồi lên Mặt Trăng.",
        "tts": "Hai bên thi nhau đưa con người vào quỹ đạo rồi lên Mặt Trăng.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    16: {
        "subtitle": "Nhưng cùng lúc đó, một cuộc đua khoan xuống lòng đất cũng bắt đầu.",
        "tts": "Nhưng cùng lúc đó, một cuộc đua khoan xuống lòng đất cũng bắt đầu.",
    },
    24: {
        "subtitle": "Nhưng càng làm càng khó, kinh phí đội lên, thêm cả tranh chấp chính trị, nên dự án dần bế tắc.",
        "tts": "Nhưng càng làm càng khó, kinh phí đội lên, thêm cả tranh chấp chính trị, nên dự án dần bế tắc.",
    },
}


def _apply_fixups(database: ProjectDatabase, project_id: str) -> list[int]:
    analyses_by_index = {
        int(row["segment_index"]): row for row in database.list_segment_analyses(project_id)
    }
    touched_indexes: list[int] = []
    for segment_index, fixup in REVIEW_FIXUPS.items():
        row = analyses_by_index.get(segment_index)
        if row is None:
            raise RuntimeError(f"Khong tim thay segment analysis index={segment_index}")

        database.update_segment_analysis_review(
            project_id,
            str(row["segment_id"]),
            approved_subtitle_text=str(fixup["subtitle"]),
            approved_tts_text=str(fixup["tts"]),
            needs_human_review=False,
            review_status="approved",
            review_scope="line",
            review_reason_codes_json=[],
            review_question="",
            semantic_qc_issues_json=[],
        )

        honorific_policy_json = fixup.get("honorific_policy_json")
        if honorific_policy_json is not None:
            with database.connect() as connection:
                connection.execute(
                    """
                    UPDATE segment_analyses
                    SET honorific_policy_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (
                        json.dumps(honorific_policy_json, ensure_ascii=False),
                        project_id,
                        str(row["segment_id"]),
                    ),
                )

        touched_indexes.append(segment_index)
    return sorted(touched_indexes)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve pending contextual review items for the Earth-depth 120s zh->vi sample."
    )
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--target-language", default="vi")
    args = parser.parse_args()

    workspace = open_project(args.project_root.expanduser().resolve())
    database = ProjectDatabase(workspace.database_path)

    touched_indexes = _apply_fixups(database, workspace.project_id)
    semantic_qc = recompute_semantic_qc(
        database,
        project_id=workspace.project_id,
        target_language=args.target_language,
    )
    pending_review_count = database.count_pending_segment_reviews(workspace.project_id)
    sync_project_snapshot(workspace)

    summary = {
        "project_root": str(workspace.root_dir),
        "touched_segment_indexes": touched_indexes,
        "pending_review_count": pending_review_count,
        "semantic_qc": semantic_qc,
    }
    summary_path = workspace.root_dir / "resolve_contextual_reviews_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
