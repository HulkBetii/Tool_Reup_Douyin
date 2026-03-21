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


REVIEW_FIXUPS: dict[int, dict[str, object]] = {
    0: {
        "subtitle": "Ơ, sao lại thế này?",
        "tts": "Ơ, sao lại thế này?",
    },
    4: {
        "subtitle": "Cua ở đây chỉ gói gọn trong một chữ.",
        "tts": "Cua ở đây chỉ gói gọn trong một chữ.",
    },
    6: {
        "subtitle": "Đúng là cả một kho đồ ăn.",
        "tts": "Đúng là cả một kho đồ ăn.",
    },
    9: {
        "subtitle": "Ngày nào cũng có cua ăn không hết.",
        "tts": "Ngày nào cũng có cua ăn không hết.",
    },
    10: {
        "subtitle": "Tôi ăn cua mấy ngày liền rồi.",
        "tts": "Tôi ăn cua mấy ngày liền rồi.",
    },
    11: {
        "subtitle": "Hôm nay tôi ngán cua rồi.",
        "tts": "Hôm nay tôi ngán cua rồi.",
    },
    12: {
        "subtitle": "Món ngon mà ăn nhiều cũng ngán.",
        "tts": "Món ngon mà ăn nhiều cũng ngán.",
    },
    13: {
        "subtitle": "Nên hôm nay tôi đổi sang ăn cá.",
        "tts": "Nên hôm nay tôi đổi sang ăn cá.",
    },
    14: {
        "subtitle": "Lấy cua làm mồi câu cá.",
        "tts": "Lấy cua làm mồi câu cá.",
    },
    15: {
        "subtitle": "Hôm nay tôi muốn câu con cá hơn 5 cân.",
        "tts": "Hôm nay tôi muốn câu con cá hơn 5 cân.",
    },
    16: {
        "subtitle": "Xa xa trời đầy mây đen.",
        "tts": "Xa xa trời đầy mây đen.",
    },
    20: {
        "subtitle": "Là một con cá nhỏ.",
        "tts": "Là một con cá nhỏ.",
    },
    22: {
        "subtitle": "Hy vọng lần này câu được con to hơn.",
        "tts": "Hy vọng lần này câu được con to hơn.",
    },
    27: {
        "subtitle": "Giờ tôi chỉ còn con cá này để làm mồi câu.",
        "tts": "Giờ tôi chỉ còn con cá này để làm mồi câu.",
    },
    28: {
        "subtitle": "Không chịu dùng thịt cá làm mồi thì không câu được cá.",
        "tts": "Không chịu dùng thịt cá làm mồi thì không câu được cá.",
    },
    29: {
        "subtitle": "Dù đây là con cá duy nhất tôi có,",
        "tts": "Dù đây là con cá duy nhất tôi có,",
    },
    30: {
        "subtitle": "nhưng tôi hy vọng dùng nó",
        "tts": "nhưng tôi hy vọng dùng nó",
    },
    31: {
        "subtitle": "để câu được con cá lớn hơn.",
        "tts": "để câu được con cá lớn hơn.",
        "honorific_policy_json": {
            "policy_id": "",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.0,
        },
    },
    32: {
        "subtitle": "Sau đó tôi lại hụt mấy lần liền.",
        "tts": "Sau đó tôi lại hụt mấy lần liền.",
        "honorific_policy_json": {
            "policy_id": "",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.0,
        },
    },
    33: {
        "subtitle": "Nhưng phao câu vẫn không nhúc nhích.",
        "tts": "Nhưng phao câu vẫn không nhúc nhích.",
        "honorific_policy_json": {
            "policy_id": "",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.0,
        },
    },
    34: {
        "subtitle": "Giờ câu cá chắc hết hy vọng rồi.",
        "tts": "Giờ câu cá chắc hết hy vọng rồi.",
        "honorific_policy_json": {
            "policy_id": "",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.0,
        },
    },
    35: {
        "subtitle": "Tôi phải kiếm món khác để ăn thôi.",
        "tts": "Tôi phải kiếm món khác để ăn thôi.",
        "honorific_policy_json": {
            "policy_id": "",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.0,
        },
    },
    36: {
        "subtitle": "Cứ vào rừng xem thử trước đã.",
        "tts": "Cứ vào rừng xem thử trước đã.",
    },
    38: {
        "subtitle": "Chỗ trước kia bắt cua giờ chẳng còn con nào.",
        "tts": "Chỗ trước kia bắt cua giờ chẳng còn con nào.",
    },
    39: {
        "subtitle": "Cuộc sống trên đảo là vậy đấy.",
        "tts": "Cuộc sống trên đảo là vậy đấy.",
    },
    41: {
        "subtitle": "Ở ngoài hoang dã,",
        "tts": "Ở ngoài hoang dã,",
    },
    42: {
        "subtitle": "tôi biết rõ một điều:",
        "tts": "tôi biết rõ một điều:",
    },
    43: {
        "subtitle": "thứ gì chưa chắc chắn",
        "tts": "thứ gì chưa chắc chắn",
    },
    44: {
        "subtitle": "thì đừng động vào.",
        "tts": "thì đừng động vào.",
    },
    47: {
        "subtitle": "Có ai giống tôi không,",
        "tts": "Có ai giống tôi không,",
    },
    48: {
        "subtitle": "vừa nhìn cái là nhận ra ngay?",
        "tts": "vừa nhìn cái là nhận ra ngay?",
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
        description="Resolve pending contextual review items for the Indian Ocean 120s zh->vi sample."
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
