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

ANH_TOI_POLICY = {
    "policy_id": "review:toi-anh",
    "self_term": "tôi",
    "address_term": "anh",
    "locked": False,
    "confidence": 0.85,
}


REVIEW_FIXUPS: dict[int, dict[str, object]] = {
    4: {
        "subtitle": "Ở đây còn có cả những con cá khổng lồ nặng hàng trăm cân.",
        "tts": "Ở đây còn có cả những con cá khổng lồ nặng hàng trăm cân.",
    },
    5: {
        "subtitle": "Cả những con cá sấu lớn ở Amazon nữa.",
        "tts": "Cả những con cá sấu lớn ở Amazon nữa.",
    },
    16: {
        "subtitle": "Có lúc tôi còn bị dị ứng khắp người.",
        "tts": "Có lúc tôi còn bị dị ứng khắp người.",
    },
    17: {
        "subtitle": "Thậm chí còn nổi mẩn khắp người.",
        "tts": "Thậm chí còn nổi mẩn khắp người.",
    },
    18: {
        "subtitle": "Thậm chí còn bị nhiễm bệnh.",
        "tts": "Thậm chí còn bị nhiễm bệnh.",
    },
    22: {
        "subtitle": "Mưa lớn kéo dài sẽ khiến khu Rain Ridge bị ngập.",
        "tts": "Mưa lớn kéo dài sẽ khiến khu Rain Ridge bị ngập.",
    },
    29: {
        "subtitle": "Tôi và người bạn đồng hành đã tháo hết các bẫy đã đặt.",
        "tts": "Tôi và người bạn đồng hành đã tháo hết các bẫy đã đặt.",
    },
    30: {
        "subtitle": "Cố gắng trả mọi thứ về gần với tự nhiên nhất.",
        "tts": "Cố gắng trả mọi thứ về gần với tự nhiên nhất.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    32: {
        "subtitle": "Để lại cho mấy con vật nhỏ trong rừng.",
        "tts": "Để lại cho mấy con vật nhỏ trong rừng.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    36: {
        "subtitle": "Ông Quan cũng tới rồi.",
        "tts": "Ông Quan cũng tới rồi.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    37: {
        "subtitle": "Đi thôi, lên thuyền nào.",
        "tts": "Đi thôi, lên thuyền nào.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    38: {
        "subtitle": "Chúng tôi đi dưới sự dẫn dắt của ông Quan.",
        "tts": "Chúng tôi đi dưới sự dẫn dắt của ông Quan.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    39: {
        "subtitle": "Chúng tôi đi thuyền dọc sông hơn sáu tiếng.",
        "tts": "Chúng tôi đi thuyền dọc sông hơn sáu tiếng.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    40: {
        "subtitle": "Rồi tới một bản làng của người bản địa.",
        "tts": "Rồi tới một bản làng của người bản địa.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    41: {
        "subtitle": "Hiện còn cách Thủ Đông vài trăm cây số.",
        "tts": "Hiện còn cách Thủ Đông vài trăm cây số.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    42: {
        "subtitle": "Tối nay chúng tôi sẽ nghỉ lại nhà ông Quan.",
        "tts": "Tối nay chúng tôi sẽ nghỉ lại nhà ông Quan.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    52: {
        "subtitle": "Lúc nãy tôi cũng không rõ nữa.",
        "tts": "Lúc nãy tôi cũng không rõ nữa.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    59: {
        "subtitle": "Anh yêu tôi.",
        "tts": "Anh yêu tôi.",
        "honorific_policy_json": ANH_TOI_POLICY,
    },
    60: {
        "subtitle": "Anh làm công việc này à?",
        "tts": "Anh làm công việc này à?",
        "honorific_policy_json": ANH_TOI_POLICY,
    },
    61: {
        "subtitle": "Tôi làm giáo viên.",
        "tts": "Tôi làm giáo viên.",
        "honorific_policy_json": ANH_TOI_POLICY,
    },
    62: {
        "subtitle": "Anh sẽ đưa tôi sang Trung Quốc chứ?",
        "tts": "Anh sẽ đưa tôi sang Trung Quốc chứ?",
        "honorific_policy_json": ANH_TOI_POLICY,
    },
    63: {
        "subtitle": "Đúng vậy.",
        "tts": "Đúng vậy.",
        "honorific_policy_json": ANH_TOI_POLICY,
    },
    67: {
        "subtitle": "Nếu được thì tốt quá.",
        "tts": "Nếu được thì tốt quá.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    68: {
        "subtitle": "Nếu hợp thì cứ tìm hiểu thêm đi.",
        "tts": "Nếu hợp thì cứ tìm hiểu thêm đi.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    69: {
        "subtitle": "Được.",
        "tts": "Được.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    70: {
        "subtitle": "Suriname nằm ở Nam Mỹ.",
        "tts": "Suriname nằm ở Nam Mỹ.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    71: {
        "subtitle": "Phong tục ở đó khá mộc mạc và chân chất.",
        "tts": "Phong tục ở đó khá mộc mạc và chân chất.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    72: {
        "subtitle": "Nhưng đây cũng là một nước có mức sống khá đắt đỏ.",
        "tts": "Nhưng đây cũng là một nước có mức sống khá đắt đỏ.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    73: {
        "subtitle": "Người dân trong làng ở đây",
        "tts": "Người dân trong làng ở đây",
        "honorific_policy_json": EMPTY_POLICY,
    },
    74: {
        "subtitle": "chủ yếu sống nhờ nguồn thu nhập đó.",
        "tts": "chủ yếu sống nhờ nguồn thu nhập đó.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    75: {
        "subtitle": "và một lối sống khá giản dị.",
        "tts": "và một lối sống khá giản dị.",
        "honorific_policy_json": EMPTY_POLICY,
    },
    76: {
        "subtitle": "Nhìn chung, cuộc sống ở đây khá mộc mạc.",
        "tts": "Nhìn chung, cuộc sống ở đây khá mộc mạc.",
        "honorific_policy_json": EMPTY_POLICY,
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
        description="Resolve pending contextual review items for the Amazon rainforest 120s zh->vi sample."
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
