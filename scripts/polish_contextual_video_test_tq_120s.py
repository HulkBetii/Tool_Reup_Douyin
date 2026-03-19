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


POLISH_FIXUPS: dict[int, dict[str, str]] = {
    0: {
        "subtitle": "Sao giờ vẫn chưa trả lương cho tôi?",
        "tts": "Sao giờ vẫn chưa trả lương cho tôi?",
    },
    1: {
        "subtitle": "Chưa qua bài kiểm tra thì lấy đâu ra lương?",
        "tts": "Chưa qua bài kiểm tra thì lấy đâu ra lương chứ?",
    },
    3: {
        "subtitle": "Rõ ràng đề của bạn khó quá mà!",
        "tts": "Rõ ràng đề của bạn khó quá còn gì!",
    },
    4: {
        "subtitle": "Đề của tôi thì khó thật đấy.",
        "tts": "Đề của tôi thì khó thật đấy.",
    },
    6: {
        "subtitle": "Lần này hỏi câu dễ đến mức cái bàn cũng đáp được.",
        "tts": "Lần này hỏi câu dễ đến mức cái bàn cũng đáp được luôn.",
    },
    7: {
        "subtitle": "Đáp đúng thì thưởng một tràng pháo tay.",
        "tts": "Đáp đúng thì thưởng luôn một tràng pháo tay.",
    },
    8: {
        "subtitle": "Thưởng gấp đôi.",
        "tts": "Thưởng gấp đôi luôn.",
    },
    11: {
        "subtitle": "Thế ba chữ đó là gì?",
        "tts": "Thế ba chữ đó là gì nào?",
    },
    17: {
        "subtitle": "Sao cậu chỉ nghĩ tới ăn thôi vậy?",
        "tts": "Sao cậu chỉ nghĩ tới ăn thôi vậy?",
    },
    19: {
        "subtitle": "Tớ chịu, đoán không ra.",
        "tts": "Tớ chịu, đoán không ra thật.",
    },
    21: {
        "subtitle": "Chẳng lẽ tôi còn ngu hơn cả heo à?",
        "tts": "Chẳng lẽ tôi còn ngu hơn cả heo à?",
    },
    22: {
        "subtitle": "Thì đúng thế còn gì.",
        "tts": "Thì đúng thế còn gì nữa.",
    },
    24: {
        "subtitle": "Tôi là số một!",
        "tts": "Tôi mới là số một!",
    },
    25: {
        "subtitle": "Đầu óc tôi siêu đỉnh!",
        "tts": "Đầu óc tôi siêu đỉnh luôn!",
    },
    26: {
        "subtitle": "Tôi là số một!",
        "tts": "Tôi mới là số một!",
    },
    27: {
        "subtitle": "Đầu óc tôi siêu đỉnh!",
        "tts": "Đầu óc tôi siêu đỉnh luôn!",
    },
    30: {
        "subtitle": "Tôi sẵn sàng rồi!",
        "tts": "Tôi sẵn sàng rồi!",
    },
    31: {
        "subtitle": "Sẵn sàng rồi thì làm tiếp đi!",
        "tts": "Sẵn sàng rồi thì làm tiếp đi!",
    },
    35: {
        "subtitle": "Sai rồi!",
        "tts": "Sai rồi!",
    },
    40: {
        "subtitle": "Sai rồi!",
        "tts": "Sai rồi!",
    },
    43: {
        "subtitle": "Tôi chỉ tăng độ khó lên chút thôi.",
        "tts": "Tôi chỉ tăng độ khó lên chút thôi mà.",
    },
    45: {
        "subtitle": "Sao anh chơi kỳ vậy?",
        "tts": "Sao anh chơi kỳ vậy chứ?",
    },
    46: {
        "subtitle": "Đã bảo ba chữ, sao lại viết bốn chữ?",
        "tts": "Đã bảo ba chữ, sao lại viết bốn chữ?",
    },
    47: {
        "subtitle": "Anh làm vậy khác gì trêu người ta chứ.",
        "tts": "Anh làm vậy khác gì trêu người ta chứ.",
    },
    51: {
        "subtitle": "Đúng đó!",
        "tts": "Đúng đó!",
    },
    53: {
        "subtitle": "Anh lại lên cơn lú rồi à?",
        "tts": "Anh lại lên cơn lú rồi à?",
    },
    54: {
        "subtitle": "Đâu có.",
        "tts": "Đâu có nhé.",
    },
    56: {
        "subtitle": "Ý tôi là cả cụm 'bốn chữ'.",
        "tts": "Ý tôi là cả cụm 'bốn chữ'.",
    },
    57: {
        "subtitle": "Tôi nói cái cụm 'bốn chữ' đó cơ.",
        "tts": "Tôi nói cái cụm 'bốn chữ' đó cơ.",
    },
    58: {
        "subtitle": "Vì trong tiếng Trung, cụm đó vốn chỉ có ba chữ mà.",
        "tts": "Vì trong tiếng Trung, cụm đó vốn chỉ có ba chữ mà.",
    },
    61: {
        "subtitle": "Ông chủ, đi tiêm một mũi cho tỉnh táo lại đi.",
        "tts": "Ông chủ, đi tiêm một mũi cho tỉnh táo lại đi.",
    },
}


def _apply_fixups(database: ProjectDatabase, project_id: str) -> list[int]:
    analyses_by_index = {
        int(row["segment_index"]): row for row in database.list_segment_analyses(project_id)
    }
    touched_indexes: list[int] = []
    for segment_index, fixup in POLISH_FIXUPS.items():
        row = analyses_by_index.get(segment_index)
        if row is None:
            raise RuntimeError(f"Khong tim thay segment analysis index={segment_index}")
        database.update_segment_analysis_review(
            project_id,
            str(row["segment_id"]),
            approved_subtitle_text=fixup["subtitle"],
            approved_tts_text=fixup["tts"],
            needs_human_review=False,
            review_status="approved",
            review_scope="line",
            review_reason_codes_json=[],
            review_question="",
            semantic_qc_issues_json=[],
        )
        touched_indexes.append(segment_index)
    return sorted(touched_indexes)


def main() -> int:
    parser = argparse.ArgumentParser(description="Polish natural Vietnamese dialogue for the 120s contextual sample.")
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
    summary_path = workspace.root_dir / "polish_contextual_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Da polish {len(touched_indexes)} dong", flush=True)
    print(f"Summary: {summary_path}", flush=True)

    if pending_review_count != 0:
        raise RuntimeError(f"Van con {pending_review_count} review item")
    if semantic_qc["error_count"] != 0 or semantic_qc["warning_count"] != 0:
        raise RuntimeError(f"Semantic QC chua sach: {semantic_qc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
