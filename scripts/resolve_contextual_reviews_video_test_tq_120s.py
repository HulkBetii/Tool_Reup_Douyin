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
        "subtitle": "Sao vẫn chưa trả lương cho tôi vậy?",
        "tts": "Sao vẫn chưa trả lương cho tôi vậy?",
    },
    5: {
        "subtitle": "Thế thì cho thêm một cơ hội nữa.",
        "tts": "Thế thì cho thêm một cơ hội nữa nhé.",
    },
    6: {
        "subtitle": "Lần này hỏi câu dễ đến mức cái bàn cũng biết đáp.",
        "tts": "Lần này hỏi câu dễ đến mức cái bàn cũng biết đáp luôn.",
    },
    7: {
        "subtitle": "Đáp đúng thì thưởng một tràng pháo tay.",
        "tts": "Đáp đúng thì thưởng luôn một tràng pháo tay.",
    },
    9: {
        "subtitle": "Được!",
        "tts": "Được!",
        "confidence_json": {
            "overall": 0.78,
            "speaker": 0.74,
            "listener": 0.7,
            "register": 0.78,
            "relation": 0.72,
            "translation": 0.8,
        },
    },
    14: {
        "subtitle": "Cấp công chạy.",
        "tts": "Cấp công chạy.",
    },
    16: {
        "subtitle": "Lão nha thang.",
        "tts": "Lão nha thang.",
    },
    18: {
        "subtitle": "Tôi đã nói rồi, là ba chữ mà!",
        "tts": "Tôi đã nói rồi, là ba chữ mà!",
    },
    20: {
        "subtitle": "Đơn giản vậy mà vẫn không đoán ra à?",
        "tts": "Đơn giản vậy mà vẫn không đoán ra à?",
    },
    21: {
        "subtitle": "Chẳng lẽ còn ngu hơn cả heo nữa sao?",
        "tts": "Chẳng lẽ còn ngu hơn cả heo nữa sao?",
    },
    23: {
        "subtitle": "Thế thì đi rửa não lại đi.",
        "tts": "Thế thì đi rửa não lại đi.",
    },
    29: {
        "subtitle": "Tới lượt cậu rồi...",
        "tts": "Tới lượt cậu rồi...",
    },
    32: {
        "subtitle": "Tôi đã viết ba chữ ở mặt sau tờ giấy này.",
        "tts": "Tôi viết ba chữ ở mặt sau tờ giấy này nhé.",
    },
    42: {
        "subtitle": "Anh đổi đề bài rồi.",
        "tts": "Anh đổi đề bài rồi.",
    },
    44: {
        "subtitle": "Lần này tôi viết hẳn bốn chữ.",
        "tts": "Lần này tôi viết hẳn bốn chữ.",
    },
    48: {
        "subtitle": "Tôi đâu có lừa cậu.",
        "tts": "Tôi đâu có lừa cậu.",
    },
    49: {
        "subtitle": "Tôi viết đúng là ba chữ mà.",
        "tts": "Tôi viết đúng là ba chữ mà.",
    },
    50: {
        "subtitle": "Anh vừa mới nói rõ là bốn chữ mà.",
        "tts": "Anh vừa mới nói rõ là bốn chữ mà.",
    },
    52: {
        "subtitle": "Anh ngốc à?",
        "tts": "Anh ngốc à?",
    },
    53: {
        "subtitle": "Anh lại phát bệnh lú rồi à?",
        "tts": "Anh lại phát bệnh lú rồi à?",
    },
    55: {
        "subtitle": "Vậy sao anh lại viết liền một cụm ba chữ?",
        "tts": "Vậy sao anh lại viết liền một cụm ba chữ?",
    },
    56: {
        "subtitle": "Ý là một cụm 'bốn chữ'.",
        "tts": "Ý là một cụm 'bốn chữ'.",
    },
    57: {
        "subtitle": "Tôi nói chính cái cụm 'bốn chữ' ấy.",
        "tts": "Tôi nói chính cái cụm 'bốn chữ' ấy.",
    },
    58: {
        "subtitle": "Vì trong tiếng Trung, cụm đó vốn chỉ có ba chữ mà.",
        "tts": "Vì trong tiếng Trung, cụm đó vốn chỉ có ba chữ mà.",
    },
    61: {
        "subtitle": "Ông chủ, đi tiêm một mũi cho tỉnh đi.",
        "tts": "Ông chủ, đi tiêm một mũi cho tỉnh đi.",
    },
    64: {
        "subtitle": "Cái tôi gọi là 'bốn chữ' ấy, thật ra trong tiếng Trung chỉ có ba chữ thôi.",
        "tts": "Cái tôi gọi là 'bốn chữ' ấy, thật ra trong tiếng Trung chỉ có ba chữ thôi.",
    },
    65: {
        "subtitle": "À, ý là cái 'bốn chữ' đó hả?",
        "tts": "À, ý là cái 'bốn chữ' đó hả?",
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

        confidence_json = fixup.get("confidence_json")
        if confidence_json is not None:
            with database.connect() as connection:
                connection.execute(
                    """
                    UPDATE segment_analyses
                    SET confidence_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE project_id = ? AND segment_id = ?
                    """,
                    (json.dumps(confidence_json, ensure_ascii=False), project_id, str(row["segment_id"])),
                )

        touched_indexes.append(segment_index)
    return sorted(touched_indexes)


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve pending contextual review items for the 120s zh->vi sample.")
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
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Da xu ly {len(touched_indexes)} review item", flush=True)
    print(f"Summary: {summary_path}", flush=True)

    if pending_review_count != 0:
        raise RuntimeError(f"Van con {pending_review_count} review item")
    if semantic_qc["error_count"] != 0 or semantic_qc["warning_count"] != 0:
        raise RuntimeError(f"Semantic QC chua sach: {semantic_qc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
