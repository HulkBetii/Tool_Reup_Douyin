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
    5: {
        "subtitle": "Không phải tôi làm điều gì tốt đẹp đâu.",
        "tts": "Không phải tôi làm điều gì tốt đẹp đâu.",
        "policy_json": {
            "policy_id": "neutral-reviewed",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.9,
        },
    },
    6: {
        "subtitle": "Ba cái dù này là sao vậy?",
        "tts": "Ba cái dù này là sao vậy?",
        "confidence_json": {
            "overall": 0.78,
            "speaker": 0.76,
            "listener": 0.74,
            "register": 0.77,
            "relation": 0.72,
            "translation": 0.8,
        },
    },
    7: {
        "subtitle": "Vì mưa suốt nên mới mua nhiều dù nhựa như vậy.",
        "tts": "Vì mưa suốt nên mới mua nhiều dù nhựa như vậy.",
    },
    8: {
        "subtitle": "Ô dù nhiều quá rồi đấy!",
        "tts": "Ô dù nhiều quá rồi đấy!",
    },
    9: {
        "subtitle": "Nhà mình có mấy người đâu mà!",
        "tts": "Nhà mình có mấy người đâu mà!",
    },
    10: {
        "subtitle": "Chẳng phải lỗi đều do bạn sao?",
        "tts": "Chẳng phải lỗi đều do bạn sao?",
    },
    11: {
        "subtitle": "Rõ ràng đang mùa mưa mà!",
        "tts": "Rõ ràng đang mùa mưa mà!",
    },
    12: {
        "subtitle": "Ra khỏi cửa còn chẳng mang ô!",
        "tts": "Ra khỏi cửa còn chẳng mang ô!",
    },
    13: {
        "subtitle": "Hễ mưa là lại mua một cái ô nhựa mới về.",
        "tts": "Hễ mưa là lại mua một cái ô nhựa mới về.",
    },
    14: {
        "subtitle": "Nhưng lúc tôi mang ô thì trời lại không mưa!",
        "tts": "Nhưng lúc tôi mang ô thì trời lại không mưa!",
    },
    15: {
        "subtitle": "Còn lúc tôi không mang thì trời lại mưa!",
        "tts": "Còn lúc tôi không mang thì trời lại mưa!",
    },
    16: {
        "subtitle": "Bạn nên dọn dẹp trong nhà đi.",
        "tts": "Bạn nên dọn dẹp trong nhà đi.",
    },
    17: {
        "subtitle": "Tôi không muốn đâu.",
        "tts": "Tôi không muốn đâu.",
    },
    18: {
        "subtitle": "Ô do bạn tự mua về thì phải tự dọn chứ.",
        "tts": "Ô do bạn tự mua về thì phải tự dọn chứ.",
    },
    19: {
        "subtitle": "Tôi còn phải giặt đồ và dọn dẹp nhà nữa mà.",
        "tts": "Tôi còn phải giặt đồ và dọn dẹp nhà nữa mà.",
    },
    20: {
        "subtitle": "Có nhiều việc phải tranh thủ lúc trời nắng mới làm được.",
        "tts": "Có nhiều việc phải tranh thủ lúc trời nắng mới làm được.",
    },
    25: {
        "subtitle": "Cái ô này hỏng rồi.",
        "tts": "Cái ô này hỏng rồi.",
    },
    26: {
        "subtitle": "Không thì gãy nan, không thì rách.",
        "tts": "Không thì gãy nan, không thì rách.",
    },
    27: {
        "subtitle": "Cái ô này còn giữ làm gì nữa?",
        "tts": "Cái ô này còn giữ làm gì nữa?",
    },
    29: {
        "subtitle": "Mẹ đúng là keo kiệt thật đấy.",
        "tts": "Mẹ đúng là keo kiệt thật đấy.",
    },
    31: {
        "subtitle": "Nhưng rốt cuộc là ai đã mua nhiều thế?",
        "tts": "Nhưng rốt cuộc là ai đã mua nhiều thế?",
    },
    32: {
        "subtitle": "Toàn là dù nhựa rẻ tiền dễ hỏng thôi mà.",
        "tts": "Toàn là dù nhựa rẻ tiền dễ hỏng thôi mà.",
    },
    33: {
        "subtitle": "Mẹ toàn chọn loại dù vừa rẻ vừa bền mà.",
        "tts": "Mẹ toàn chọn loại dù vừa rẻ vừa bền mà.",
    },
    34: {
        "subtitle": "Bền chỗ nào chứ?",
        "tts": "Bền chỗ nào chứ?",
    },
    35: {
        "subtitle": "Cái nào cũng hỏng hết rồi mà.",
        "tts": "Cái nào cũng hỏng hết rồi mà.",
    },
    36: {
        "subtitle": "Không phải vậy đâu.",
        "tts": "Không phải vậy đâu.",
        "policy_json": {
            "policy_id": "neutral-reviewed",
            "self_term": "",
            "address_term": "",
            "locked": False,
            "confidence": 0.9,
        },
    },
    37: {
        "subtitle": "Mỗi lần tôi mua được cái dù nhựa tốt,",
        "tts": "Mỗi lần tôi mua được cái dù nhựa tốt,",
    },
    38: {
        "subtitle": "Đôi khi tôi để cái dù ở đâu đó,",
        "tts": "Đôi khi tôi để cái dù ở đâu đó,",
    },
    39: {
        "subtitle": "Rồi cái dù đó lại biến mất.",
        "tts": "Rồi cái dù đó lại biến mất.",
    },
    40: {
        "subtitle": "Còn dù hỏng thì vẫn để nguyên chỗ cũ.",
        "tts": "Còn dù hỏng thì vẫn để nguyên chỗ cũ.",
    },
    41: {
        "subtitle": "Chắc chắn có người xấu bụng rồi.",
        "tts": "Chắc chắn có người xấu bụng rồi.",
    },
}

NEUTRAL_POLICY = {
    "policy_id": "neutral-reviewed",
    "self_term": "",
    "address_term": "",
    "locked": False,
    "confidence": 0.9,
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
            honorific_policy_json=fixup.get("policy_json", NEUTRAL_POLICY),
            approved_subtitle_text=str(fixup["subtitle"]),
            approved_tts_text=str(fixup["tts"]),
            needs_human_review=False,
            review_status="approved",
            review_scope="line",
            review_reason_codes_json=[],
            review_question="",
            semantic_qc_passed=True,
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
    parser = argparse.ArgumentParser(description="Resolve pending contextual review items for the 120s Shinchan zh->vi sample.")
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
    if semantic_qc["error_count"] != 0:
        raise RuntimeError(f"Semantic QC con loi: {semantic_qc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
