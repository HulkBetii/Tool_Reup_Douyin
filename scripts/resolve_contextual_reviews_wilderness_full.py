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
        "subtitle": "Chưa từng thấy người đàn ông nào ghê gớm như Vương Đức Phát.",
        "tts": "Chưa từng thấy người đàn ông nào ghê gớm như Vương Đức Phát.",
    },
    6: {
        "subtitle": "Anh ấy chỉ muốn ra vùng hoang dã dựng một mái nhà.",
        "tts": "Anh ấy chỉ muốn ra vùng hoang dã dựng một mái nhà.",
    },
    7: {
        "subtitle": "Anh ấy tìm thấy một gò đất hoàng thổ hiếm gặp.",
        "tts": "Anh ấy tìm thấy một gò đất hoàng thổ hiếm gặp.",
    },
    9: {
        "subtitle": "Anh quyết không ở kiểu nhà xây sẵn nữa.",
        "tts": "Anh quyết không ở kiểu nhà xây sẵn nữa.",
    },
    24: {
        "subtitle": "Rồi anh ấy tự chế luôn một bộ cáp kéo đất.",
        "tts": "Rồi anh ấy tự chế luôn một bộ cáp kéo đất.",
    },
    26: {
        "subtitle": "Sau đó lắp cái xe trượt lên đường ray.",
        "tts": "Sau đó lắp cái xe trượt lên đường ray.",
    },
    28: {
        "subtitle": "Dù có một người ngồi lên cái xe trượt đó cũng không sao.",
        "tts": "Dù có một người ngồi lên cái xe trượt đó cũng không sao.",
    },
    30: {
        "subtitle": "Nhờ bộ cáp kéo này mà anh ấy đỡ tốn sức hơn nhiều.",
        "tts": "Nhờ bộ cáp kéo này mà anh ấy đỡ tốn sức hơn nhiều.",
    },
    31: {
        "subtitle": "Nhưng anh ấy làm mãi không biết dừng.",
        "tts": "Nhưng anh ấy làm mãi không biết dừng.",
    },
    32: {
        "subtitle": "Trông cứ như một cỗ máy không biết mệt.",
        "tts": "Trông cứ như một cỗ máy không biết mệt.",
    },
    33: {
        "subtitle": "Cứ lặp đi lặp lại đúng một động tác.",
        "tts": "Cứ lặp đi lặp lại đúng một động tác.",
    },
    34: {
        "subtitle": "Cây xẻng trong tay anh ấy vung lên vun vút.",
        "tts": "Cây xẻng trong tay anh ấy vung lên vun vút.",
    },
    37: {
        "subtitle": "Còn phần bố trí bên trong thì Vương Đức Phát tự tay lo hết.",
        "tts": "Còn phần bố trí bên trong thì Vương Đức Phát tự tay lo hết.",
    },
    38: {
        "subtitle": "Đúng là đôi tay khéo léo cân được mọi thứ.",
        "tts": "Đúng là đôi tay khéo léo cân được mọi thứ.",
        "confidence_json": {
            "overall": 0.78,
            "speaker": 0.78,
            "listener": 0.78,
            "register": 0.78,
            "relation": 0.74,
            "translation": 0.8,
        },
    },
    39: {
        "subtitle": "Anh ấy tiếp tục trang trí cho toàn bộ không gian bên trong.",
        "tts": "Anh ấy tiếp tục trang trí cho toàn bộ không gian bên trong.",
    },
    44: {
        "subtitle": "Còn chuyện nhóm bếp thì anh ấy rành khỏi nói.",
        "tts": "Còn chuyện nhóm bếp thì anh ấy rành khỏi nói.",
    },
    49: {
        "subtitle": "Nhìn vào độ sâu này là biết ngay.",
        "tts": "Nhìn vào độ sâu này là biết ngay.",
    },
    50: {
        "subtitle": "Chỗ này ít nhất cũng cách mặt đất hơn ba mét.",
        "tts": "Chỗ này ít nhất cũng cách mặt đất hơn ba mét.",
    },
    51: {
        "subtitle": "Cả công trình được làm rất vuông vắn, gọn ghẽ.",
        "tts": "Cả công trình được làm rất vuông vắn, gọn ghẽ.",
    },
    52: {
        "subtitle": "Nhìn từ góc nào cũng thấy các cạnh rất gọn.",
        "tts": "Nhìn từ góc nào cũng thấy các cạnh rất gọn.",
    },
    53: {
        "subtitle": "Anh ấy vẫn làm một mạch, không hề nghỉ tay.",
        "tts": "Anh ấy vẫn làm một mạch, không hề nghỉ tay.",
    },
    54: {
        "subtitle": "Rồi tiếp tục đào thông thêm rãnh dẫn lửa ở bên cạnh.",
        "tts": "Rồi tiếp tục đào thông thêm rãnh dẫn lửa ở bên cạnh.",
    },
    55: {
        "subtitle": "Để hơi nóng lan đều hơn.",
        "tts": "Để hơi nóng lan đều hơn.",
    },
    57: {
        "subtitle": "Rãnh này được nối thẳng sang bệ đất bên cạnh.",
        "tts": "Rãnh này được nối thẳng sang bệ đất bên cạnh.",
    },
    61: {
        "subtitle": "Sau khi thông hết toàn bộ các rãnh,",
        "tts": "Sau khi thông hết toàn bộ các rãnh,",
    },
    62: {
        "subtitle": "Anh ấy dùng thước cuộn đo chiều dài của rãnh lớn.",
        "tts": "Anh ấy dùng thước cuộn đo chiều dài của rãnh lớn.",
    },
    65: {
        "subtitle": "Và mỗi thanh tre đều phải khoan lỗ ở một đầu.",
        "tts": "Và mỗi thanh tre đều phải khoan lỗ ở một đầu.",
    },
    66: {
        "subtitle": "Rồi đặt các đoạn tre đã ghép vào rãnh chừa sẵn.",
        "tts": "Rồi đặt các đoạn tre đã ghép vào rãnh chừa sẵn.",
    },
    68: {
        "subtitle": "Nhưng trước khi lấp đất, vẫn phải chèn thêm chốt gỗ.",
        "tts": "Nhưng trước khi lấp đất, vẫn phải chèn thêm chốt gỗ.",
    },
    69: {
        "subtitle": "Sau đó bắt đầu phủ kín phần bệ giường.",
        "tts": "Sau đó bắt đầu phủ kín phần bệ giường.",
    },
    70: {
        "subtitle": "Khi bề mặt bệ giường đã được san cho thật phẳng,",
        "tts": "Khi bề mặt bệ giường đã được san cho thật phẳng,",
    },
    71: {
        "subtitle": "Thì có thể đặt hệ ống dẫn nhiệt xuống.",
        "tts": "Thì có thể đặt hệ ống dẫn nhiệt xuống.",
    },
    75: {
        "subtitle": "Như mọi khi, anh ấy hong khô bếp lò trước đã.",
        "tts": "Như mọi khi, anh ấy hong khô bếp lò trước đã.",
    },
    83: {
        "subtitle": "Ngủ một giấc là tỉnh cả người.",
        "tts": "Ngủ một giấc là tỉnh cả người.",
    },
    85: {
        "subtitle": "Vương Đức Phát lại ra ngoài kiếm một thân cây đổ ngang.",
        "tts": "Vương Đức Phát lại ra ngoài kiếm một thân cây đổ ngang.",
    },
    86: {
        "subtitle": "Không nói không rằng, anh ấy bắt tay vào đẽo gọt luôn.",
        "tts": "Không nói không rằng, anh ấy bắt tay vào đẽo gọt luôn.",
    },
    88: {
        "subtitle": "Một khúc làm chân đỡ thì vừa khít.",
        "tts": "Một khúc làm chân đỡ thì vừa khít.",
    },
    90: {
        "subtitle": "Rồi dùng chúng để làm một món đồ gỗ mộc mạc ngoài trời.",
        "tts": "Rồi dùng chúng để làm một món đồ gỗ mộc mạc ngoài trời.",
    },
    91: {
        "subtitle": "Tiếp đó anh ấy san mặt đất cho thật bằng phẳng.",
        "tts": "Tiếp đó anh ấy san mặt đất cho thật bằng phẳng.",
    },
    92: {
        "subtitle": "Tiện tay bỏ thêm ít củi khô vào bếp lò.",
        "tts": "Tiện tay bỏ thêm ít củi khô vào bếp lò.",
    },
    101: {
        "subtitle": "Cuộc sống kiểu này đúng là êm ru.",
        "tts": "Cuộc sống kiểu này đúng là êm ru.",
    },
    102: {
        "subtitle": "Rồi đắp chiếc chăn cũ đã theo mình bao năm.",
        "tts": "Rồi đắp chiếc chăn cũ đã theo mình bao năm.",
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
    parser = argparse.ArgumentParser(description="Resolve pending contextual review items for the wilderness full zh->vi sample.")
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
