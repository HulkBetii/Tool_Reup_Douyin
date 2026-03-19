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


POLISH_FIXUPS: dict[int, str] = {
    1: "Chỉ trong ba ngày, anh ấy đã khoét rỗng cả một quả đồi lớn.",
    2: "Rồi biến nó thành một căn nhà ngầm cực kỳ chỉn chu.",
    3: "Cả căn còn được lắp hẳn một hệ thống sưởi nền.",
    4: "Ngủ một giấc ở đây chắc phải sướng lắm.",
    5: "Vương Đức Phát chẳng thích kiểu sống gò bó.",
    6: "Anh ấy chỉ muốn ra vùng hoang dã tự dựng cho mình một mái nhà.",
    8: "Đất chỗ này vừa chắc, vừa hợp để đào hầm ở.",
    9: "Anh quyết không ở kiểu nhà làm sẵn nữa.",
    10: "Anh quyết dựng cho mình một chỗ trú nhỏ ngay tại đây.",
    11: "Không nói nhiều, Vương Đức Phát bắt tay đào ngay.",
    12: "Tay đào của anh ấy khỏe và gọn, nhìn là biết có nghề.",
    13: "Lớp đất ở đây vừa tay: không quá cứng, cũng không quá mềm.",
    14: "Nhờ vậy mà đào lên khá thuận.",
    16: "Chẳng mấy chốc, một hố sâu đã hiện ra rõ rệt.",
    17: "Anh dùng que gỗ luồn một sợi dây mảnh qua.",
    18: "Dưới tay anh ấy, cây xẻng đi đúng từng đường.",
    19: "Cây xẻng trong tay anh ấy cứ như có mắt.",
    20: "Mỗi nhát xúc đều gọn và chuẩn xác.",
    21: "Chẳng mấy chốc, một mặt bàn gọn gàng đã thành hình.",
    22: "Hố đào lúc này đã sâu quá mười mét.",
    23: "Nên phải tính cách chuyển đất cho đỡ vất vả hơn.",
    25: "Trước tiên phải đóng chốt gỗ để cố định đường ray kéo đất.",
    27: "Nhìn cả bộ ray vận hành trơn tru hẳn.",
    29: "Bộ kéo đất này đúng kiểu nửa cơ khí, nửa thủ công.",
    30: "Nhờ bộ kéo này mà anh ấy đỡ tốn sức hơn hẳn.",
    35: "Chẳng mấy chốc, một bệ giường hình vòng cung đã thành hình.",
    36: "Cả chỗ đặt nến cũng được chừa ra gọn gàng.",
    38: "Đúng là đôi tay khéo, đụng vào đâu cũng ra việc.",
    39: "Anh lại tiếp tục hoàn thiện phần bên trong.",
    40: "Đặt thêm vài ngọn nến vào là không gian ấm lên hẳn.",
    41: "Nhìn còn ra dáng ấm cúng, lại có chút cầu kỳ.",
    42: "Thế là thành một căn hầm vừa rộng vừa ra gì.",
    43: "Sống ở đây thì dĩ nhiên cũng không thể thiếu một cái bếp.",
    45: "Làm đến đâu ra dáng đến đó, đúng kiểu người có nghề.",
    46: "Nhìn cách anh ấy làm là biết tay nghề cứng cỡ nào.",
    48: "Chẳng mấy chốc, anh đã đào thông lối lên mặt đất.",
    56: "Các rãnh được chừa vừa đẹp, không rộng cũng không hẹp.",
    59: "không khó để đoán anh ấy đã quen tay với mấy việc kiểu này.",
    60: "Không có kinh nghiệm thật thì khó mà làm gọn đến mức này.",
    63: "Sau đó anh đi kiếm mấy bụi tre cho vừa việc.",
    64: "Rồi cắt ghép các ống tre theo đúng chiều dài cần dùng.",
    73: "Phủ kín toàn bộ hệ ống dẫn nhiệt bên dưới.",
    74: "Vương Đức Phát mang củi khô lại.",
    77: "Từng lỗ thông hơi bắt đầu nhả ra hơi nóng.",
    80: "Phải công nhận, kiểu đàn ông làm việc đến nơi đến chốn thế này hiếm thật.",
    81: "Giữa chốn hoang dã mà thiếu một người bầu bạn thì cũng hơi tiếc.",
    82: "Chẳng mấy chốc, một tấm chiếu tre đã xong.",
    84: "Vương Đức Phát nghỉ chưa ấm chỗ đã lại đứng dậy.",
    90: "Rồi ghép chúng lại thành một món đồ gỗ mộc mạc đặt ngoài trời.",
    98: "Vương Đức Phát ngồi xuống thưởng thức thành quả.",
    99: "Nhìn anh ấy ăn thôi cũng biết bữa này ngon thật.",
    103: "Cả căn phòng dần ấm lên nhờ hệ thống sưởi nền.",
    104: "Vương Đức Phát ngủ một giấc yên bình.",
}


def _apply_fixups(database: ProjectDatabase, project_id: str) -> list[int]:
    analyses_by_index = {
        int(row["segment_index"]): row for row in database.list_segment_analyses(project_id)
    }
    touched_indexes: list[int] = []
    for segment_index, polished_text in sorted(POLISH_FIXUPS.items()):
        row = analyses_by_index.get(segment_index)
        if row is None:
            raise RuntimeError(f"Khong tim thay segment analysis index={segment_index}")
        database.update_segment_analysis_review(
            project_id,
            str(row["segment_id"]),
            approved_subtitle_text=polished_text,
            approved_tts_text=polished_text,
            needs_human_review=False,
            review_status="approved",
            review_scope="line",
            review_reason_codes_json=[],
            review_question="",
            semantic_qc_issues_json=[],
        )
        touched_indexes.append(segment_index)
    return touched_indexes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polish narration lines for the contextual wilderness full zh->vi project."
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
    summary_path = workspace.root_dir / "polish_contextual_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Summary: {summary_path}", flush=True)
    print(
        f"Polished {len(touched_indexes)} lines; pending_review_count={pending_review_count}; semantic_qc={semantic_qc}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
