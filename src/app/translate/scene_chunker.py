from __future__ import annotations

from dataclasses import dataclass
from sqlite3 import Row


DEFAULT_SCENE_GAP_MS = 1500
DEFAULT_SCENE_MAX_SEGMENTS = 24
DEFAULT_SCENE_MAX_DURATION_MS = 75_000


@dataclass(slots=True, frozen=True)
class SceneChunk:
    scene_id: str
    scene_index: int
    start_segment_index: int
    end_segment_index: int
    start_ms: int
    end_ms: int
    segment_ids: list[str]
    segments: list[Row]

    @property
    def duration_ms(self) -> int:
        return max(0, self.end_ms - self.start_ms)


def chunk_segments_into_scenes(
    segments: list[Row],
    *,
    gap_ms: int = DEFAULT_SCENE_GAP_MS,
    max_segments: int = DEFAULT_SCENE_MAX_SEGMENTS,
    max_duration_ms: int = DEFAULT_SCENE_MAX_DURATION_MS,
) -> list[SceneChunk]:
    if not segments:
        return []

    scenes: list[SceneChunk] = []
    current: list[Row] = []
    scene_index = 0

    def flush() -> None:
        nonlocal current
        if not current:
            return
        start_segment_index = int(current[0]["segment_index"])
        end_segment_index = int(current[-1]["segment_index"])
        start_ms = int(current[0]["start_ms"])
        end_ms = int(current[-1]["end_ms"])
        scene_id = f"scene_{scene_index:04d}"
        scenes.append(
            SceneChunk(
                scene_id=scene_id,
                scene_index=scene_index,
                start_segment_index=start_segment_index,
                end_segment_index=end_segment_index,
                start_ms=start_ms,
                end_ms=end_ms,
                segment_ids=[str(row["segment_id"]) for row in current],
                segments=list(current),
            )
        )
        current = []

    previous_row: Row | None = None
    for row in segments:
        if previous_row is None:
            current.append(row)
            previous_row = row
            continue

        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        previous_end_ms = int(previous_row["end_ms"])
        current_start_ms = int(current[0]["start_ms"])
        should_split = False
        if start_ms - previous_end_ms >= gap_ms:
            should_split = True
        elif len(current) >= max_segments:
            should_split = True
        elif end_ms - current_start_ms >= max_duration_ms:
            should_split = True

        if should_split:
            flush()
            scene_index += 1
        current.append(row)
        previous_row = row

    flush()
    return scenes
