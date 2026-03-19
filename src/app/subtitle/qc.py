from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(slots=True, frozen=True)
class SubtitleQcIssue:
    segment_id: str
    segment_index: int
    code: str
    severity: str
    message: str
    cps: float | None = None
    cpl: int | None = None


@dataclass(slots=True, frozen=True)
class SubtitleQcConfig:
    max_lines: int = 2
    max_cpl: int = 42
    max_cps: float = 18.0
    min_duration_ms: int = 800
    max_duration_ms: int = 7000


@dataclass(slots=True, frozen=True)
class SubtitleQcReport:
    total_segments: int
    issues: list[SubtitleQcIssue]

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")

    @property
    def ok_count(self) -> int:
        return max(0, self.total_segments - len({issue.segment_id for issue in self.issues}))


def _normalized_subtitle_text(row: dict[str, object]) -> str:
    subtitle_text = str(row.get("subtitle_text", "") or "").strip()
    translated_text = str(row.get("translated_text", "") or "").strip()
    source_text = str(row.get("source_text", "") or "").strip()
    return subtitle_text or translated_text or source_text


def _count_visible_characters(text: str) -> int:
    return len(text.replace("\r", "").replace("\n", ""))


def _max_line_length(text: str) -> int:
    return max((len(line) for line in text.splitlines()), default=0)


def _line_count(text: str) -> int:
    return max(1, len(text.splitlines())) if text else 0


def analyze_subtitle_rows(
    rows: Iterable[dict[str, object]],
    *,
    config: SubtitleQcConfig | None = None,
) -> SubtitleQcReport:
    cfg = config or SubtitleQcConfig()
    normalized_rows = list(rows)
    issues: list[SubtitleQcIssue] = []
    previous_end_ms: int | None = None

    for row in normalized_rows:
        segment_id = str(row["segment_id"])
        segment_index = int(row["segment_index"])
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        explicit_text = str(row.get("subtitle_text", "") or "").strip() or str(
            row.get("translated_text", "") or ""
        ).strip()
        text = explicit_text or _normalized_subtitle_text(row)
        duration_ms = end_ms - start_ms
        cps = (_count_visible_characters(text) / (duration_ms / 1000.0)) if duration_ms > 0 and text else 0.0
        cpl = _max_line_length(text)
        line_count = _line_count(text)

        if previous_end_ms is not None and start_ms < previous_end_ms:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="overlap",
                    severity="error",
                    message="Segment bi overlap voi segment truoc",
                )
            )
        previous_end_ms = end_ms

        if duration_ms <= 0:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="invalid_duration",
                    severity="error",
                    message="Duration phai lon hon 0",
                )
            )
            continue

        if not explicit_text:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="empty_text",
                    severity="warning",
                    message="Subtitle text dang rong",
                )
            )

        if duration_ms < cfg.min_duration_ms:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="short_duration",
                    severity="warning",
                    message=f"Duration ngan hon {cfg.min_duration_ms} ms",
                )
            )

        if duration_ms > cfg.max_duration_ms:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="long_duration",
                    severity="warning",
                    message=f"Duration dai hon {cfg.max_duration_ms} ms",
                )
            )

        if line_count > cfg.max_lines:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="too_many_lines",
                    severity="warning",
                    message=f"So dong vuot qua {cfg.max_lines}",
                    cpl=cpl,
                )
            )

        if cpl > cfg.max_cpl:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="high_cpl",
                    severity="warning",
                    message=f"CPL vuot qua {cfg.max_cpl}",
                    cpl=cpl,
                )
            )

        if cps > cfg.max_cps:
            issues.append(
                SubtitleQcIssue(
                    segment_id=segment_id,
                    segment_index=segment_index,
                    code="high_cps",
                    severity="warning",
                    message=f"CPS vuot qua {cfg.max_cps:.1f}",
                    cps=round(cps, 2),
                    cpl=cpl,
                )
            )

    return SubtitleQcReport(total_segments=len(normalized_rows), issues=issues)
