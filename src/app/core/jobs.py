from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Any, Callable
from uuid import uuid4

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal, Slot

from app.core.logging import get_job_log_path, get_logger
from app.project.database import ProjectDatabase
from app.project.models import JobRunRecord


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    CANCELING = "canceling"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"


class JobCancelledError(RuntimeError):
    """Raised when a running job is canceled."""


@dataclass(slots=True)
class JobResult:
    message: str = ""
    output_paths: list[Path] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobState:
    job_id: str
    stage: str
    description: str
    status: str
    progress: int
    message: str
    project_id: str | None = None
    project_db_path: Path | None = None
    retry_of_job_id: str | None = None
    log_path: Path | None = None
    output_paths: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobSpec:
    stage: str
    description: str
    handler: Callable[["JobContext"], JobResult | None]
    project_id: str | None
    project_db_path: Path | None
    retry_of_job_id: str | None = None


class CancellationToken:
    def __init__(self) -> None:
        self._event = Event()

    def cancel(self) -> None:
        self._event.set()

    def is_canceled(self) -> bool:
        return self._event.is_set()

    def raise_if_canceled(self) -> None:
        if self.is_canceled():
            raise JobCancelledError("Job da bi huy")


class JobContext:
    def __init__(
        self,
        *,
        job_id: str,
        logger_name: str,
        cancellation_token: CancellationToken,
        progress_callback: Callable[[int, str], None],
    ) -> None:
        self.job_id = job_id
        self.logger = get_logger(logger_name, job_id=job_id)
        self.cancellation_token = cancellation_token
        self._progress_callback = progress_callback

    def report_progress(self, value: int, message: str) -> None:
        self._progress_callback(max(0, min(value, 100)), message)

    def sleep_with_cancel(self, seconds: float, interval: float = 0.05) -> None:
        elapsed = 0.0
        while elapsed < seconds:
            self.cancellation_token.raise_if_canceled()
            remaining = max(0.0, seconds - elapsed)
            pause = min(interval, remaining)
            time.sleep(pause)
            elapsed += pause


class JobSignals(QObject):
    status_changed = Signal(str, str, int, str)
    finished = Signal(str, object)
    failed = Signal(str, str)
    canceled = Signal(str, str)


class CallableJob(QRunnable):
    def __init__(
        self,
        *,
        job_id: str,
        logger_name: str,
        handler: Callable[[JobContext], JobResult | None],
        cancellation_token: CancellationToken,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.logger_name = logger_name
        self.handler = handler
        self.cancellation_token = cancellation_token
        self.signals = JobSignals()
        self.setAutoDelete(True)

    @Slot()
    def run(self) -> None:
        self.signals.status_changed.emit(self.job_id, JobStatus.RUNNING.value, 0, "Dang chay")
        context = JobContext(
            job_id=self.job_id,
            logger_name=self.logger_name,
            cancellation_token=self.cancellation_token,
            progress_callback=lambda value, message: self.signals.status_changed.emit(
                self.job_id,
                JobStatus.RUNNING.value,
                value,
                message,
            ),
        )

        try:
            result = self.handler(context) or JobResult(message="Hoan tat")
            self.cancellation_token.raise_if_canceled()
        except JobCancelledError as exc:
            self.signals.canceled.emit(self.job_id, str(exc))
        except Exception as exc:  # pragma: no cover - runtime safety path
            context.logger.exception("Job failed")
            self.signals.failed.emit(self.job_id, str(exc))
        else:
            self.signals.finished.emit(self.job_id, result)


class JobManager(QObject):
    job_updated = Signal(object)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._pool = QThreadPool.globalInstance()
        self._states: dict[str, JobState] = {}
        self._specs: dict[str, JobSpec] = {}
        self._tokens: dict[str, CancellationToken] = {}

    def submit_job(
        self,
        *,
        stage: str,
        description: str,
        handler: Callable[[JobContext], JobResult | None],
        project_id: str | None = None,
        project_db_path: Path | None = None,
        retry_of_job_id: str | None = None,
    ) -> str:
        job_id = str(uuid4())
        log_path = get_job_log_path(job_id)
        state = JobState(
            job_id=job_id,
            stage=stage,
            description=description,
            status=JobStatus.QUEUED.value,
            progress=0,
            message="Dang vao hang doi",
            project_id=project_id,
            project_db_path=project_db_path,
            retry_of_job_id=retry_of_job_id,
            log_path=log_path,
        )
        spec = JobSpec(
            stage=stage,
            description=description,
            handler=handler,
            project_id=project_id,
            project_db_path=project_db_path,
            retry_of_job_id=retry_of_job_id,
        )
        token = CancellationToken()

        self._states[job_id] = state
        self._specs[job_id] = spec
        self._tokens[job_id] = token

        self._persist_job_insert(state)
        self.job_updated.emit(state)

        runnable = CallableJob(
            job_id=job_id,
            logger_name=f"jobs.{stage}",
            handler=handler,
            cancellation_token=token,
        )
        runnable.signals.status_changed.connect(self._on_status_changed)
        runnable.signals.finished.connect(self._on_finished)
        runnable.signals.failed.connect(self._on_failed)
        runnable.signals.canceled.connect(self._on_canceled)
        self._pool.start(runnable)
        return job_id

    def cancel_job(self, job_id: str) -> None:
        token = self._tokens.get(job_id)
        state = self._states.get(job_id)
        if not token or not state:
            return

        token.cancel()
        state.status = JobStatus.CANCELING.value
        state.message = "Dang huy"
        self._persist_job_update(state)
        self.job_updated.emit(state)

    def retry_job(self, job_id: str) -> str | None:
        spec = self._specs.get(job_id)
        if not spec:
            return None
        return self.submit_job(
            stage=spec.stage,
            description=spec.description,
            handler=spec.handler,
            project_id=spec.project_id,
            project_db_path=spec.project_db_path,
            retry_of_job_id=job_id,
        )

    @Slot(str, str, int, str)
    def _on_status_changed(self, job_id: str, status: str, progress: int, message: str) -> None:
        state = self._states[job_id]
        state.status = status
        state.progress = progress
        state.message = message
        self._persist_job_update(state)
        self.job_updated.emit(state)

    @Slot(str, object)
    def _on_finished(self, job_id: str, result: JobResult) -> None:
        state = self._states[job_id]
        state.status = JobStatus.SUCCESS.value
        state.progress = 100
        state.message = result.message or "Hoan tat"
        state.output_paths = [str(path) for path in result.output_paths]
        state.extra = result.extra
        self._persist_job_update(
            state,
            ended_at=utc_now_iso(),
            output_paths=state.output_paths,
        )
        self.job_updated.emit(state)

    @Slot(str, str)
    def _on_failed(self, job_id: str, error_message: str) -> None:
        state = self._states[job_id]
        state.status = JobStatus.FAILED.value
        state.message = error_message
        state.extra = {"error": error_message}
        self._persist_job_update(
            state,
            ended_at=utc_now_iso(),
            error_json={"message": error_message},
        )
        self.job_updated.emit(state)

    @Slot(str, str)
    def _on_canceled(self, job_id: str, message: str) -> None:
        state = self._states[job_id]
        state.status = JobStatus.CANCELED.value
        state.message = message
        state.extra = {"canceled": True}
        self._persist_job_update(state, ended_at=utc_now_iso())
        self.job_updated.emit(state)

    def _persist_job_insert(self, state: JobState) -> None:
        if not state.project_db_path:
            return
        database = ProjectDatabase(state.project_db_path)
        database.insert_job_run(
            JobRunRecord(
                job_id=state.job_id,
                project_id=state.project_id,
                stage=state.stage,
                description=state.description,
                status=state.status,
                progress=state.progress,
                started_at=utc_now_iso(),
                log_path=str(state.log_path) if state.log_path else None,
                retry_of_job_id=state.retry_of_job_id,
                message=state.message,
            )
        )

    def _persist_job_update(
        self,
        state: JobState,
        *,
        ended_at: str | None = None,
        output_paths: list[str] | None = None,
        error_json: dict[str, object] | None = None,
    ) -> None:
        if not state.project_db_path:
            return
        database = ProjectDatabase(state.project_db_path)
        database.update_job_run(
            state.job_id,
            status=state.status,
            progress=state.progress,
            message=state.message,
            log_path=str(state.log_path) if state.log_path else None,
            ended_at=ended_at,
            output_paths=output_paths,
            error_json=error_json,
        )
