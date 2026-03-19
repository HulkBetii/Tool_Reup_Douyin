from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from app.core.jobs import JobStatus


class StatusPanel(QWidget):
    cancel_requested = Signal(str)
    retry_requested = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._row_by_job_id: dict[str, int] = {}

        self._title = QLabel("Tiến trình tác vụ")
        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["Mã job", "Công đoạn", "Trạng thái", "Tiến độ", "Thông báo"])
        self._table.setWordWrap(True)
        header = self._table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.itemSelectionChanged.connect(self._update_buttons)

        self._cancel_button = QPushButton("Hủy")
        self._cancel_button.clicked.connect(self._emit_cancel_requested)
        self._retry_button = QPushButton("Chạy lại")
        self._retry_button.clicked.connect(self._emit_retry_requested)

        button_row = QHBoxLayout()
        button_row.addWidget(self._cancel_button)
        button_row.addWidget(self._retry_button)
        button_row.addStretch(1)

        layout = QVBoxLayout(self)
        layout.addWidget(self._title)
        layout.addWidget(self._table)
        layout.addLayout(button_row)
        self._update_buttons()

    def upsert_job(self, state: object) -> None:
        job_id = getattr(state, "job_id")
        if job_id in self._row_by_job_id:
            row = self._row_by_job_id[job_id]
        else:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._row_by_job_id[job_id] = row

        values = [
            getattr(state, "job_id"),
            getattr(state, "stage"),
            getattr(state, "status"),
            f"{getattr(state, 'progress')}%",
            getattr(state, "message"),
        ]
        for column, value in enumerate(values):
            item = self._table.item(row, column)
            if item is None:
                item = QTableWidgetItem()
                item.setFlags(item.flags() ^ Qt.ItemFlag.ItemIsEditable)
                self._table.setItem(row, column, item)
            item.setText(str(value))
        self._table.resizeRowToContents(row)

        self._update_buttons()

    def _selected_job_id(self) -> str | None:
        items = self._table.selectedItems()
        if not items:
            return None
        return self._table.item(items[0].row(), 0).text()

    def _selected_status(self) -> str | None:
        items = self._table.selectedItems()
        if not items:
            return None
        return self._table.item(items[0].row(), 2).text()

    def _update_buttons(self) -> None:
        status = self._selected_status()
        can_cancel = status in {
            JobStatus.QUEUED.value,
            JobStatus.RUNNING.value,
            JobStatus.CANCELING.value,
        }
        can_retry = status in {JobStatus.FAILED.value, JobStatus.CANCELED.value}
        self._cancel_button.setEnabled(can_cancel)
        self._retry_button.setEnabled(can_retry)

    def _emit_cancel_requested(self) -> None:
        job_id = self._selected_job_id()
        if job_id:
            self.cancel_requested.emit(job_id)

    def _emit_retry_requested(self) -> None:
        job_id = self._selected_job_id()
        if job_id:
            self.retry_requested.emit(job_id)
