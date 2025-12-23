"""Database models for task storage."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any

import sqlalchemy as sa
from advanced_alchemy.types import DateTimeUTC, JsonB
from sqlalchemy.orm import Mapped, mapped_column

from kortex.db import AuditMixin

from .enum import TaskStatus

__all__ = ("TaskModel",)


class TaskModel(AuditMixin):
    """Task record model.

    Stores task execution history and results in database.
    """

    __tablename__ = "task"

    name: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Task name",
    )
    queue: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Task queue",
    )
    status: Mapped[TaskStatus] = mapped_column(
        sa.String(),
        nullable=False,
        default=TaskStatus.PENDING,
        comment="Task status",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JsonB,
        nullable=False,
        default={},
        comment="Task payload",
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(
        JsonB,
        nullable=True,
        comment="Task result",
    )
    error: Mapped[str | None] = mapped_column(
        sa.String(),
        nullable=True,
        comment="Error message if failed",
    )
    retry_count: Mapped[int] = mapped_column(
        default=0,
        nullable=False,
        comment="Number of retries",
    )
    max_retries: Mapped[int] = mapped_column(
        default=3,
        nullable=False,
        comment="Maximum number of retries",
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTimeUTC(timezone=True),
        nullable=True,
        comment="When task was scheduled",
    )
    executed_at: Mapped[datetime | None] = mapped_column(
        DateTimeUTC(timezone=True),
        nullable=True,
        comment="When task was executed",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTimeUTC(timezone=True),
        nullable=True,
        comment="When task was completed",
    )
    worker_id: Mapped[str | None] = mapped_column(
        sa.String(),
        nullable=True,
        comment="Worker ID",
    )
    priority: Mapped[int] = mapped_column(
        sa.Integer,
        default=0,
        nullable=False,
        comment="Task priority",
    )
    ttl: Mapped[int | None] = mapped_column(
        sa.Integer,
        nullable=True,
        comment="Time-to-live in seconds",
    )
    timeout: Mapped[int | None] = mapped_column(
        sa.Integer,
        nullable=True,
        comment="Task execution timeout in seconds",
    )
    meta: Mapped[dict[str, Any]] = mapped_column(
        JsonB,
        nullable=False,
        default={},
        comment="Additional task metadata",
    )

    __table_args__ = (
        # Index for common queries
        sa.Index("idx_task_status", "status"),
        sa.Index("idx_task_queue", "queue"),
        sa.Index("idx_task_worker", "worker_id"),
        sa.Index("idx_task_created", "created_at"),
        sa.Index("idx_task_completed", "completed_at"),
    )

    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self.status in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.REVOKED,
            TaskStatus.EXPIRED,
        )

    def is_success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.SUCCESS

    def is_failed(self) -> bool:
        """Check if task failed permanently."""
        return self.status == TaskStatus.FAILED

    def should_retry(self) -> bool:
        """Check if task should be retried."""
        return self.status == TaskStatus.RETRYING or (
            self.status == TaskStatus.FAILED and self.retry_count < self.max_retries
        )
