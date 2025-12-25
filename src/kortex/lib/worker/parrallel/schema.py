from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from .enum import TaskStatus

__all__ = ("TaskResult",)


@dataclass
class TaskResult:
    """Container for task execution result."""

    task_id: UUID
    status: TaskStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    executed_at: datetime | None = None
    completed_at: datetime | None = None

    def is_success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.SUCCESS

    def is_failed(self) -> bool:
        """Check if task failed permanently."""
        return self.status == TaskStatus.FAILED

    def is_completed(self) -> bool:
        """Check if task is completed (success or failed)."""
        return self.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.REVOKED)
