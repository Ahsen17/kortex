"""Abstract interfaces for parallel task framework."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from uuid import UUID

    from .message import TaskMessage, TaskResult

__all__ = (
    "HealthChecker",
    "TaskExecutor",
    "TaskStore",
)


class TaskExecutor(ABC):
    """Abstract interface for task execution."""

    @abstractmethod
    async def execute(self, message: TaskMessage) -> TaskResult:
        """Execute a task.

        Args:
            message: Task message with execution context

        Returns:
            Task execution result
        """


class TaskStore(ABC):
    """Abstract interface for task storage."""

    @abstractmethod
    async def save(self, message: TaskMessage) -> None:
        """Save task message to storage.

        Args:
            message: Task message to save
        """

    @abstractmethod
    async def update(self, task_id: UUID, **kwargs: Any) -> None:
        """Update task status and metadata.

        Args:
            task_id: Task identifier
            **kwargs: Fields to update
        """

    @abstractmethod
    async def get(self, task_id: UUID) -> TaskMessage | None:
        """Retrieve task by ID.

        Args:
            task_id: Task identifier

        Returns:
            Task message or None if not found
        """

    @abstractmethod
    async def get_result(self, task_id: UUID) -> TaskResult | None:
        """Get task result.

        Args:
            task_id: Task identifier

        Returns:
            Task result or None if not available
        """

    @abstractmethod
    async def delete(self, task_id: UUID) -> None:
        """Delete task from storage.

        Args:
            task_id: Task identifier
        """


class HealthChecker(ABC):
    """Abstract interface for health checking."""

    @abstractmethod
    async def check(self) -> dict[str, Any]:
        """Perform health check.

        Returns:
            Health status information
        """

    @abstractmethod
    async def is_healthy(self) -> bool:
        """Check if service is healthy.

        Returns:
            True if healthy, False otherwise
        """
