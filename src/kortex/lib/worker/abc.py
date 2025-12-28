from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from .enum import TaskStatus
from .integration import TaskResult

__all__ = ("TaskStoreABC",)


class TaskStoreABC(ABC):
    """Abstract interface for task storage."""

    @abstractmethod
    async def save(
        self,
        task_id: UUID,
        name: str,
        payload: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Save task message to storage.

        Args:
            task_id: Task identifier
            name: Task name
            queue: Queue name
            key: Task key (optional)
            payload: Task payload data (optional)
        """

    @abstractmethod
    async def update(
        self,
        task_id: UUID,
        status: TaskStatus | None = None,
        **kwargs: Any,
    ) -> TaskResult:
        """Update task status and metadata.

        Args:
            task_id: Task identifier
            status: Task status to update
            **kwargs: Task result data
        """

    @abstractmethod
    async def get(self, task_id: UUID) -> TaskResult:
        """Retrieve task by ID.

        Args:
            task_id: Task identifier

        Returns:
            Task or None if not found
        """

    @abstractmethod
    async def delete(self, task_id: UUID) -> TaskResult:
        """Delete task from storage.

        Args:
            task_id: Task identifier
        """
