from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal, Self, overload
from uuid import UUID

from ..broker import AsyncBrokerABC, Message, MessageStatus
from .enum import TaskStatus
from .scheduler import Scheduler
from .schema import TaskResult

if TYPE_CHECKING:
    from ..broker import BrokerHealth

__all__ = (
    "ParallelBrokerABC",
    "TaskStoreABC",
)


class TaskStoreABC(ABC):
    """Abstract interface for task storage."""

    @abstractmethod
    async def save(self, message: Message) -> None:
        """Save task message to storage.

        Args:
            message: Task message to save
        """

    @abstractmethod
    async def update(
        self,
        task_id: UUID,
        status: TaskStatus | None = None,
        data: TaskResult | None = None,
    ) -> None:
        """Update task status and metadata.

        Args:
            task_id: Task identifier
            status: Task status to update
            data: Task result data
        """

    @abstractmethod
    async def get(self, task_id: UUID) -> Message:
        """Retrieve task by ID.

        Args:
            task_id: Task identifier

        Returns:
            Task or None if not found
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


class ParallelBrokerABC(ABC):
    """Parallel task broker.

    Wraps an existing AsyncBroker and adds task management capabilities.
    Provides task submission, status tracking, and result retrieval.
    """

    def __init__(
        self,
        underlying_broker: AsyncBrokerABC,
        store: TaskStoreABC | None = None,
    ) -> None:
        self._broker = underlying_broker
        self._store = store

        self._scheduler = Scheduler(underlying_broker)

    async def connect(self) -> None:
        """Connect to broker."""

        await self._broker.connect()

    async def disconnect(self) -> None:
        """Disconnect from broker."""

        await self._broker.disconnect()

    async def is_connected(self) -> bool:
        """Check if connected."""

        return await self._broker.is_connected()

    async def health_check(self) -> "BrokerHealth":
        """Perform health check."""

        return await self._broker.health_check()

    @overload
    async def submit_task(
        self,
        message: Message,
        schedule: Literal["immediate"],
    ) -> None: ...

    @overload
    async def submit_task(
        self,
        message: Message,
        schedule: Literal["delay"],
        *,
        delay_seconds: int = 0,
    ) -> None: ...

    @overload
    async def submit_task(
        self,
        message: Message,
        schedule: Literal["cron"],
        *,
        cron: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def submit_task(
        self,
        message: Message,
        schedule: Literal["immediate", "delay", "cron"] = "immediate",
        delay_seconds: int = 0,
        cron: str | None = None,
    ) -> None:
        """Submit a task for execution.

        Args:
            message: Task message
            schedule: Schedule mode (immediate/delay/cron)
            delay_seconds: Delay in seconds (for delay mode)
            cron: Cron expression (for cron mode)

        Returns:
            Task ID

        Example:
            >>> broker = ParallelBroker(AsyncValkeyBroker())
            >>> await broker.connect()
            >>> task_id = await broker.submit_task(
            ...     Message(...),
            ...     schedule="delay",
            ...     delay_seconds=300,
            ... )
        """

    @abstractmethod
    async def get_task_status(self, message_id: UUID) -> MessageStatus:
        """Get task status.

        Args:
            message_id: Task identifier

        Returns:
            Task status

        Raises:
            TaskNotFoundError: If task not found
        """

    @abstractmethod
    async def get_result(self, message_id: UUID) -> TaskResult | None:
        """Get task result.

        Args:
            message_id: Task identifier

        Returns:
            Task result or None if not available

        Raises:
            TaskNotFoundError: If task not found
        """

    @abstractmethod
    async def wait_for_task(
        self,
        message_id: UUID,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> TaskResult:
        """Wait for task completion.

        Args:
            message_id: Task identifier
            timeout: Maximum wait time in seconds
            poll_interval: Interval between status checks

        Returns:
            Task result

        Raises:
            TaskTimeoutError: If timeout occurs
            TaskNotFoundError: If task not found
        """

    @abstractmethod
    async def revoke_task(
        self,
        task_id: UUID,
        terminate: bool = False,
    ) -> bool:
        """Revoke (cancel) a task.

        Args:
            task_id: Task identifier
            terminate: If True, attempt to terminate running task

        Returns:
            True if task was revoked, False otherwise
        """

    async def produce(self, message: Message) -> None:
        """Produce message to queue (direct broker operation)."""

        await self._broker.produce(message)

    async def consume(
        self,
        queue: str,
        key: str | None = None,
        timeout: float | None = None,
    ) -> Message | None:
        """Consume message from queue (direct broker operation)."""

        return await self._broker.consume(queue, key, timeout)

    async def queue_size(self, queue: str, key: str | None = None, **kwargs: Any) -> int:
        """Get queue size (direct broker operation)."""

        return await self._broker.queue_size(queue, key, **kwargs)

    async def list_queues(self, **kwargs: Any) -> list[str]:
        """List all queues (direct broker operation)."""

        return await self._broker.list_queues(**kwargs)

    async def purge(self, queue: str, key: str | None = None, **kwargs: Any) -> int:
        """Purge queue (direct broker operation)."""

        return await self._broker.purge(queue, key, **kwargs)

    # Context manager
    async def __aenter__(self) -> Self:
        """Enter context manager."""

        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Exception,
        exc_val: Any,
        exc_tb: Any,
    ) -> None:
        """Exit context manager."""

        await self._scheduler.stop()

        await self.disconnect()
