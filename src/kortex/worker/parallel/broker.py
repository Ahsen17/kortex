"""Parallel task broker - wraps existing broker with task management."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from .enum import TaskStatus
from .exception import BrokerError, TaskNotFoundError, TaskTimeoutError
from .message import TaskMessage, TaskResult
from .scheduler import Scheduler

if TYPE_CHECKING:
    from uuid import UUID

    from kortex.worker.broker.interface import AsyncBroker, BrokerMessage

    from .config import SchedulerConfig
    from .interface import TaskStore


__all__ = ("ParallelBroker",)


class ParallelBroker:
    """Parallel task broker.

    Wraps an existing AsyncBroker and adds task management capabilities.
    Provides task submission, status tracking, and result retrieval.
    """

    def __init__(
        self,
        underlying_broker: AsyncBroker[TaskMessage],
        task_store: TaskStore,
        scheduler_config: SchedulerConfig | None = None,
    ):
        """Initialize parallel broker.

        Args:
            underlying_broker: Base broker (e.g., AsyncValkeyBroker)
            task_store: Storage for task results (optional)
            scheduler_config: Scheduler configuration
        """
        self._broker = underlying_broker
        self._task_store = task_store
        self._scheduler = Scheduler(underlying_broker, scheduler_config)

    # Delegated broker methods
    async def connect(self, **kwargs: Any) -> None:
        """Connect to broker."""
        await self._broker.connect(**kwargs)

    async def disconnect(self) -> None:
        """Disconnect from broker."""
        await self._broker.disconnect()

    async def is_connected(self) -> bool:
        """Check if connected."""
        return await self._broker.is_connected()

    async def health_check(self) -> Any:
        """Perform health check."""
        return await self._broker.health_check()

    # Task submission
    async def submit_task(
        self,
        task_name: str,
        payload: dict[str, Any],
        queue: str = "default",
        schedule: str = "immediate",
        delay_seconds: int = 0,
        cron: str | None = None,
        **kwargs: Any,
    ) -> UUID:
        """Submit a task for execution.

        Args:
            task_name: Name of the task function
            payload: Task arguments
            queue: Target queue
            schedule: Schedule mode (immediate/delay/cron)
            delay_seconds: Delay in seconds (for delay mode)
            cron: Cron expression (for cron mode)
            **kwargs: Task metadata (priority, ttl, retry, timeout)

        Returns:
            Task ID

        Example:
            >>> broker = ParallelBroker(AsyncValkeyBroker())
            >>> await broker.connect()
            >>> task_id = await broker.submit_task(
            ...     "send_email",
            ...     {"to": "user@example.com", "subject": "Hello"},
            ...     schedule="delay",
            ...     delay_seconds=300,
            ...     retry=3,
            ... )
        """
        # Start scheduler if not running
        if not self._scheduler._running:
            await self._scheduler.start()

        task_id = await self._scheduler.schedule(
            task_name=task_name,
            payload=payload,
            queue=queue,
            schedule=schedule,
            delay_seconds=delay_seconds,
            cron=cron,
            **kwargs,
        )

        # Save initial task state if store is available
        message = TaskMessage(
            queue=queue,
            payload=payload,
            task_id=task_id,
            task_name=task_name,
            status=TaskStatus.PENDING,
            priority=kwargs.get("priority", 0),
            max_retries=kwargs.get("retry", 3),
            ttl=kwargs.get("ttl"),
            timeout=kwargs.get("timeout"),
        )

        if self._task_store:
            await self._task_store.save(message)

        # Produce a message
        await self._broker.produce(queue, message)

        return task_id

    # Task status and results
    async def get_task_status(self, task_id: UUID) -> TaskStatus:
        """Get task status.

        Args:
            task_id: Task identifier

        Returns:
            Task status

        Raises:
            TaskNotFoundError: If task not found
        """
        if not self._task_store:
            raise BrokerError("Task store not configured")

        message = await self._task_store.get(task_id)
        if not message:
            raise TaskNotFoundError(f"Task {task_id} not found")

        return message.status

    async def get_result(self, task_id: UUID) -> TaskResult | None:
        """Get task result.

        Args:
            task_id: Task identifier

        Returns:
            Task result or None if not available

        Raises:
            TaskNotFoundError: If task not found
        """
        if not self._task_store:
            raise BrokerError("Task store not configured")

        return await self._task_store.get_result(task_id)

    async def wait_for_task(
        self,
        task_id: UUID,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> TaskResult:
        """Wait for task completion.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time in seconds
            poll_interval: Interval between status checks

        Returns:
            Task result

        Raises:
            TaskTimeoutError: If timeout occurs
            TaskNotFoundError: If task not found
        """
        start_time = asyncio.get_event_loop().time()

        while True:
            result = await self.get_result(task_id)

            if result and result.is_completed():
                return result

            if timeout and (asyncio.get_event_loop().time() - start_time) > timeout:
                raise TaskTimeoutError(f"Task {task_id} timed out after {timeout}s")

            await asyncio.sleep(poll_interval)

    # Task control
    async def revoke_task(self, task_id: UUID, terminate: bool = False) -> bool:
        """Revoke (cancel) a task.

        Args:
            task_id: Task identifier
            terminate: If True, attempt to terminate running task

        Returns:
            True if task was revoked, False otherwise
        """
        # Cancel from scheduler
        cancelled = await self._scheduler.cancel_task(task_id)

        # Update task store if available
        if self._task_store and cancelled:
            await self._task_store.update(task_id, status=TaskStatus.REVOKED)

        return cancelled

    # Queue operations (delegated)
    async def produce(
        self,
        queue: str,
        payload: Any,
        priority: int | None = None,
        ttl: float | None = None,
        key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Produce message to queue (direct broker operation)."""
        await self._broker.produce(
            queue=queue,
            payload=payload,
            priority=priority,
            ttl=ttl,
            key=key,
            **kwargs,
        )

    async def consume(
        self,
        queue: str,
        key: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> BrokerMessage[Any] | None:
        """Consume message from queue (direct broker operation)."""
        return await self._broker.consume(
            queue=queue,
            key=key,
            timeout=timeout,
            **kwargs,
        )

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
    async def __aenter__(self) -> ParallelBroker:
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
