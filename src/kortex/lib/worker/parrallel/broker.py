import asyncio
from typing import Literal, overload
from uuid import UUID

from kortex.lib.worker.broker.message import Message

from ..broker import MessageStatus
from .exception import BrokerError, TaskNotFoundError, TaskTimeoutError
from .protocol import ParallelBrokerABC
from .schema import TaskResult

__all__ = ("ParallelBroker",)


class ParallelBroker(ParallelBrokerABC):
    """Parallel task broker."""

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

    async def submit_task(
        self,
        message: Message,
        schedule: Literal["immediate", "delay", "cron"],
        cron: str | None = None,
        delay_seconds: int = 0,
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

        # Start scheduler if not running
        if not self._scheduler._running:
            await self._scheduler.start()

        if self._store:
            await self._store.save(message)

        match schedule:
            case "immediate":
                await self._scheduler.schedule(
                    message=message,
                    schedule="immediate",
                )
            case "delay":
                await self._scheduler.schedule(
                    message=message,
                    schedule="delay",
                    delay_seconds=delay_seconds,
                )
            case "cron":
                await self._scheduler.schedule(
                    message=message,
                    schedule="cron",
                    cron=cron,
                )

    async def get_task_status(self, message_id: UUID) -> MessageStatus:
        """Get task message status.

        Args:
            message_id: Task message identifier

        Returns:
            Task message status

        Raises:
            TaskNotFoundError: If task not found
        """

        if not self._store:
            raise BrokerError("Task store not configured")

        message = await self._store.get(message_id)
        if not message:
            raise TaskNotFoundError(f"Task {message_id} not found")

        return message.status

    async def get_result(self, message_id: UUID) -> TaskResult | None:
        """Get task result.

        Args:
            message_id: Task identifier

        Returns:
            Task result or None if not available

        Raises:
            TaskNotFoundError: If task not found
        """

        if not self._store:
            raise BrokerError("Task store not configured")

        return await self._store.get_result(message_id)

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

        start_time = asyncio.get_event_loop().time()

        while True:
            result = await self.get_result(message_id)

            if result and result.is_completed():
                return result

            if timeout and (asyncio.get_event_loop().time() - start_time) > timeout:
                raise TaskTimeoutError(f"Task {message_id} timed out after {timeout}s")

            await asyncio.sleep(poll_interval)

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

        # Cancel from scheduler
        cancelled = await self._scheduler.cancel_task(task_id)

        # Update task store if available
        if self._store and cancelled:
            await self._store.update(task_id, status=MessageStatus.ACKED)

        return cancelled
