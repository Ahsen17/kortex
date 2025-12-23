"""Task scheduler for delayed and scheduled execution."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from croniter import croniter

if TYPE_CHECKING:
    from kortex.worker.broker.interface import AsyncBroker


import contextlib

from .config import SchedulerConfig
from .enum import TaskStatus
from .exception import SchedulerError
from .message import TaskMessage

__all__ = ("Scheduler",)


class Scheduler:
    """Task scheduler for delayed and cron-based execution.

    Supports three scheduling modes:
    - immediate: Task is queued immediately
    - delay: Task is queued after a delay
    - cron: Task is queued based on cron expression
    """

    def __init__(
        self,
        broker: AsyncBroker,
        config: SchedulerConfig | None = None,
    ):
        """Initialize scheduler.

        Args:
            broker: AsyncBroker instance for queue operations
            config: Scheduler configuration
        """
        self.broker = broker
        self.config = config or SchedulerConfig()
        self._running = False
        self._scheduled_tasks: dict[str, asyncio.Task] = {}
        self._cron_tasks: dict[str, asyncio.Task] = {}
        self._delay_queue: asyncio.PriorityQueue[tuple[float, str, dict]] = asyncio.PriorityQueue()

    async def schedule(
        self,
        task_name: str,
        payload: dict[str, Any],
        queue: str = "default",
        schedule: str = "immediate",
        delay_seconds: int = 0,
        cron: str | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Schedule a task for execution.

        Args:
            task_name: Name of the task to execute
            payload: Task payload (arguments)
            queue: Target queue name
            schedule: Schedule mode (immediate/delay/cron)
            delay_seconds: Delay in seconds (for delay mode)
            cron: Cron expression (for cron mode)
            **kwargs: Additional task metadata (priority, ttl, retry, etc.)

        Returns:
            Task ID

        Raises:
            SchedulerError: If scheduling fails
        """
        task_id = uuid.uuid4()

        try:
            if schedule == "immediate":
                await self._submit_to_queue(
                    task_id=task_id,
                    task_name=task_name,
                    payload=payload,
                    queue=queue,
                    **kwargs,
                )

            elif schedule == "delay":
                # Add to delay queue with priority (earliest first)
                execute_at = asyncio.get_event_loop().time() + delay_seconds
                await self._delay_queue.put(
                    (
                        execute_at,
                        task_id.hex,
                        {
                            "task_name": task_name,
                            "payload": payload,
                            "queue": queue,
                            **kwargs,
                        },
                    )
                )

                # Start delay processor if not running
                if not hasattr(self, "_delay_processor") or self._delay_processor.done():
                    self._delay_processor: asyncio.Task = asyncio.create_task(self._process_delay_queue())

            elif schedule == "cron":
                if not cron:
                    raise SchedulerError("Cron expression required for cron schedule")

                # Create cron task
                cron_task = asyncio.create_task(
                    self._process_cron_schedule(task_id, task_name, payload, queue, cron, **kwargs)
                )
                self._cron_tasks[task_id.hex] = cron_task

            else:
                raise SchedulerError(f"Unknown schedule mode: {schedule}")

            return task_id

        except Exception as e:
            raise SchedulerError(f"Failed to schedule task: {e}") from e

    async def _submit_to_queue(
        self,
        task_id: uuid.UUID,
        task_name: str,
        payload: dict[str, Any],
        queue: str,
        **kwargs: Any,
    ) -> None:
        """Submit task to broker queue.

        Args:
            task_id: Task identifier
            task_name: Task function name
            payload: Task arguments
            queue: Target queue
            **kwargs: Task metadata
        """
        # Create task message
        message = TaskMessage(
            queue=queue,
            payload=payload,
            task_id=task_id,
            task_name=task_name,
            status=TaskStatus.QUEUED,
            priority=kwargs.get("priority", 0),
            max_retries=kwargs.get("retry", 3),
            ttl=kwargs.get("ttl"),
            timeout=kwargs.get("timeout"),
            scheduled_at=datetime.now(UTC),
        )

        # Submit to broker
        await self.broker.produce(
            queue=queue,
            payload=message.to_dict(),
            priority=message.priority,
            ttl=message.ttl,
            key=message.key,
        )

    async def _process_delay_queue(self) -> None:
        """Process delayed tasks from queue."""
        while self._running and not self._delay_queue.empty():
            try:
                # Get next task with timeout
                execute_at, task_id, task_info = await asyncio.wait_for(self._delay_queue.get(), timeout=1.0)

                # Check if it's time to execute
                now = asyncio.get_event_loop().time()
                if now >= execute_at:
                    # Submit to queue
                    await self._submit_to_queue(
                        task_id=uuid.UUID(task_id),
                        task_name=task_info["task_name"],
                        payload=task_info["payload"],
                        queue=task_info["queue"],
                        **{k: v for k, v in task_info.items() if k not in ("task_name", "payload", "queue")},
                    )
                else:
                    # Put back with updated time
                    await self._delay_queue.put((execute_at, task_id, task_info))
                    await asyncio.sleep(0.1)  # Small delay to prevent busy loop

            except TimeoutError:
                continue
            except Exception:  # noqa: BLE001, S112
                # Log error and continue
                continue

    async def _process_cron_schedule(
        self,
        task_id: uuid.UUID,
        task_name: str,
        payload: dict[str, Any],
        queue: str,
        cron: str,
        **kwargs: Any,
    ) -> None:
        """Process cron-based scheduling.

        Args:
            task_id: Task identifier
            task_name: Task function name
            payload: Task arguments
            queue: Target queue
            cron: Cron expression
            **kwargs: Task metadata
        """
        from datetime import datetime  # noqa: PLC0415

        iterator = croniter(cron, datetime.now(UTC))
        while self._running and task_id in self._cron_tasks:
            try:
                # Get next execution time
                next_run = iterator.get_next(datetime)
                wait_seconds = (next_run - datetime.now(UTC)).total_seconds()

                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                # Check if still running
                if not self._running or task_id not in self._cron_tasks:
                    break

                # Submit to queue
                await self._submit_to_queue(
                    task_id=task_id,
                    task_name=task_name,
                    payload=payload,
                    queue=queue,
                    **kwargs,
                )

            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                # Log error and wait before retry
                await asyncio.sleep(60)

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        # Start delay processor
        self._delay_processor = asyncio.create_task(self._process_delay_queue())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

        # Cancel all cron tasks
        for task in self._cron_tasks.values():
            task.cancel()

        # Wait for tasks to complete
        if self._cron_tasks:
            await asyncio.gather(*self._cron_tasks.values(), return_exceptions=True)

        # Cancel delay processor
        if hasattr(self, "_delay_processor") and not self._delay_processor.done():
            self._delay_processor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._delay_processor

        self._scheduled_tasks.clear()
        self._cron_tasks.clear()

    async def cancel_task(self, task_id: uuid.UUID) -> bool:
        """Cancel a scheduled task.

        Args:
            task_id: Task identifier

        Returns:
            True if task was cancelled, False otherwise
        """
        # Cancel cron task
        if task_id in self._cron_tasks:
            task = self._cron_tasks.pop(task_id.hex)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            return True

        # Remove from delay queue (rebuild queue without task)
        if not self._delay_queue.empty():
            new_queue: asyncio.PriorityQueue[tuple[float, str, dict]] = asyncio.PriorityQueue()
            while not self._delay_queue.empty():
                item = await self._delay_queue.get()
                if uuid.UUID(item[1]) != task_id:
                    await new_queue.put(item)
            self._delay_queue = new_queue
            return True

        return False
