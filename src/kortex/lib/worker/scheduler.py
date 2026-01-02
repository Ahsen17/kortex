import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Literal, assert_never, overload
from uuid import UUID

import structlog
from croniter import croniter

from .broker import AsyncBrokerABC, Message
from .config import SchedulerConfig
from .exception import SchedulerError

logger = structlog.stdlib.get_logger(__name__)


class Scheduler:
    def __init__(
        self,
        broker: AsyncBrokerABC,
        config: SchedulerConfig | None = None,
    ) -> None:
        self._broker = broker
        self._config = config or SchedulerConfig()

        self._running = False

        self._cron_tasks: dict[UUID, asyncio.Task] = {}
        self._delay_queue: asyncio.PriorityQueue[tuple[float, Message]] = asyncio.PriorityQueue()

    @overload
    async def schedule(
        self,
        message: Message,
        schedule: Literal["immediate"],
    ) -> UUID: ...

    @overload
    async def schedule(
        self,
        message: Message,
        schedule: Literal["delay"],
        *,
        delay_seconds: int = 0,
    ) -> UUID: ...

    @overload
    async def schedule(
        self,
        message: Message,
        schedule: Literal["cron"],
        *,
        cron: str | None = None,
    ) -> UUID: ...

    async def schedule(
        self,
        message: Message,
        schedule: Literal["immediate", "delay", "cron"],
        delay_seconds: int = 0,
        cron: str | None = None,
    ) -> UUID:
        try:
            if not self._running:
                raise SchedulerError("Scheduler is not running")

            match schedule:
                case "immediate":
                    await self._submit_to_queue(message)

                case "delay":
                    # Add to delay queue with priority (earliest first)
                    execute_at = asyncio.get_event_loop().time() + delay_seconds
                    await self._delay_queue.put((execute_at, message))

                    # Start delay processor if not running
                    if not hasattr(self, "_delay_processor") or self._delay_processor.done():
                        self._delay_processor: asyncio.Task = asyncio.create_task(self._process_delay_queue())

                case "cron":
                    if not cron:
                        raise SchedulerError("Cron expression required for cron schedule")

                    # Create cron task
                    cron_task = asyncio.create_task(
                        self._process_cron_schedule(message, cron),
                    )
                    self._cron_tasks[message.id] = cron_task

                case never:
                    assert_never(never)

            return message.id

        except Exception as e:
            raise SchedulerError(f"Failed to schedule task: {e}") from e

    async def _submit_to_queue(self, message: Message) -> None:
        await self._broker.produce(message)

    async def _process_delay_queue(self) -> None:
        while self._running and not self._delay_queue.empty():
            try:
                # Get next task with timeout
                execute_at, message = await asyncio.wait_for(
                    self._delay_queue.get(),
                    timeout=1.0,
                )

                # Check if it's time to execute
                now = asyncio.get_event_loop().time()
                if now >= execute_at:
                    # Submit to queue
                    await self._submit_to_queue(message)

                else:
                    # Put back with updated time
                    await self._delay_queue.put((execute_at, message))
                    await asyncio.sleep(0.1)  # Small delay to prevent busy loop

            except TimeoutError:
                continue

            except Exception as e:  # noqa: BLE001
                # Log error
                logger.error(
                    "Error processing delayed task",
                    error=str(e),
                )

                continue

    async def _process_cron_schedule(self, message: Message, cron: str) -> None:
        iterator = croniter(cron, datetime.now(UTC))

        task_id = message.id

        while self._running and self._cron_tasks:
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
                await self._submit_to_queue(message)

            except asyncio.CancelledError:
                break

            except Exception as e:  # noqa: BLE001
                # Log error and wait before retry

                logger.error(
                    "Error processing cron task",
                    task_id=task_id,
                    error=str(e),
                )

                await asyncio.sleep(60)

    async def start(self) -> None:
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

        self._cron_tasks.clear()

    async def cancel_task(self, task_id: UUID) -> None:
        """Cancel a scheduled task by its ID."""

        # TODO: cancle orient task by id
