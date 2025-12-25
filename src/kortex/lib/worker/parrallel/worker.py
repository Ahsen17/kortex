import asyncio
import importlib
import inspect
import time
from collections.abc import Callable
from typing import Any

import structlog

from ..broker import Message
from .config import WorkerConfig
from .enum import WorkerState
from .integration import WorkerHealth, WorkerStat
from .protocol import ParallelBrokerABC, TaskStoreABC

__all__ = ("TaskRegister",)

logger = structlog.stdlib.get_logger(__name__)


class TaskRegister:
    _task_registry: dict[str, Callable] = {}

    @classmethod
    def register_task(cls, name: str, func: Callable) -> None:
        """Register a task function in the global registry."""

        cls._task_registry[name] = func

    @classmethod
    def get_task(cls, name: str) -> Callable | None:
        """Get a task function from the global registry."""

        return cls._task_registry.get(name, None)


class Worker:
    """Worker executor for processing tasks from queue."""

    def __init__(
        self,
        broker: ParallelBrokerABC,
        queue: str,
        config: WorkerConfig | None = None,
        task_store: TaskStoreABC | None = None,
    ):
        """Initialize worker.

        Args:
            broker: AsyncBroker instance
            queue: Queue name to consume from
            config: Worker configuration
            task_store: Optional task storage for results
        """
        self.broker = broker
        self.queue = queue
        self.config = config or WorkerConfig()
        self.task_store = task_store

        self._state = WorkerState.IDLE
        self._start_time: float | None = None
        self._running = False
        self._current_tasks: set[asyncio.Task] = set()
        self._processed_count = 0
        self._failed_count = 0
        self._last_heartbeat = time.time()

    @property
    def state(self) -> WorkerState:
        """Get current worker state."""
        return self._state

    @property
    def stats(self) -> WorkerStat:
        """Get worker statistics."""

        return WorkerStat(
            state=self._state.value,
            processed=self._processed_count,
            failed=self._failed_count,
            running_tasks=len(self._current_tasks),
            last_heartbeat=self._last_heartbeat,
            uptime=time.time() - self._start_time if self._start_time else 0,
        )

    async def start(self) -> None:
        """Start worker."""

        self._running = True
        self._state = WorkerState.RUNNING
        self._start_time = time.time()

        try:
            while self._running:
                # Check if we can accept more tasks
                if len(self._current_tasks) >= self.config.concurrency:
                    await asyncio.sleep(0.1)
                    continue

                # Consume task from queue
                message = None

                try:
                    message = await self.broker.consume(
                        self.queue,
                        timeout=self.config.queue_timeout,
                    )

                except Exception as e:  # noqa: BLE001
                    # Log error and continue
                    logger.error("Error consuming message from queue", error=e)

                    # TODO: Cannot find oriented queue

                    await asyncio.sleep(1)
                    continue

                if message is None:
                    # No message available, continue
                    await asyncio.sleep(0.1)
                    continue

                # Parse message
                try:
                    # Execute task
                    task = asyncio.create_task(self._execute_task(message))
                    self._current_tasks.add(task)
                    task.add_done_callback(self._current_tasks.discard)

                except Exception:  # noqa: BLE001, S112
                    # Invalid message, skip
                    continue

        except asyncio.CancelledError:
            pass

        finally:
            self._state = WorkerState.STOPPED

    async def _execute_task(self, message: Message) -> None:
        """Execute a single task.

        Args:
            message: Task message
        """

        raise NotImplementedError()  # TODO: need refactor `Message` struct

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        self._running = False
        self._state = WorkerState.STOPPING

        # Wait for current tasks to complete
        if self._current_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._current_tasks, return_exceptions=True),
                    timeout=self.config.task_timeout,
                )
            except TimeoutError:
                # Force cancel remaining tasks
                for task in self._current_tasks:
                    task.cancel()

        self._state = WorkerState.STOPPED

    def _get_task_function(self, task_name: str) -> Callable | None:
        """Get task function by name.

        Args:
            task_name: Task function name

        Returns:
            Task function or None
        """
        # Check global registry
        if task := TaskRegister.get_task(task_name):
            return task

        # Try to import from module
        if "." in task_name:
            try:
                module_name, func_name = task_name.rsplit(".", 1)
                module = importlib.import_module(module_name)

                return getattr(module, func_name, None)

            except (ImportError, AttributeError):
                pass

        return None

    async def _run_task_function(self, func: Callable, payload: dict[str, Any]) -> dict[str, Any]:
        """Run task function with proper argument handling.

        Args:
            func: Task function
            payload: Task payload

        Returns:
            Function result
        """
        # Check if function is async
        if inspect.iscoroutinefunction(func):
            result = await func(**payload)
        else:
            result = func(**payload)

        # Convert result to dict
        if result is None:
            return {"status": "completed"}

        if isinstance(result, dict):
            return result

        return {"result": result}

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """Calculate retry delay with exponential backoff.

        Args:
            retry_count: Current retry count

        Returns:
            Delay in seconds
        """
        base_delay = 1  # 1 second
        max_delay = 300  # 5 minutes
        delay = min(base_delay * (2**retry_count), max_delay)
        return int(delay)

    async def _retry_task(self, message: Message, delay: int) -> None:
        """Re-submit task for retry with delay.

        Args:
            message: Task message
            delay: Delay in seconds
        """
        # Wait for delay
        await asyncio.sleep(delay)

        # Re-submit to broker
        await self.broker.produce(message)

    def health_check(self) -> WorkerHealth:
        """Perform health check.

        Returns:
            Health status
        """
        is_healthy = (
            self._running
            and self._state in (WorkerState.RUNNING, WorkerState.IDLE)
            and (time.time() - self._last_heartbeat) < 60
        )

        return WorkerHealth(
            healthy=is_healthy,
            state=self._state,
            processed=self._processed_count,
            failed=self._failed_count,
            running_tasks=len(self._current_tasks),
            last_heartbeat=self._last_heartbeat,
        )
