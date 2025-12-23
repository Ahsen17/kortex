"""Worker executor for parallel task framework."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from kortex.worker.broker.interface import AsyncBroker

    from .interface import TaskStore

from .config import WorkerConfig
from .enum import TaskStatus, WorkerState
from .integration import WorkerStat
from .message import TaskMessage

__all__ = ("Worker", "register_task")


# Global task registry
_task_registry: dict[str, Callable] = {}


def register_task(name: str, func: Callable) -> None:
    """Register a task function in the global registry.

    Args:
        name: Task name
        func: Task function
    """
    _task_registry[name] = func


class Worker:
    """Worker executor for processing tasks from queue."""

    def __init__(
        self,
        broker: AsyncBroker,
        queue: str,
        config: WorkerConfig | None = None,
        task_store: TaskStore | None = None,
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
            uptime=time.time() - self._start_time if hasattr(self, "_start_time") else 0,
        )

    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            return

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
                except Exception:  # noqa: BLE001
                    # Log error and continue
                    await asyncio.sleep(1)
                    continue

                if message is None:
                    # No message available, continue
                    continue

                # Parse message
                try:
                    task_message = TaskMessage.from_dict(message.payload)

                    # Execute task
                    task = asyncio.create_task(self._execute_task(message=task_message))
                    self._current_tasks.add(task)
                    task.add_done_callback(self._current_tasks.discard)
                except Exception:  # noqa: BLE001, S112
                    # Invalid message, skip
                    continue

        except asyncio.CancelledError:
            pass
        finally:
            self._state = WorkerState.STOPPED

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

    async def _execute_task(self, message: TaskMessage) -> None:
        """Execute a single task.

        Args:
            message: Task message
        """
        time.time()
        task_func = None

        try:
            # Update status to PROCESSING
            message.status = TaskStatus.PROCESSING
            message.executed_at = datetime.now(UTC)
            message.worker_id = f"worker-{id(self)}"

            if self.task_store:
                await self.task_store.update(
                    message.task_id,
                    status=message.status,
                    executed_at=message.executed_at,
                    worker_id=message.worker_id,
                )

            # Get task function
            task_func = self._get_task_function(message.task_name)
            if not task_func:
                raise ValueError(f"Task function '{message.task_name}' not found")

            # Execute with timeout
            timeout = message.timeout or self.config.task_timeout
            result = await asyncio.wait_for(
                self._run_task_function(task_func, message.payload),
                timeout=timeout,
            )

            # Success
            message.status = TaskStatus.SUCCESS
            message.result = result
            message.completed_at = datetime.now(UTC)

            self._processed_count += 1

        except TimeoutError:
            timeout_value = message.timeout or self.config.task_timeout
            message.status = TaskStatus.FAILED
            message.error = f"Task timeout after {timeout_value}s"
            message.completed_at = datetime.now(UTC)
            self._failed_count += 1

        except Exception as e:  # noqa: BLE001
            # Check retry
            if message.retry_count < message.max_retries:
                message.status = TaskStatus.RETRYING
                message.retry_count += 1
                message.error = str(e)

                # Re-submit to queue with delay
                delay = self._calculate_retry_delay(message.retry_count)
                await self._retry_task(message, delay)
            else:
                message.status = TaskStatus.FAILED
                message.error = str(e)
                message.completed_at = datetime.now(UTC)
                self._failed_count += 1

        finally:
            # Update task store
            if self.task_store:
                await self.task_store.update(
                    message.task_id,
                    status=message.status,
                    result=message.result,
                    error=message.error,
                    retry_count=message.retry_count,
                    completed_at=message.completed_at,
                )

            # Log completion
            self._last_heartbeat = time.time()

    def _get_task_function(self, task_name: str) -> Callable | None:
        """Get task function by name.

        Args:
            task_name: Task function name

        Returns:
            Task function or None
        """
        # Check global registry
        if task_name in _task_registry:
            return _task_registry[task_name]

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

    async def _retry_task(self, message: TaskMessage, delay: int) -> None:
        """Re-submit task for retry with delay.

        Args:
            message: Task message
            delay: Delay in seconds
        """
        # Wait for delay
        await asyncio.sleep(delay)

        # Re-submit to broker
        await self.broker.produce(
            queue=message.queue,
            payload=message.to_dict(),
            priority=message.priority,
            ttl=message.ttl,
            key=message.key,
        )

    def health_check(self) -> dict[str, Any]:
        """Perform health check.

        Returns:
            Health status
        """
        is_healthy = (
            self._running
            and self._state in (WorkerState.RUNNING, WorkerState.IDLE)
            and (time.time() - self._last_heartbeat) < 60
        )

        return {
            "healthy": is_healthy,
            "state": self._state.value,
            "processed": self._processed_count,
            "failed": self._failed_count,
            "running_tasks": len(self._current_tasks),
            "last_heartbeat": self._last_heartbeat,
        }
