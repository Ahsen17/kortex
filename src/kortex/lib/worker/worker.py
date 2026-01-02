import asyncio
import inspect
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import structlog
from msgspec import json

from .abc import TaskStoreABC
from .broker import AsyncBrokerABC, Message
from .config import WorkerConfig
from .enum import TaskStatus, WorkerState
from .exception import ParallelTaskNotFoundError
from .stat import WorkerStat
from .task import get_task

if TYPE_CHECKING:
    from .integration import TaskResult

logger = structlog.stdlib.get_logger(__name__)


class Worker:
    def __init__(
        self,
        broker: AsyncBrokerABC,
        queue: str,
        config: WorkerConfig | None = None,
        store: TaskStoreABC | None = None,
    ) -> None:
        self._name = self.gen_worker_name()
        self._broker = broker
        self._queue = queue
        self._config = config or WorkerConfig()
        self._store = store

        self._current_tasks: set[asyncio.Task] = set()

        self._state = WorkerState.IDLE
        self._start_time: float | None = None
        self._running = False

        self._processed_count = 0
        self._failed_count = 0
        self._last_heartbeat = time.time()

    @classmethod
    def gen_worker_name(cls) -> str:
        return f"worker-{uuid4().hex[-4:]}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> WorkerState:
        """Get current worker state."""
        return self._state

    @property
    def stats(self) -> WorkerStat:
        """Get worker statistics."""

        return WorkerStat(
            name=self.name,
            state=self._state.value,
            processed=self._processed_count,
            failed=self._failed_count,
            running_tasks=len(self._current_tasks),
            last_heartbeat=self._last_heartbeat,
            uptime=time.time() - self._start_time if self._start_time else 0,
        )

    async def start(self) -> None:
        logger.info("Starting worker...", worker=self.name)

        self._running = True
        self._state = WorkerState.RUNNING
        self._start_time = time.time()

        if not await self._broker.is_connected():
            raise Exception("Broker is not connected")

        while self._running:
            if len(self._current_tasks) >= self._config.concurrency:
                await asyncio.sleep(0.5)
                continue

            message: Message | None = None

            try:
                message = await self._broker.consume(
                    queue=self._queue,
                    timeout=self._config.queue_timeout,
                )

            except Exception as e:  # noqa: BLE001
                # Log error and continue
                logger.error(
                    "Error consuming message from queue",
                    error=e,
                )

                await asyncio.sleep(1)
                continue

            if message is None:
                await asyncio.sleep(0.5)
                continue

            try:
                task = asyncio.create_task(
                    self._execute_task(message),
                )
                self._current_tasks.add(task)
                task.add_done_callback(self._current_tasks.discard)

            except Exception:  # noqa: BLE001, S112
                continue

    async def stop(self) -> None:
        logger.info("Worker stopping...", worker=self.name)

        self._running = False
        self._state = WorkerState.STOPPING

        if self._current_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._current_tasks),
                    timeout=self._config.task_timeout,
                )

            except TimeoutError:
                for task in self._current_tasks:
                    task.cancel()

        self._state = WorkerState.STOPPED

    async def _execute_task(self, message: Message) -> None:
        task_func = None
        task_result: TaskResult | None = None

        try:
            if self._store:
                task_result = await self._store.save(
                    task_id=message.id,
                    name=message.name,
                    payload=message.payload,
                )

            task_func = self._get_task_function(message.name)
            if not task_func:
                raise ParallelTaskNotFoundError(f"Task function not found: {message.name}")

            # Execute with timeout
            result = await asyncio.wait_for(
                self._run_task_function(task_func, message.payload or {}),
                timeout=self._config.task_timeout,
            )

            logger.debug(
                "Task executed successfully",
                worker=self.name,
                task_id=str(message.id),
            )

            json.encode(result)  # Check if serializable

            if task_result and self._store:
                task_result.status = TaskStatus.SUCCESS
                task_result.result = result
                task_result.completed_at = datetime.now(UTC)

                await self._store.update(task_id=message.id, data=task_result)

            self._processed_count += 1

        except TimeoutError:
            self._failed_count += 1

            if task_result and self._store:
                task_result.status = TaskStatus.FAILED
                task_result.error = f"Task timeout after {self._config.task_timeout}s"
                task_result.completed_at = datetime.now(UTC)

                await self._store.update(message.id, data=task_result)

        except Exception as e:  # noqa: BLE001
            # TODO: retry implement

            self._failed_count += 1

            if task_result and self._store:
                task_result.status = TaskStatus.FAILED
                task_result.error = str(e)
                task_result.completed_at = datetime.now(UTC)

                await self._store.update(message.id, data=task_result)

        finally:
            if task_result and self._store:
                await self._store.update(message.id, data=task_result)

            self._last_heartbeat = time.time()

    def _get_task_function(self, task_name: str) -> Callable[..., Any] | None:
        if cell := get_task(task_name):
            return cell.func

        return None

    async def _run_task_function(
        self,
        func: Callable[..., Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        # Check if function is async
        if inspect.iscoroutinefunction(func):
            result = await func(**payload)
        else:
            result = func(**payload)

        # Convert result to dict
        if result is None:
            return {"status": TaskStatus.SUCCESS, "result": None}

        return {"status": TaskStatus.SUCCESS, "result": result}
