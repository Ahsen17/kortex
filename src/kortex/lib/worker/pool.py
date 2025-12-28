import asyncio
import contextlib

import structlog

from .abc import TaskStoreABC
from .broker import AsyncBrokerABC
from .config import PoolConfig, WorkerConfig
from .enum import WorkerPoolState
from .stat import WorkerPoolStat, WorkerQueueStat
from .worker import Worker

__all__ = ("WorkerPool",)

logger = structlog.stdlib.get_logger(__name__)


class WorkerPool:
    def __init__(
        self,
        broker: AsyncBrokerABC,
        store: TaskStoreABC | None = None,
        pool_config: PoolConfig | None = None,
        worker_config: WorkerConfig | None = None,
    ) -> None:
        self._broker = broker
        self._store = store
        self._config = pool_config or PoolConfig()
        self._worker_config = worker_config or WorkerConfig()

        self._workers: dict[str, list[Worker]] = {}
        self._monitor_task: asyncio.Task | None = None

        self._state = WorkerPoolState.IDLE
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._state = WorkerPoolState.RUNNING

        try:
            # Initialize workers for each queue
            for queue in self._config.queues:
                self._workers[queue] = []
                for _ in range(self._config.base_workers):
                    await self._add_worker(queue)

            # Start elastic scaling monitor
            if self._config.auto_scale:
                self._monitor_task = asyncio.create_task(self._monitor_and_scale())

        except asyncio.CancelledError:
            logger.info("Worker pool stopped.")

        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running:
            return

        self._state = WorkerPoolState.STOPPED
        self._running = False

        # Stop monitor
        if self._monitor_task:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task

        # Stop all workers
        stop_tasks = [worker.stop() for workers in self._workers.values() for worker in workers]

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._workers.clear()

    async def _add_worker(self, queue: str) -> None:
        worker = Worker(
            broker=self._broker,
            queue=queue,
            config=self._worker_config,
            store=self._store,
        )

        self._workers[queue].append(worker)

        await asyncio.create_task(worker.start())

    async def _remove_worker(self, queue: str) -> bool:
        if queue not in self._workers or not self._workers[queue]:
            return False

        # Don't remove below base_workers
        if len(self._workers[queue]) <= self._config.base_workers:
            return False

        worker = self._workers[queue].pop()
        await worker.stop()
        return True

    async def _monitor_and_scale(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds

                for queue in self._config.queues:
                    # Get queue size
                    try:
                        queue_size = await self._broker.queue_size(queue)
                    except Exception:  # noqa: BLE001, S112
                        continue

                    current_workers = len(self._workers.get(queue, []))

                    # Scale up if queue is overloaded
                    if queue_size > current_workers * 5 and current_workers < self._config.max_workers:
                        await self._add_worker(queue)

                    # Scale down if queue is empty
                    elif queue_size == 0 and current_workers > self._config.base_workers:
                        await self._remove_worker(queue)

            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                # Log error and continue
                logger.error("Error monitoring and scaling workers", error=e)

                await asyncio.sleep(1)

    def get_stats(self) -> WorkerPoolStat:
        stats = WorkerPoolStat()

        for queue, workers in self._workers.items():
            worker_stats = [w.stats for w in workers]

            stats.queues[queue] = WorkerQueueStat(
                worker_count=len(workers),
                worker_stats=worker_stats,
            )

            stats.total_workers += len(workers)

        return stats

    def get_worker_count(self, queue: str | None = None) -> int:
        if queue:
            return len(self._workers.get(queue, []))

        return sum(len(workers) for workers in self._workers.values())
