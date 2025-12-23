"""Worker pool management with elastic scaling."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kortex.worker.broker.interface import AsyncBroker

    from .interface import TaskStore

import contextlib

from .config import WorkerConfig
from .exception import WorkerPoolError
from .integration import (
    WorkerPoolHealth,
    WorkerPoolStat,
    WorkerQueueHealth,
    WorkerQueueStat,
)
from .worker import Worker

__all__ = ("WorkerPool",)


class WorkerPool:
    """Worker pool with elastic scaling.

    Manages multiple workers across queues with automatic scaling
    based on queue load.
    """

    def __init__(
        self,
        broker: AsyncBroker,
        base_workers: int = 2,
        max_workers: int = 10,
        queues: list[str] | None = None,
        auto_scale: bool = True,
        config: WorkerConfig | None = None,
        task_store: TaskStore | None = None,
    ):
        """Initialize worker pool.

        Args:
            broker: AsyncBroker instance
            base_workers: Minimum workers per queue
            max_workers: Maximum workers per queue
            queues: List of queues to process
            auto_scale: Enable automatic scaling
            config: Worker configuration
            task_store: Optional task storage
        """
        self.broker = broker
        self.base_workers = base_workers
        self.max_workers = max_workers
        self.queues = queues or ["default"]
        self.auto_scale = auto_scale
        self.config = config or WorkerConfig()
        self.task_store = task_store

        self._workers: dict[str, list[Worker]] = {}
        self._monitor_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Start the worker pool."""
        if self._running:
            return

        self._running = True

        # Initialize workers for each queue
        for queue in self.queues:
            self._workers[queue] = []
            for _ in range(self.base_workers):
                await self._add_worker(queue)

        # Start elastic scaling monitor
        if self.auto_scale:
            self._monitor_task = asyncio.create_task(self._monitor_and_scale())

    async def stop(self) -> None:
        """Stop the worker pool gracefully."""
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
        """Add a worker to the pool.

        Args:
            queue: Queue name
        """
        worker = Worker(
            broker=self.broker,
            queue=queue,
            config=self.config,
            task_store=self.task_store,
        )

        self._workers[queue].append(worker)

        await asyncio.create_task(worker.start())

    async def _remove_worker(self, queue: str) -> bool:
        """Remove a worker from the pool.

        Args:
            queue: Queue name

        Returns:
            True if worker was removed, False otherwise
        """
        if queue not in self._workers or not self._workers[queue]:
            return False

        # Don't remove below base_workers
        if len(self._workers[queue]) <= self.base_workers:
            return False

        worker = self._workers[queue].pop()
        await worker.stop()
        return True

    async def _monitor_and_scale(self) -> None:
        """Monitor queue load and scale workers."""
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds

                for queue in self.queues:
                    # Get queue size
                    try:
                        queue_size = await self.broker.queue_size(queue)
                    except Exception:  # noqa: BLE001, S112
                        continue

                    current_workers = len(self._workers.get(queue, []))

                    # Scale up if queue is overloaded
                    if queue_size > current_workers * 5 and current_workers < self.max_workers:
                        await self._add_worker(queue)

                    # Scale down if queue is empty
                    elif queue_size == 0 and current_workers > self.base_workers:
                        await self._remove_worker(queue)

            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                # Log error and continue
                await asyncio.sleep(1)

    def get_stats(self) -> WorkerPoolStat:
        """Get pool statistics.

        Returns:
            Dictionary of statistics
        """
        stats = WorkerPoolStat()

        for queue, workers in self._workers.items():
            worker_stats = [w.stats for w in workers]

            stats.queues[queue] = WorkerQueueStat(
                worker_count=len(workers),
                worker_stats=worker_stats,
            )

            stats.total_workers += len(workers)

        return stats

    async def health_check(self) -> WorkerPoolHealth:
        """Perform health check on all workers.

        Returns:
            Health status
        """
        if not self._running:
            return WorkerPoolHealth(
                healthy=False,
                error="Worker pool is not running",
            )

        all_healthy = True
        queue_health: dict[str, WorkerQueueHealth] = {}

        for queue, workers in self._workers.items():
            queue_healthy = True
            worker_health = []

            for worker in workers:
                health = worker.health_check()
                worker_health.append(health)
                if not health["healthy"]:
                    queue_healthy = False
                    all_healthy = False

            queue_health[queue] = WorkerQueueHealth(
                queue_healthy,
                worker_health,
            )

        return WorkerPoolHealth(
            healthy=all_healthy,
            queues=queue_health,
            total_workers=sum(len(w) for w in self._workers.values()),
        )

    def get_worker_count(self, queue: str | None = None) -> int:
        """Get worker count for a queue or total.

        Args:
            queue: Queue name (None for total)

        Returns:
            Worker count
        """
        if queue:
            return len(self._workers.get(queue, []))
        return sum(len(workers) for workers in self._workers.values())

    async def scale_to(self, queue: str, target_count: int) -> None:
        """Scale workers to specific count.

        Args:
            queue: Queue name
            target_count: Target worker count
        """
        if not self._running:
            raise WorkerPoolError("Pool not running")

        if target_count < self.base_workers or target_count > self.max_workers:
            raise WorkerPoolError(f"Target count must be between {self.base_workers} and {self.max_workers}")

        current_count = self.get_worker_count(queue)

        if target_count > current_count:
            # Add workers
            for _ in range(target_count - current_count):
                await self._add_worker(queue)
        elif target_count < current_count:
            # Remove workers
            for _ in range(current_count - target_count):
                await self._remove_worker(queue)
