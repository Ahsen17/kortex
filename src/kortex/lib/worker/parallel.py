import contextlib
import multiprocessing as mp
import os
from typing import TYPE_CHECKING, Literal

import structlog

from .abc import TaskStoreABC
from .broker import AsyncBrokerABC
from .config import WorkerConfig
from .process import WorkerProcess

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

__all__ = ("Parallel",)

logger = structlog.stdlib.get_logger(__name__)


class Parallel:
    def __init__(
        self,
        broker: AsyncBrokerABC,
        num_processes: int = 1,
        queues: list[str] | None = None,
        boot_method: Literal["spawn", "fork", "forkserver"] = "spawn",
        store: TaskStoreABC | None = None,
        worker_config: WorkerConfig | None = None,
    ) -> None:
        self._num_processes = num_processes

        self._broker = broker
        self._queues = queues or ["default"]

        self._store = store
        self._worker_config = worker_config or WorkerConfig()

        self._ctx = mp.get_context(boot_method)
        self._processes: list[WorkerProcess] = []
        self._stop_events: list[Event] = []

    def start(self) -> None:
        logger.info("Starting parallel...")

        if self._processes:
            raise RuntimeError("Processes already running")

        if self._num_processes <= 0:
            raise ValueError("Number of processes must be greater than 0")

        for i in range(self._num_processes):
            # Distribute queues round-robin
            queue = self._queues[i % len(self._queues)]

            # Create stop event
            stop_event = self._ctx.Event()

            # Create process
            process = WorkerProcess(
                broker=self._broker,
                stop_event=stop_event,
                queue=queue,
                store=self._store,
                worker_config=self._worker_config,
            )

            # Start process
            process.start()
            self._processes.append(process)
            self._stop_events.append(stop_event)

        # TODO: cannot stop normally

    def stop(self, graceful: bool = True) -> None:
        logger.info("Stopping parallel...")

        if not self._processes:
            return

        # Signal all processes to stop
        for event in self._stop_events:
            event.set()

        if graceful:
            # Wait for graceful shutdown
            for _, process in enumerate(self._processes):
                try:
                    process.join()

                    if process.is_alive():
                        process.terminate()
                        process.join(timeout=5)

                        if process.is_alive():
                            process.kill()
                            process.join()

                except Exception:  # noqa: BLE001, S110
                    pass
        else:
            # Force stop
            for process in self._processes:
                process.terminate()
                process.join()

        self._processes.clear()
        self._stop_events.clear()

    def is_running(self) -> bool:
        return any(p.is_alive() for p in self._processes)

    def get_pids(self) -> list[int]:
        return [p.pid for p in self._processes if p.pid is not None]

    def send_signal(self, signum: int) -> None:
        for process in self._processes:
            if process.pid:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(process.pid, signum)
