"""Multi-process worker manager using spawn mode."""

from __future__ import annotations

import asyncio
import contextlib
import multiprocessing as mp
import os
import signal
import sys
import time
from multiprocessing import Process
from typing import TYPE_CHECKING, Any

from ..broker.plugin import AsyncValkeyBroker
from .config import ProcessConfig
from .exception import ProcessManagerError
from .pool import WorkerPool

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

__all__ = ("ProcessManager", "WorkerProcess")


class WorkerProcess(Process):
    """Worker process for parallel task execution.

    Runs in a separate process using spawn mode.
    """

    def __init__(
        self,
        process_id: int,
        broker_config: dict[str, Any],
        queue: str,
        stop_event: Event,
        config: ProcessConfig,
    ) -> None:
        """Initialize worker process.

        Args:
            process_id: Process identifier
            broker_config: Broker connection configuration
            queue: Queue name to process
            stop_event: Event to signal process stop
            config: Process configuration
        """
        super().__init__(name=f"Worker-{process_id}", daemon=False)
        self.process_id = process_id
        self.broker_config = broker_config
        self.queue = queue
        self.stop_event = stop_event
        self.config = config

    def run(self) -> None:
        """Process entry point."""
        # Set process title
        try:
            import setproctitle  # pyright: ignore[reportMissingImports]  # noqa: PLC0415

            setproctitle.setproctitle(f"kortex-worker-{self.process_id}")
        except ImportError:
            pass

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Run async main
        try:
            asyncio.run(self._worker_main())
        except KeyboardInterrupt:
            pass
        except Exception:  # noqa: BLE001
            sys.exit(1)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle termination signals."""
        self.stop_event.set()

    async def _worker_main(self) -> None:
        """Main worker logic."""
        # Import here to avoid issues with multiprocessing

        # Create broker
        broker: AsyncValkeyBroker = AsyncValkeyBroker(**self.broker_config)

        # Connect
        try:
            await broker.connect()
        except Exception:  # noqa: BLE001
            return

        # Create worker pool
        pool = WorkerPool(
            broker=broker,
            base_workers=1,
            max_workers=2,
            queues=[self.queue],
            auto_scale=False,  # Disable auto-scale in individual processes
        )

        # Start pool
        try:
            await pool.start()
        except Exception:  # noqa: BLE001
            await broker.disconnect()
            return

        # Main loop - wait for stop event
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            # Graceful shutdown

            # Stop pool
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(pool.stop(), timeout=self.config.graceful_timeout)

            # Disconnect broker
            with contextlib.suppress(Exception):
                await broker.disconnect()


class ProcessManager:
    """Multi-process worker manager.

    Manages multiple worker processes using spawn mode.
    Provides lifecycle management.
    """

    def __init__(
        self,
        num_processes: int = 2,
        broker_config: dict[str, Any] | None = None,
        queues: list[str] | None = None,
        config: ProcessConfig | None = None,
    ) -> None:
        """Initialize process manager.

        Args:
            num_processes: Number of worker processes to spawn
            broker_config: Broker connection configuration
            queues: List of queues to distribute among processes
            config: Process configuration
        """
        self.num_processes = num_processes
        self.broker_config = broker_config or {"host": "localhost", "port": 6379}
        self.queues = queues or ["default"]
        self.config = config or ProcessConfig()

        self.processes: list[WorkerProcess] = []
        self.stop_events: list[Event] = []  # pyright: ignore[reportInvalidTypeForm]
        self._ctx = mp.get_context("spawn")

    def start(self) -> None:
        """Start worker processes."""
        if self.processes:
            raise ProcessManagerError("Processes already running")

        for i in range(self.num_processes):
            # Distribute queues round-robin
            queue = self.queues[i % len(self.queues)]

            # Create stop event
            stop_event = self._ctx.Event()

            # Create process
            process = WorkerProcess(
                process_id=i,
                broker_config=self.broker_config,
                queue=queue,
                stop_event=stop_event,
                config=self.config,
            )

            # Start process
            process.start()
            self.processes.append(process)
            self.stop_events.append(stop_event)

    def stop(self, graceful: bool = True) -> None:
        """Stop worker processes.

        Args:
            graceful: If True, wait for graceful shutdown
        """
        if not self.processes:
            return

        # Signal all processes to stop
        for event in self.stop_events:
            event.set()

        if graceful:
            # Wait for graceful shutdown
            for _i, process in enumerate(self.processes):
                try:
                    process.join(timeout=self.config.graceful_timeout)
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
            for process in self.processes:
                process.terminate()
                process.join()

        self.processes.clear()
        self.stop_events.clear()

    def restart(self) -> None:
        """Restart worker processes."""
        self.stop()
        time.sleep(1)
        self.start()

    def get_status(self) -> dict[str, Any]:
        """Get status of all processes.

        Returns:
            Dictionary of process statuses
        """
        status = {
            "num_processes": self.num_processes,
            "running": len(self.processes),
            "processes": [],
        }

        for i, process in enumerate(self.processes):
            proc_status = {
                "id": i,
                "pid": process.pid,
                "alive": process.is_alive(),
                "exitcode": process.exitcode if not process.is_alive() else None,
            }
            status["processes"].append(proc_status)  # type: ignore

        return status

    def is_running(self) -> bool:
        """Check if any processes are running.

        Returns:
            True if at least one process is running
        """
        return any(p.is_alive() for p in self.processes)

    def get_pids(self) -> list[int]:
        """Get list of process PIDs.

        Returns:
            List of PIDs
        """
        return [p.pid for p in self.processes if p.pid is not None]

    def send_signal(self, signum: int) -> None:
        """Send signal to all processes.

        Args:
            signum: Signal number
        """
        for process in self.processes:
            if process.pid:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(process.pid, signum)
