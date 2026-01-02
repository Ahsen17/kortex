import asyncio
import contextlib
import multiprocessing as mp
import signal
import sys
from multiprocessing.synchronize import Event
from typing import Any
from uuid import uuid4

import structlog

from .abc import TaskStoreABC
from .broker import AsyncBrokerABC
from .config import WorkerConfig
from .worker import Worker

__all__ = ("WorkerProcess",)


logger = structlog.stdlib.get_logger(__name__)


class WorkerProcess(mp.Process):
    def __init__(
        self,
        broker: AsyncBrokerABC,
        stop_event: Event,
        queue: str = "default",
        store: TaskStoreABC | None = None,
        worker_config: WorkerConfig | None = None,
    ) -> None:
        super().__init__()

        self._process_id = self.gen_process_id()
        self._broker = broker
        self._queue = queue

        self._stop_event = stop_event
        self._store = store
        self._worker_config = worker_config or WorkerConfig()

    @classmethod
    def gen_process_id(cls) -> str:
        return f"worker-{uuid4().hex[-4:]}"

    def run(self) -> None:
        try:
            import setproctitle  # pyright: ignore[reportMissingImports]  # noqa: PLC0415

            setproctitle.setproctitle(self._process_id)
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

        self._stop_event.set()

    async def _worker_main(self) -> None:
        worker = Worker(
            self._broker,
            self._queue,
            self._worker_config,
            self._store,
        )

        try:
            await worker.start()

            while not self._stop_event.is_set():
                await asyncio.sleep(0.1)  # Faster response to shutdown

        except asyncio.CancelledError:
            pass

        except Exception as e:  # noqa: BLE001
            logger.error("Worker process error", error=e)

        finally:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(fut=worker.stop(), timeout=5.0)
