"""Health check and lifecycle management for parallel task framework."""

from __future__ import annotations

import asyncio
import os
import resource
import signal
import sys
import time
from typing import TYPE_CHECKING, Any

import pypsutil as psutil
import setproctitle

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi.routing import APIRouter

__all__ = (
    "HealthCheckService",
    "ProcessLifecycleManager",
    "create_health_router",
    "lifespan",
)


class HealthCheckService:
    """Health check service for worker processes.

    Provides metrics tracking and health status.
    Can be mounted to FastAPI router or used standalone.
    """

    def __init__(self) -> None:
        """Initialize health check service."""
        self._start_time = time.time()
        self._metrics: dict[str, Any] = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_retrying": 0,
        }
        self._ready = False

    def update_metrics(self, **kwargs: Any) -> None:
        """Update metrics.

        Args:
            **kwargs: Metrics to update
        """
        self._metrics.update(kwargs)

    def set_ready(self, ready: bool = True) -> None:
        """Set ready status.

        Args:
            ready: Ready state
        """
        self._ready = ready

    def get_health(self) -> dict[str, Any]:
        """Get health status.

        Returns:
            Health status dictionary
        """
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "uptime": time.time() - self._start_time,
        }

    def get_ready(self) -> dict[str, Any]:
        """Get readiness status.

        Returns:
            Readiness status dictionary
        """
        return {
            "status": "ready" if self._ready else "not_ready",
            "metrics": self._metrics,
        }

    def get_metrics(self) -> dict[str, Any]:
        """Get metrics.

        Returns:
            Metrics dictionary
        """
        return {
            "uptime": time.time() - self._start_time,
            "timestamp": time.time(),
            "tasks": self._metrics,
        }

    def get_liveness(self) -> dict[str, Any]:
        """Get liveness status.

        Returns:
            Liveness status dictionary
        """
        return {"alive": True}

    def get_startup(self) -> dict[str, Any]:
        """Get startup status.

        Returns:
            Startup status dictionary
        """
        return {"started": True}


def create_health_router(service: HealthCheckService | None = None) -> APIRouter:
    """Create FastAPI router for health check endpoints.

    Args:
        service: HealthCheckService instance (optional, creates new if None)

    Returns:
        FastAPI router with health check endpoints
    """
    from fastapi import APIRouter  # noqa: PLC0415

    router = APIRouter(prefix="/backend")
    svc = service or HealthCheckService()

    @router.get("/health")
    async def health_handler() -> dict[str, Any]:
        return svc.get_health()

    @router.get("/ready")
    async def ready_handler() -> dict[str, Any]:
        return svc.get_ready()

    @router.get("/metrics")
    async def metrics_handler() -> dict[str, Any]:
        return svc.get_metrics()

    @router.get("/live")
    async def liveness_handler() -> dict[str, Any]:
        return svc.get_liveness()

    @router.get("/startup")
    async def startup_handler() -> dict[str, Any]:
        return svc.get_startup()

    return router


class ProcessLifecycleManager:
    """Process lifecycle and signal management."""

    @staticmethod
    def register_signals() -> None:
        """Register signal handlers for graceful shutdown."""

        signal.signal(signal.SIGTERM, ProcessLifecycleManager._graceful_shutdown)
        signal.signal(signal.SIGINT, ProcessLifecycleManager._graceful_shutdown)
        signal.signal(signal.SIGUSR1, ProcessLifecycleManager._handle_user_signal)

    @staticmethod
    def _graceful_shutdown(signum: int, frame: Any) -> None:
        """Handle graceful shutdown signals."""

        # This will be caught by the main loop
        sys.exit(0)

    @staticmethod
    def _handle_user_signal(signum: int, frame: Any) -> None:
        """Handle user-defined signals (e.g., for stats dump)."""

    @staticmethod
    def set_process_title(name: str) -> None:
        """Set process title for better visibility.

        Args:
            name: Process title
        """

        setproctitle.setproctitle(name)

    @staticmethod
    def get_process_info() -> dict[str, Any]:
        """Get current process information.

        Returns:
            Process information dictionary
        """

        pid = os.getpid()
        rusage = resource.getrusage(resource.RUSAGE_SELF)

        return {
            "pid": pid,
            "ppid": os.getppid(),
            "cwd": os.getcwd(),  # noqa: PTH109
            "max_fd": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
            "cpu_time": rusage.ru_utime + rusage.ru_stime,
            "memory_mb": rusage.ru_maxrss / 1024,  # Convert to MB
        }

    @staticmethod
    async def wait_for_children(timeout: float = 30.0) -> bool:
        """Wait for child processes to finish.

        Args:
            timeout: Maximum wait time

        Returns:
            True if all children finished, False if timeout
        """

        try:
            current = psutil.Process()
            children = current.children(recursive=True)

            if not children:
                return True

            # Wait for children
            start_time = time.time()
            while time.time() - start_time < timeout:
                children = [c for c in children if c.is_running()]
                if not children:
                    return True
                await asyncio.sleep(0.5)

            return False
        except (ImportError, psutil.NoSuchProcess):
            return True


# Lifespan for FastAPI integration
from contextlib import asynccontextmanager  # noqa: E402


@asynccontextmanager
async def lifespan(
    app: Any,
    broker: Any | None = None,
    worker_pool: Any | None = None,
    process_manager: Any | None = None,
    health_service: HealthCheckService | None = None,
    start_workers: bool = True,
    start_processes: bool = False,
) -> AsyncGenerator[None, None]:
    """FastAPI lifespan for parallel task framework.

    Args:
        app: FastAPI application
        broker: Parallel broker instance (optional)
        worker_pool: Worker pool instance (optional)
        process_manager: Process manager instance (optional)
        health_service: Health check service instance (optional)
        start_workers: Whether to start worker pool
        start_processes: Whether to start multi-process manager

    Yields:
        None
    """
    # Startup

    # Store in app state
    app.state.task_broker = broker
    app.state.worker_pool = worker_pool
    app.state.process_manager = process_manager
    app.state.health_service = health_service

    # Start components
    try:
        if broker:
            await broker.connect()
            if health_service:
                health_service.set_ready(True)

        if worker_pool and start_workers:
            await worker_pool.start()

        if process_manager and start_processes:
            process_manager.start()

    except Exception:
        raise

    yield  # FastAPI handles requests

    # Shutdown

    try:
        if process_manager:
            process_manager.stop()

        if worker_pool:
            await worker_pool.stop()

        if broker:
            await broker.disconnect()
            if health_service:
                health_service.set_ready(False)

    except Exception:  # noqa: BLE001, S110
        pass
