"""Parallel task execution framework.

A comprehensive background task processing framework built on top of the
Kortex broker system. Supports multi-queue processing, elastic scaling,
delayed/cron scheduling, and multi-process execution.

Key Features:
- Decorator-based task definition
- Multi-queue support with elastic Worker scaling
- Three scheduling modes: immediate, delay, cron
- Multi-process execution (spawn mode)
- Health checks and lifecycle management
- FastAPI lifespan integration
- Database-backed task storage

Example:
    >>> from kortex.worker.parallel import task, ParallelBroker
    >>> from kortex.worker.broker.plugin.valkey import AsyncValkeyBroker
    >>>
    >>> @task(queue="default", retry=3)
    >>> async def send_email(to: str, subject: str):
    ...     # Send email logic
    ...     return {"status": "sent"}
    >>>
    >>> # Submit task
    >>> task_id = await send_email.delay("user@example.com", "Hello")
    >>>
    >>> # Get result
    >>> result = await send_email.result(task_id)
"""

from .broker import ParallelBroker
from .config import (
    ParallelConfig,
    ProcessConfig,
    SchedulerConfig,
    TaskConfig,
    WorkerConfig,
)
from .enum import ProcessState, SchedulerState, TaskStatus, WorkerState
from .exception import (
    BrokerError,
    HealthCheckError,
    ParallelError,
    ProcessManagerError,
    SchedulerError,
    TaskNotFoundError,
    TaskTimeoutError,
    TaskValidationError,
    WorkerPoolError,
)
from .health import (
    HealthCheckService,
    ProcessLifecycleManager,
    create_health_router,
    lifespan,
)
from .manager import ProcessManager, WorkerProcess
from .message import TaskMessage, TaskResult
from .model import TaskModel
from .pool import WorkerPool
from .scheduler import Scheduler
from .service import TaskService, TaskStoreImpl
from .task import Task, task
from .worker import Worker

__all__ = (
    # Exceptions
    "BrokerError",
    "HealthCheckError",
    # Health & Lifecycle
    "HealthCheckService",
    # Core classes
    "ParallelBroker",
    # Configuration
    "ParallelConfig",
    "ParallelError",
    "ProcessConfig",
    "ProcessLifecycleManager",
    "ProcessManager",
    "ProcessManagerError",
    "ProcessState",
    "Scheduler",
    "SchedulerConfig",
    "SchedulerError",
    "SchedulerState",
    "Task",
    "TaskConfig",
    "TaskMessage",
    # Database
    "TaskModel",
    "TaskNotFoundError",
    "TaskResult",
    "TaskService",
    "TaskStatus",
    "TaskStoreImpl",
    "TaskTimeoutError",
    "TaskValidationError",
    # Worker
    "Worker",
    "WorkerConfig",
    "WorkerPool",
    "WorkerPoolError",
    "WorkerProcess",
    "WorkerState",
    # Utilities
    "create_health_router",
    "lifespan",
    # Decorator
    "task",
)
