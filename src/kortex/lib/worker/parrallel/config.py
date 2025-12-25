"""Configuration classes for parallel task framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

__all__ = (
    "ParallelConfig",
    "ProcessConfig",
    "SchedulerConfig",
    "TaskConfig",
    "WorkerConfig",
)


@dataclass
class WorkerConfig:
    """Worker configuration."""

    base_workers: int = 2
    """Minimum number of workers per queue"""

    max_workers: int = 10
    """Maximum number of workers per queue"""

    auto_scale: bool = True
    """Enable automatic scaling based on queue load"""

    queue_timeout: int = 5
    """Timeout for consuming from queue (seconds)"""

    task_timeout: int = 300
    """Maximum time for task execution (seconds)"""

    concurrency: int = 1
    """Number of concurrent tasks per worker"""

    heartbeat_interval: int = 30
    """Interval for worker heartbeat (seconds)"""


@dataclass
class ProcessConfig:
    """Multi-process configuration."""

    num_processes: int = 2
    """Number of worker processes to spawn"""

    health_check_port: int = 8080
    """Base port for health check servers"""

    spawn_method: str = "spawn"
    """Process spawn method (fixed to spawn)"""

    graceful_timeout: int = 30
    """Graceful shutdown timeout (seconds)"""


@dataclass
class SchedulerConfig:
    """Scheduler configuration."""

    max_scheduled_tasks: int = 1000
    """Maximum number of scheduled tasks in memory"""

    cron_check_interval: int = 60
    """Interval to check cron schedules (seconds)"""

    delay_check_interval: int = 10
    """Interval to check delayed tasks (seconds)"""

    max_concurrent_schedules: int = 100
    """Maximum concurrent scheduled tasks"""


@dataclass
class TaskConfig:
    """Task-specific configuration."""

    queue: str = "default"
    """Target queue name"""

    retry: int = 3
    """Maximum retry attempts"""

    priority: int = 0
    """Task priority (higher = more urgent)"""

    ttl: int | None = None
    """Time-to-live in seconds"""

    timeout: int | None = None
    """Task execution timeout in seconds"""

    schedule: str = "immediate"
    """Schedule mode: immediate, delay, cron"""

    delay_seconds: int = 0
    """Delay before execution (seconds)"""

    cron: str | None = None
    """Cron expression for scheduled tasks"""

    retry_policy: dict[str, Any] | None = None
    """Custom retry policy configuration"""


@dataclass
class ParallelConfig:
    """Main configuration for parallel task framework."""

    broker: dict[str, Any] = field(default_factory=lambda: {"host": "localhost", "port": 6379})
    """Broker connection configuration"""

    worker: WorkerConfig = field(default_factory=WorkerConfig)
    """Worker configuration"""

    process: ProcessConfig = field(default_factory=ProcessConfig)
    """Process configuration"""

    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    """Scheduler configuration"""

    queues: list[str] = field(default_factory=lambda: ["default"])
    """List of queues to process"""

    database: dict[str, Any] = field(default_factory=dict)
    """Database configuration for task storage"""
