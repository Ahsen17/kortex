from dataclasses import dataclass, field

from .enum import WorkerState

__all__ = (
    "WorkerHealth",
    "WorkerPoolHealth",
    "WorkerPoolStat",
    "WorkerQueueHealth",
    "WorkerQueueStat",
    "WorkerStat",
)


@dataclass
class WorkerStat:
    """Worker state."""

    state: str = ""
    processed: int = 0
    failed: int = 0
    running_tasks: int = 0
    last_heartbeat: float = 0.0
    uptime: float = 0.0


@dataclass
class WorkerQueueStat:
    """Worker queue state."""

    worker_count: int = 0
    worker_stats: list[WorkerStat] = field(default_factory=list)


@dataclass
class WorkerPoolStat:
    """Worker pool state."""

    running: bool = False
    auto_scale: bool = False
    queues: dict[str, WorkerQueueStat] = field(default_factory=dict)
    total_workers: int = 0


@dataclass
class WorkerHealth:
    """Worker health status."""

    healthy: bool = False
    state: WorkerState = WorkerState.UNHEALTHY
    processed: int = 0
    failed: int = 0
    running_tasks: int = 0
    last_heartbeat: float = 0.0


@dataclass
class WorkerQueueHealth:
    """Worker queue health status."""

    healthy: bool = False
    worker_healthy: list[WorkerHealth] = field(default_factory=list)


@dataclass
class WorkerPoolHealth:
    """Worker pool health status."""

    healthy: bool = False
    queues: dict[str, WorkerQueueHealth] = field(default_factory=dict)
    total_workers: int = 0
    error: str | None = None
