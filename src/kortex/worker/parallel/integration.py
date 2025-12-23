from kortex.base.types import BaseStruct

__all__ = (
    "WorkerHealth",
    "WorkerPoolHealth",
    "WorkerPoolStat",
    "WorkerQueueHealth",
    "WorkerQueueStat",
    "WorkerStat",
)


class WorkerStat(BaseStruct):
    """Worker state."""

    state: str = ""
    processed: int = 0
    failed: int = 0
    running_tasks: int = 0
    last_heartbeat: float = 0.0
    uptime: float = 0.0


class WorkerQueueStat(BaseStruct):
    """Worker queue state."""

    worker_count: int = 0
    worker_stats: list[WorkerStat] = []


class WorkerPoolStat(BaseStruct):
    """Worker pool state."""

    running: bool = False
    auto_scale: bool = False
    queues: dict[str, WorkerQueueStat] = {}
    total_workers: int = 0


class WorkerHealth(BaseStruct):
    """Worker health status."""


class WorkerQueueHealth(BaseStruct):
    """Worker queue health status."""

    healthy: bool = False
    worker_healthy: list[WorkerHealth] = []


class WorkerPoolHealth(BaseStruct):
    """Worker pool health status."""

    healthy: bool = False
    queues: dict[str, WorkerQueueHealth] = {}
    total_workers: int = 0
    error: str | None = None
