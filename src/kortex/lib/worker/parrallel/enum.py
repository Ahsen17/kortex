"""Enumeration types for parallel task framework."""

from enum import StrEnum, auto

__all__ = (
    "ProcessState",
    "SchedulerState",
    "TaskStatus",
    "WorkerState",
)


class TaskStatus(StrEnum):
    """Task lifecycle status."""

    PENDING = auto()
    """Task created, waiting to be queued"""

    QUEUED = auto()
    """Task in queue, waiting for worker"""

    PROCESSING = auto()
    """Task being executed"""

    SUCCESS = auto()
    """Task completed successfully"""

    FAILED = auto()
    """Task failed permanently"""

    RETRYING = auto()
    """Task failed but will retry"""

    REVOKED = auto()
    """Task was cancelled"""

    EXPIRED = auto()
    """Task TTL expired"""


class WorkerState(StrEnum):
    """Worker operational state."""

    IDLE = auto()
    """Worker waiting for tasks"""

    RUNNING = auto()
    """Worker executing tasks"""

    STOPPING = auto()
    """Worker shutting down"""

    STOPPED = auto()
    """Worker stopped"""

    HEALTHY = auto()
    """Worker health check passed"""

    UNHEALTHY = auto()
    """Worker health check failed"""


class SchedulerState(StrEnum):
    """Scheduler operational state."""

    RUNNING = auto()
    """Scheduler active"""

    PAUSED = auto()
    """Scheduler paused"""

    STOPPED = auto()
    """Scheduler stopped"""


class ProcessState(StrEnum):
    """Process lifecycle state."""

    STARTING = auto()
    """Process starting"""

    RUNNING = auto()
    """Process running"""

    STOPPING = auto()
    """Process stopping"""

    STOPPED = auto()
    """Process stopped"""

    CRASHED = auto()
    """Process crashed unexpectedly"""
