"""Exception hierarchy for parallel task framework."""

from __future__ import annotations

__all__ = (
    "BrokerError",
    "HealthCheckError",
    "ParallelError",
    "ProcessManagerError",
    "SchedulerError",
    "TaskNotFoundError",
    "TaskTimeoutError",
    "TaskValidationError",
    "WorkerPoolError",
)


class ParallelError(Exception):
    """Base exception for all parallel task framework errors."""


class TaskValidationError(ParallelError):
    """Raised when task validation fails."""


class TaskTimeoutError(ParallelError):
    """Raised when task execution times out."""


class TaskNotFoundError(ParallelError):
    """Raised when task is not found."""


class WorkerPoolError(ParallelError):
    """Raised when worker pool encounters an error."""


class ProcessManagerError(ParallelError):
    """Raised when process manager encounters an error."""


class SchedulerError(ParallelError):
    """Raised when scheduler encounters an error."""


class BrokerError(ParallelError):
    """Raised when broker operations fail."""


class HealthCheckError(ParallelError):
    """Raised when health check fails."""
