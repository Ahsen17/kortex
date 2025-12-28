__all__ = (
    "ParallelError",
    "ParallelTaskDuplicatedError",
    "ParallelTaskNotFoundError",
)


class ParallelError(Exception):
    """Parallel error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ParallelTaskDuplicatedError(ParallelError):
    """Parallel task duplicated error."""


class ParallelTaskNotFoundError(ParallelError):
    """Parallel task not found error."""


class SchedulerError(ParallelError):
    """Scheduler error."""
