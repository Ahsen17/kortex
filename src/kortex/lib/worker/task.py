from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, overload

from .broker.constant import DEFAULT_TTL
from .exception import ParallelTaskDuplicatedError, ParallelTaskNotFoundError

__all__ = (
    "TaskCell",
    "get_task",
    "register_task",
    "task",
)


@dataclass
class TaskCell:
    func: Callable[..., Any]
    schedule: Literal["immediate", "delay", "cron"] = "immediate"
    queue: str = "default"
    key: str | None = None
    priority: int | None = None
    ttl: float = DEFAULT_TTL
    meta: dict[str, Any] | None = None
    delay_seconds: int = 0
    cron: str | None = None


class Wrapper:
    _tasks: ClassVar[dict[str, TaskCell]] = {}

    @classmethod
    def register(
        cls,
        task_name: str,
        func: Callable[..., Any],
        **kwargs: Any,
    ) -> None:
        if task_name not in cls._tasks:
            cls._tasks[task_name] = TaskCell(
                func=func,
                schedule=kwargs.get("schedule", "immediate"),
                queue=kwargs.get("queue", "default"),
                key=kwargs.get("key"),
                priority=kwargs.get("priority"),
                ttl=kwargs.get("ttl", DEFAULT_TTL),
                meta=kwargs.get("meta"),
                delay_seconds=kwargs.get("delay_seconds", 0),
                cron=kwargs.get("cron"),
            )
        else:
            raise ParallelTaskDuplicatedError(
                f"Task {task_name} duplicated.",
            )

    @classmethod
    def get(cls, task_name: str) -> TaskCell:
        if task_name not in cls._tasks:
            raise ParallelTaskNotFoundError(
                f"Task {task_name} not found.",
            )

        return cls._tasks[task_name]


def register_task(
    name: str,
    func: Callable[..., Any],
    **kwargs: Any,
) -> None:
    Wrapper.register(name, func, **kwargs)


def get_task(task_name: str) -> TaskCell:
    return Wrapper.get(task_name)


@overload
def task(
    name: str,
    schedule: Literal["immediate"],
    *,
    queue: str | None = None,
    key: str | None = None,
    priority: int | None = None,
    ttl: float | None = None,
    meta: dict[str, Any] | None = None,
) -> Callable[..., None]: ...


@overload
def task(
    name: str,
    schedule: Literal["delay"],
    *,
    delay_seconds: int,
    queue: str | None = None,
    key: str | None = None,
    priority: int | None = None,
    ttl: float | None = None,
    meta: dict[str, Any] | None = None,
) -> Callable[..., None]: ...


@overload
def task(
    name: str,
    schedule: Literal["cron"],
    *,
    cron: str,
    queue: str | None = None,
    key: str | None = None,
    priority: int | None = None,
    ttl: float | None = None,
    meta: dict[str, Any] | None = None,
) -> Callable[..., None]: ...


def task(
    name: str,
    schedule: Literal["immediate", "delay", "cron"],
    queue: str | None = None,
    key: str | None = None,
    priority: int | None = None,
    ttl: float | None = None,
    meta: dict[str, Any] | None = None,
    delay_seconds: int = 0,
    cron: str | None = None,
) -> Callable[..., None]:
    def decorator(func: Callable[..., Any]) -> None:
        Wrapper.register(
            name,
            func=func,
            schedule=schedule,
            queue=queue,
            key=key,
            priority=priority,
            ttl=ttl,
            meta=meta,
            delay_seconds=delay_seconds,
            cron=cron,
        )

    return decorator
