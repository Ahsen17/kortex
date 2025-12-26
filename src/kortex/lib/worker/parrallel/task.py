import functools
import inspect
from collections.abc import Callable
from typing import Any, Literal, overload
from uuid import UUID

import structlog

from ..broker import Message
from .broker import ParallelBroker
from .config import TaskConfig
from .exception import BrokerError
from .schema import TaskResult
from .worker import TaskRegister

__all__ = (
    "ParallelBrokerNullError",
    "TaskWrapper",
    "task",
)

logger = structlog.stdlib.get_logger(__name__)


class ParallelBrokerNullError(BrokerError):
    """Parallel broker is not set."""

    def __init__(self) -> None:
        super().__init__("Parallel broker is not set")


class TaskWrapper:
    """Wrapper for task functions with delay and result methods."""

    _broker: ParallelBroker | None = None

    def __init__(
        self,
        func: Callable[..., Any],
        config: TaskConfig,
    ) -> None:
        """Initialize task wrapper.

        Args:
            func: Original function
            config: Task configuration
            broker: Parallel broker
        """
        self.func = func
        self.config = config
        self.task_name = f"{func.__module__}.{func.__name__}"

        # Preserve function metadata
        functools.update_wrapper(self, func)

    @classmethod
    def set_parallel_broker(cls, broker: ParallelBroker) -> None:
        """Set the parallel broker for tasks."""

        cls._broker = broker

    @overload
    async def delay(
        self,
        message: Message,
        schedule: Literal["immediate"],
    ) -> None: ...

    @overload
    async def delay(
        self,
        message: Message,
        schedule: Literal["delay"],
        *,
        delay_seconds: int = 0,
    ) -> None: ...

    @overload
    async def delay(
        self,
        message: Message,
        schedule: Literal["cron"],
        *,
        cron: str | None = None,
    ) -> None: ...

    async def delay(
        self,
        message: Message,  # TODO: pass arguments to build message object instead
        schedule: Literal["immediate", "delay", "cron"],
        delay_seconds: int = 0,
        cron: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Submit task for delayed execution.

        Args:
            message: Task message
            schedule: Override schedule mode
            delay_seconds: Override delay
            cron: Override cron expression

            *args: Positional arguments for the task
            **kwargs: Keyword arguments for the task
        """

        if self._broker is None:
            raise ParallelBrokerNullError()

        if not await self._broker.is_connected():
            await self._broker.connect()

        message.name = self.task_name
        message.body = self._build_payload(args, kwargs)

        match schedule:
            case "immediate":
                await self._broker.submit_task(
                    message,
                    schedule="immediate",
                )

            case "delay":
                await self._broker.submit_task(
                    message,
                    schedule="delay",
                    delay_seconds=delay_seconds,
                )

            case "cron":
                await self._broker.submit_task(
                    message,
                    schedule="cron",
                    cron=cron,
                )

    async def result(self, task_id: UUID, timeout: float | None = None) -> TaskResult | None:
        """Get task result.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time (None = no wait)

        Returns:
            Task result or None
        """

        if self._broker is None:
            raise ParallelBrokerNullError()

        if not await self._broker.is_connected():
            await self._broker.connect()

        if timeout:
            return await self._broker.wait_for_task(task_id, timeout=timeout)
        return await self._broker.get_result(task_id)

    async def status(self, task_id: UUID) -> str:
        """Get task status.

        Args:
            task_id: Task identifier

        Returns:
            Task status as string
        """

        if self._broker is None:
            raise ParallelBrokerNullError()

        if not await self._broker.is_connected():
            await self._broker.connect()

        status = await self._broker.get_task_status(task_id)
        return status.value

    async def revoke(self, task_id: UUID, terminate: bool = False) -> bool:
        """Revoke (cancel) a task.

        Args:
            task_id: Task identifier
            terminate: Whether to terminate if running

        Returns:
            True if revoked
        """

        if self._broker is None:
            raise ParallelBrokerNullError()

        if not await self._broker.is_connected():
            await self._broker.connect()

        return await self._broker.revoke_task(task_id, terminate=terminate)

    def _build_payload(self, args: tuple, kwargs: dict) -> dict[str, Any]:
        """Build payload from args and kwargs.

        Args:
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Payload dictionary
        """

        # Get function signature
        sig = inspect.signature(self.func)
        params = sig.parameters

        # Bind arguments
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()

        # Convert to dict
        payload = bound.arguments

        # Handle *args and **kwargs
        for name, param in params.items():
            if param.kind == inspect.Parameter.VAR_POSITIONAL and name in payload:
                # *args - Flatten varargs into list
                payload[name] = list(payload[name])
            if param.kind == inspect.Parameter.VAR_KEYWORD and name in payload:
                # **kwargs - Merge kwargs
                kwargs_dict = payload.pop(name)
                payload.update(kwargs_dict)

        return dict(payload)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the original function directly."""

        return self.func(*args, **kwargs)


@overload
def task(func: Callable[..., Any]) -> TaskWrapper: ...


@overload
def task(**config: Any) -> Callable[[Callable[..., Any]], TaskWrapper]: ...


def task(
    func: Callable[..., Any] | None = None,
    **config: Any,
) -> TaskWrapper | Callable[[Callable[..., Any]], TaskWrapper]:
    """Decorator for defining parallel tasks.

    Can be used with or without configuration:

    Without config:
        >>> @task
        ... async def my_task(x: int):
        ...     return x * 2

    With config:
        >>> @task(queue="high", retry=5, priority=10)
        ... async def my_task(x: int):
        ...     return x * 2

    Args:
        func: Function to decorate
        **config: Task configuration (queue, retry, priority, ttl, timeout, schedule, delay_seconds, cron)

    Returns:
        TaskWrapper or decorator function
    """
    # This will be set when the decorator is applied

    def decorator(f: Callable[..., Any]) -> TaskWrapper:
        # Create task config
        task_config = TaskConfig(**config)

        # Create wrapper
        wrapper = TaskWrapper(f, task_config)

        # Register in global registry
        task_name = f"{f.__module__}.{f.__name__}"
        TaskRegister.register_task(task_name, f)

        return wrapper

    # Handle both @task and @task(...) usage
    if func is None:
        # Called as @task(...)
        return decorator
    # Called as @task
    # Need to set broker - will be done when parallel module is initialized
    # For now, create a lazy wrapper that will be configured later
    TaskConfig(**config)
    # Return a placeholder that will be configured when broker is available
    return decorator(func)
