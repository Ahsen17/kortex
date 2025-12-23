"""Task decorator and task wrapper for parallel execution."""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Any, overload

from kortex.config import AppConfig

from ..broker.plugin import AsyncValkeyBroker
from .broker import ParallelBroker
from .config import TaskConfig
from .service import TaskService, TaskStoreImpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from .message import TaskResult

__all__ = (
    "Task",
    "task",
)


@functools.cache
def default_parallel_broker() -> ParallelBroker:
    """Get default parallel broker."""

    config = AppConfig.get_config()

    return ParallelBroker(
        underlying_broker=AsyncValkeyBroker(
            host=config.cache.host,
            port=config.cache.port,
            password=config.cache.password,
            db=config.cache.database,
        ),
        task_store=TaskStoreImpl(TaskService),
    )


class TaskWrapper:
    """Wrapper for task functions with delay and result methods."""

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
        self.broker = default_parallel_broker()
        self.task_name = f"{func.__module__}.{func.__name__}"

        # Preserve function metadata
        functools.update_wrapper(self, func)

    async def delay(
        self,
        *args: Any,
        queue: str | None = None,
        schedule: str | None = None,
        delay_seconds: int | None = None,
        cron: str | None = None,
        retry: int | None = None,
        priority: int | None = None,
        ttl: int | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> UUID:
        """Submit task for delayed execution.

        Args:
            *args: Positional arguments for the task
            queue: Override queue name
            schedule: Override schedule mode
            delay_seconds: Override delay
            cron: Override cron expression
            retry: Override retry count
            priority: Override priority
            ttl: Override TTL
            timeout: Override timeout
            **kwargs: Keyword arguments for the task

        Returns:
            Task ID

        Example:
            >>> @task(queue="default", retry=3)
            ... async def send_email(to: str, subject: str):
            ...     # send email
            ...     return {"status": "sent"}
            >>>
            >>> task_id = await send_email.delay(
            ...     "user@example.com", "Hello", schedule="delay", delay_seconds=300
            ... )
        """

        if not await self.broker.is_connected():
            await self.broker.connect()

        # Merge args and kwargs
        payload = self._build_payload(args, kwargs)

        # Use provided values or defaults from config
        q = queue or self.config.queue
        sched = schedule or self.config.schedule
        delay = delay_seconds or self.config.delay_seconds
        cr = cron or self.config.cron
        r = retry if retry is not None else self.config.retry
        p = priority if priority is not None else self.config.priority
        t = ttl if ttl is not None else self.config.ttl
        to = timeout if timeout is not None else self.config.timeout

        return await self.broker.submit_task(
            task_name=self.task_name,
            payload=payload,
            queue=q,
            schedule=sched,
            delay_seconds=delay,
            cron=cr,
            retry=r,
            priority=p,
            ttl=t,
            timeout=to,
        )

    async def result(self, task_id: UUID, timeout: float | None = None) -> TaskResult | None:
        """Get task result.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time (None = no wait)

        Returns:
            Task result or None
        """

        if not await self.broker.is_connected():
            await self.broker.connect()

        if timeout:
            return await self.broker.wait_for_task(task_id, timeout=timeout)
        return await self.broker.get_result(task_id)

    async def status(self, task_id: UUID) -> str:
        """Get task status.

        Args:
            task_id: Task identifier

        Returns:
            Task status as string
        """

        if not await self.broker.is_connected():
            await self.broker.connect()

        status = await self.broker.get_task_status(task_id)
        return status.value

    async def revoke(self, task_id: UUID, terminate: bool = False) -> bool:
        """Revoke (cancel) a task.

        Args:
            task_id: Task identifier
            terminate: Whether to terminate if running

        Returns:
            True if revoked
        """

        if not await self.broker.is_connected():
            await self.broker.connect()

        return await self.broker.revoke_task(task_id, terminate=terminate)

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
        from .worker import register_task  # noqa: PLC0415

        task_name = f"{f.__module__}.{f.__name__}"
        register_task(task_name, f)

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


class Task:
    """Base class for task definitions (alternative to decorator)."""

    queue: str = "default"
    retry: int = 3
    priority: int = 0
    ttl: int | None = None
    timeout: int | None = None
    schedule: str = "immediate"
    delay_seconds: int = 0
    cron: str | None = None

    _broker: ParallelBroker = default_parallel_broker()

    @classmethod
    def set_broker(cls, broker: ParallelBroker) -> None:
        """Set broker for task class.

        Args:
            broker: Parallel broker
            task_store: Optional task store
        """
        cls._broker = broker

    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        """Execute the task.

        Args:
            **kwargs: Task arguments

        Returns:
            Task result
        """
        raise NotImplementedError("Subclasses must implement execute()")

    async def delay(self, **kwargs: Any) -> UUID:
        """Submit task for execution.

        Args:
            **kwargs: Task arguments

        Returns:
            Task ID
        """

        if not self._broker.is_connected():
            await self._broker.connect()

        task_name = f"{self.__class__.__module__}.{self.__class__.__name__}"

        return await self._broker.submit_task(
            task_name=task_name,
            payload=kwargs,
            queue=self.queue,
            schedule=self.schedule,
            delay_seconds=self.delay_seconds,
            cron=self.cron,
            retry=self.retry,
            priority=self.priority,
            ttl=self.ttl,
            timeout=self.timeout,
        )

    async def result(self, task_id: UUID, timeout: float | None = None) -> TaskResult | None:
        """Get task result.

        Args:
            task_id: Task identifier
            timeout: Maximum wait time

        Returns:
            Task result
        """

        if not self._broker.is_connected():
            await self._broker.connect()

        if timeout:
            return await self._broker.wait_for_task(task_id, timeout=timeout)
        return await self._broker.get_result(task_id)

    async def status(self, task_id: UUID) -> str:
        """Get task status.

        Args:
            task_id: Task identifier

        Returns:
            Task status
        """

        if not self._broker.is_connected():
            await self._broker.connect()

        status = await self._broker.get_task_status(task_id)
        return status.value

    async def revoke(self, task_id: UUID, terminate: bool = False) -> bool:
        """Revoke task.

        Args:
            task_id: Task identifier
            terminate: Whether to terminate if running

        Returns:
            True if revoked
        """

        if not self._broker.is_connected():
            await self._broker.connect()

        return await self._broker.revoke_task(task_id, terminate=terminate)
