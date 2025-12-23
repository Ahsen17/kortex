"""Database service for task storage."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from advanced_alchemy.repository import SQLAlchemyAsyncRepository

from kortex.db import BaseService

from .interface import TaskStore
from .message import TaskMessage, TaskResult
from .model import TaskModel

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

__all__ = ("TaskService",)


class TaskRepository(SQLAlchemyAsyncRepository[TaskModel]):
    """Repository for task execution records."""

    model_type = TaskModel


class TaskService(BaseService[TaskModel]):
    """Task storage service using database."""

    repository_type = TaskRepository


class TaskStoreImpl[T: BaseService](TaskStore):
    """Task storage implementation using database"""

    def __init__(self, ser_cls: type[T]) -> None:
        self.service_cls = ser_cls

    @asynccontextmanager
    async def provide(self) -> AsyncGenerator[T]:
        async with self.service_cls.session_scope() as service:
            yield service

    async def save(self, message: TaskMessage) -> None:
        async with self.provide() as service:
            await service.upsert(data=message.to_dict())

    async def update(self, task_id: UUID, **kwargs: Any) -> None:
        async with self.provide() as service:
            await service.update(data=kwargs, item_id=task_id)

    async def get(self, task_id: UUID) -> TaskMessage | None:
        async with self.provide() as service:
            model = await service.get_one_or_none(TaskModel.id == task_id)

        if model is None:
            return None

        return TaskMessage.from_dict(model.to_dict())

    async def get_result(self, task_id: UUID) -> TaskResult | None:
        async with self.provide() as service:
            model = await service.get_one_or_none(TaskModel.id == task_id)

        if model is None:
            return None

        return TaskResult(
            task_id=model.id,
            status=model.status,
            result=model.result,
            error=model.error,
            executed_at=model.executed_at,
            completed_at=model.completed_at,
        )

    async def delete(self, task_id: UUID) -> None:
        async with self.provide() as service:
            await service.delete(item_id=task_id)
