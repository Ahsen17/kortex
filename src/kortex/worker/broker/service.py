from advanced_alchemy.repository import SQLAlchemyAsyncRepository

from kortex.db import BaseService

from .model import MessageModel

__all__ = ("MessageService",)


class MessageRepository(SQLAlchemyAsyncRepository[MessageModel]):
    """Message repository."""

    model_type = MessageModel


class MessageService(BaseService[MessageModel]):
    """Message database service."""

    repository_type = MessageRepository
