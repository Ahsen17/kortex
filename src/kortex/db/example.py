import sqlalchemy as sa
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from sqlalchemy.orm import Mapped, mapped_column

from .base import AuditMixin, BaseService


class Example(AuditMixin):
    """Example model."""

    __tablename__ = "example"

    name: Mapped[str] = mapped_column(sa.String, unique=True)


class ExampleRepository(SQLAlchemyAsyncRepository[Example]):
    """Example repository."""

    model_type = Example


class ExampleService(BaseService[Example]):
    """Example service."""

    repository_type = ExampleRepository
