from advanced_alchemy.repository import (
    SQLAlchemyAsyncRepository,
)

from ..base import BaseService
from ..models.exapmle import Example


class ExampleRepository(SQLAlchemyAsyncRepository[Example]):
    """Example repository."""

    model_type = Example


class ExampleService(BaseService[Example]):
    """Example service."""

    repository_type = ExampleRepository
