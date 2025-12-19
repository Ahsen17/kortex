import functools
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Self

from advanced_alchemy.config.asyncio import (
    AlembicAsyncConfig,
    AsyncSessionConfig,
)
from advanced_alchemy.extensions.fastapi.config import SQLAlchemyAsyncConfig
from advanced_alchemy.repository.typing import ModelT
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryReadService

if TYPE_CHECKING:
    from advanced_alchemy.repository import LoadSpec
    from sqlalchemy.ext.asyncio import AsyncSession

from ..config import AppConfig

config = AppConfig.get_config()


@functools.lru_cache(maxsize=1, typed=True)
def alchemy_config() -> SQLAlchemyAsyncConfig:
    return SQLAlchemyAsyncConfig(
        engine_instance=config.db.engine,
        commit_mode="autocommit",
        session_config=AsyncSessionConfig(expire_on_commit=False),
        alembic_config=AlembicAsyncConfig(
            version_table_name=config.db.migration_ddl_version_table,
            script_config=config.db.migration_config,
            script_location=config.db.migration_path,
        ),
    )


class BaseService(SQLAlchemyAsyncRepositoryReadService[ModelT]):
    """Base service."""

    @property
    def session(self) -> "AsyncSession":
        return self.repository.session  # type: ignore

    @classmethod
    @asynccontextmanager
    async def session_scope(
        cls,
        session: "AsyncSession | None" = None,
        load: "LoadSpec | None" = None,
        **kwargs: Any,
    ) -> AsyncGenerator[Self, None]:
        async with cls.new(
            session=session,
            config=None,
            load=load,
            **kwargs,
        ) as service:
            yield service
