from pathlib import Path
from typing import Any, ClassVar, Final, Self

from msgspec import field, json, toml, yaml
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from .base.types import BaseStruct
from .lib.toolkit import get_env
from .schemas import OpenaiProvider

APP_NAME: Final[str] = "kortex"

ROOT_DIR: Final[Path] = Path.cwd()
BASE_DIR: Final[Path] = ROOT_DIR / f"src/{APP_NAME}"


def encode_json(a: Any) -> bytes:
    return json.encode(a)


def decode_json(a: bytes) -> Any:
    return json.decode(a)


class ServerConfig(BaseStruct):
    """Configuration for the Kortex server."""

    host: str = field(default="127.0.0.1")
    port: int = field(default=8000)
    debug: bool = field(default=True)
    reload: bool = field(default=False)
    concurrency: int | None = field(default=None)

    root_path: str = field(default="")
    docs_url: str = field(default="/docs")


class DatabaseConfig(BaseStruct):
    """Configuration for Kortex Database."""

    echo: bool = field(default_factory=get_env("DATABASE_ECHO", False))
    """Enable SQLAlchemy engine logs."""
    echo_pool: bool = field(default_factory=get_env("DATABASE_ECHO_POOL", False))
    """Enable SQLAlchemy connection pool logs."""
    pool_disabled: bool = field(default_factory=get_env("DATABASE_POOL_DISABLED", False))
    """Disable SQLAlchemy pool configuration."""
    pool_max_overflow: int = field(default_factory=get_env("DATABASE_MAX_POOL_OVERFLOW", 10))
    """Max overflow for SQLAlchemy connection pool"""
    pool_size: int = field(default_factory=get_env("DATABASE_POOL_SIZE", 5))
    """Pool size for SQLAlchemy connection pool"""
    pool_timeout: int = field(default_factory=get_env("DATABASE_POOL_TIMEOUT", 30))
    """Time in seconds for timing connections out of the connection pool."""
    pool_recycle: int = field(default_factory=get_env("DATABASE_POOL_RECYCLE", 300))
    """Amount of time to wait before recycling connections."""
    pool_pre_ping: bool = field(default_factory=get_env("DATABASE_PRE_POOL_PING", False))
    """Optionally ping database before fetching a session from the connection pool."""
    dsn: str = field(default_factory=get_env("DATABASE_URL", "sqlite+aiosqlite:///db.sqlite3"))
    """SQLAlchemy Database URL."""
    migration_config: str = field(
        default_factory=get_env("DATABASE_MIGRATION_CONFIG", f"{BASE_DIR}/db/migrations/alembic.ini")
    )
    """The path to the `alembic.ini` configuration file."""
    migration_path: str = field(default_factory=get_env("DATABASE_MIGRATION_PATH", f"{BASE_DIR}/db/migrations"))
    """The path to the `alembic` database migrations."""
    migration_ddl_version_table: str = field(
        default_factory=get_env("DATABASE_MIGRATION_DDL_VERSION_TABLE", "ddl_version")
    )
    """The name to use for the `alembic` versions table name."""
    fixture_path: str = field(default_factory=get_env("DATABASE_FIXTURE_PATH", f"{BASE_DIR}/db/fixtures"))
    """The path to JSON fixture files to load into tables."""
    _engine_instance: AsyncEngine | None = None
    """SQLAlchemy engine instance generated from settings."""

    @property
    def engine(self) -> AsyncEngine:
        return self.get_engine()

    def get_engine(self) -> AsyncEngine:
        if self._engine_instance is not None:
            return self._engine_instance

        if self.dsn.startswith("postgresql+asyncpg"):
            engine = create_async_engine(
                url=self.dsn,
                future=True,
                json_serializer=encode_json,
                json_deserializer=decode_json,
                echo=self.echo,
                echo_pool=self.echo_pool,
                max_overflow=self.pool_max_overflow,
                pool_size=self.pool_size,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                pool_pre_ping=self.pool_pre_ping,
                poolclass=NullPool if self.pool_disabled else None,
            )

            @event.listens_for(engine.sync_engine, "connect")
            def _sqla_on_connect(dbapi_connection: Any, _: Any) -> Any:  # pragma: no cover
                """Using msgspec for serialization of the json column values means that the
                output is binary, not `str` like `json.dumps` would output.
                SQLAlchemy expects that the json serializer returns `str` and calls `.encode()` on the value to
                turn it to bytes before writing to the JSONB column. I'd need to either wrap `serialization.to_json` to
                return a `str` so that SQLAlchemy could then convert it to binary, or do the following, which
                changes the behaviour of the dialect to expect a binary value from the serializer.
                See Also https://github.com/sqlalchemy/sqlalchemy/blob/14bfbadfdf9260a1c40f63b31641b27fe9de12a0/lib/sqlalchemy/dialects/postgresql/asyncpg.py#L934  pylint: disable=line-too-long
                """

                def encoder(bin_value: bytes) -> bytes:
                    return b"\x01" + encode_json(bin_value)

                def decoder(bin_value: bytes) -> Any:
                    # the byte is the \x01 prefix for jsonb used by PostgreSQL.
                    # asyncpg returns it when format='binary'
                    return decode_json(bin_value[1:])

                dbapi_connection.await_(
                    dbapi_connection.driver_connection.set_type_codec(
                        "jsonb",
                        encoder=encoder,
                        decoder=decoder,
                        schema="pg_catalog",
                        format="binary",
                    ),
                )
                dbapi_connection.await_(
                    dbapi_connection.driver_connection.set_type_codec(
                        "json",
                        encoder=encoder,
                        decoder=decoder,
                        schema="pg_catalog",
                        format="binary",
                    ),
                )
        elif self.dsn.startswith("sqlite+aiosqlite"):
            engine = create_async_engine(
                url=self.dsn,
                future=True,
                json_serializer=encode_json,
                json_deserializer=decode_json,
                echo=self.echo,
                echo_pool=self.echo_pool,
                pool_recycle=self.pool_recycle,
                pool_pre_ping=self.pool_pre_ping,
            )
            """Database session factory.

            See [`async_sessionmaker()`][sqlalchemy.ext.asyncio.async_sessionmaker].
            """

            @event.listens_for(engine.sync_engine, "connect")
            def _sqla_on_connect(dbapi_connection: Any, _: Any) -> Any:  # pragma: no cover
                """Override the default begin statement.  The disables the built in begin execution."""
                dbapi_connection.isolation_level = None

            @event.listens_for(engine.sync_engine, "begin")
            def _sqla_on_begin(dbapi_connection: Any) -> Any:  # pragma: no cover
                """Emits a custom begin"""
                dbapi_connection.exec_driver_sql("BEGIN")
        else:
            engine = create_async_engine(
                url=self.dsn,
                future=True,
                json_serializer=encode_json,
                json_deserializer=decode_json,
                echo=self.echo,
                echo_pool=self.echo_pool,
                max_overflow=self.pool_max_overflow,
                pool_size=self.pool_size,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                pool_pre_ping=self.pool_pre_ping,
                pool_use_lifo=True,  # use lifo to reduce the number of idle connections
                poolclass=NullPool if self.pool_disabled else None,
            )
        self._engine_instance = engine
        return self._engine_instance


class CacheConfig(BaseStruct):
    """Cache configurations."""

    host: str = field(default="127.0.0.1")
    port: int = field(default=6379)
    password: str | None = field(default=None)
    database: int = field(default=0)


class AppConfig(BaseStruct):
    """Application configurations."""

    _instance: ClassVar["AppConfig | None"] = None

    server: ServerConfig = field(default_factory=ServerConfig)
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    openai_providers: list[OpenaiProvider] = field(default=[])

    @classmethod
    def from_file(cls, filename: str | None = None) -> Self:
        """Load the configuration from a file.

        Args:
            filename (`str`): The name of the configuration file, like "config.yaml".

        Note:
            Configuration filename suffix determines the format:
            - `.yaml`: YAML format
            - `.toml`: TOML format
        """

        if filename is None:
            filename = "config.yaml"

        if (config_file := ROOT_DIR / filename).exists():
            with config_file.open("r", encoding="utf-8") as f:
                configuration = f.read()

            match suffix := Path(filename).suffix:
                case ".yaml":
                    return yaml.decode(configuration, type=cls)
                case ".toml":
                    return toml.decode(configuration, type=cls)
                case _:
                    raise ValueError(f"Unsupported configuration file format: {suffix}")

        return cls()

    def get_provider(self, provider: str) -> OpenaiProvider | None:
        """Get the configuration for a specific AI provider."""
        for p in self.openai_providers:
            if p.provider == provider:
                return p

        return None

    @classmethod
    def get_config(cls, filename: str | None = None) -> "AppConfig":
        """Get the application configuration."""

        if cls._instance is None:
            cls._instance = cls.from_file(filename)

        return cls._instance
