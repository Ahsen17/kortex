import functools
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from ..config import AppConfig, ServerConfig
from ._version import __version__ as version

__all__ = ("Application",)


class Application:
    """Kortex application."""

    _instance: "Application | None" = None

    def __init__(self, config: ServerConfig) -> None:
        self._app = FastAPI(
            debug=config.debug,
            version=version,
            docs_url=config.docs_url,
            root_path=config.root_path,
        )

    @property
    def service(self) -> FastAPI:
        return self._app

    @property
    def version(self) -> str:
        return version

    def get_service(self) -> FastAPI:
        return self.service

    def get[T, **P](
        self,
        path: str,
        *,
        response_model: Any = None,
        status_code: int | None = None,
        description: str | None = None,
        deprecated: bool | None = None,
        operation_id: str | None = None,
        response_class: type[Response] = JSONResponse,
        name: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            decorated_func = self.service.get(
                path=path,
                response_model=response_model,
                status_code=status_code,
                description=description,
                deprecated=deprecated,
                operation_id=operation_id,
                response_class=response_class,
                name=name,
            )(func)

            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                return await decorated_func(*args, **kwargs)

            return wrapper

        return decorator

    def post[T, **P](
        self,
        path: str,
        *,
        response_model: Any = None,
        status_code: int | None = None,
        description: str | None = None,
        deprecated: bool | None = None,
        operation_id: str | None = None,
        response_class: type[Response] = JSONResponse,
        name: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            decorated_func = self.service.post(
                path=path,
                response_model=response_model,
                status_code=status_code,
                description=description,
                deprecated=deprecated,
                operation_id=operation_id,
                response_class=response_class,
                name=name,
            )(func)

            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                return await decorated_func(*args, **kwargs)

            return wrapper

        return decorator

    @classmethod
    def create(cls, config: ServerConfig | None = None) -> "Application":
        if cls._instance is None:
            if config is None:
                config = AppConfig.get_config().server

            cls._instance = cls(config)

        return cls._instance
