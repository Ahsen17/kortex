from collections.abc import AsyncGenerator, Generator
from io import TextIOWrapper
from pathlib import Path

import aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper

from ..base import BaseSchema
from ..base.abc import PathOrStr

__all__ = ("FileReader",)


class FileReader(BaseSchema):
    """Schema for file paths."""

    path: PathOrStr

    def __enter__(self) -> Generator[TextIOWrapper]:
        with Path(self.path).open(encoding="utf-8") as f:
            yield f

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self) -> AsyncGenerator[AsyncTextIOWrapper]:
        async with aiofiles.open(self.path, encoding="utf-8") as f:
            yield f

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
