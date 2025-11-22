from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

type JsonString = str
type JsonBytes = bytes

type AnyCallable = Callable[..., Any]
type AnyAsyncCallable = Callable[..., Awaitable[Any]]

type PathOrStr = Path | str
