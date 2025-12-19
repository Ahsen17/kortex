from collections.abc import Iterable
from typing import Any


class Worker:
    @classmethod
    def start(cls, *args: Any, **kwargs: Any) -> "Worker":
        raise NotImplementedError()

    def process(self, items: Iterable[tuple[int, Any]]) -> Iterable[tuple[int, Any]]:
        raise NotImplementedError()
