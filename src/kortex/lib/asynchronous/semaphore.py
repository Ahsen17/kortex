from typing import Self

import anyio

__all__ = ("Semaphore",)


class Semaphore:
    def __init__(
        self,
        capacity: int = 1,
        ever_delay: int = 5,
    ) -> None:
        """Async Semaphore initialization.

        Args:
            capacity(int): The maximum number of concurrent tasks.
            ever_delay(int): Each cycle of seconds to wait for a token.
        """

        self._lock = anyio.Lock()

        self._capacity = capacity
        self._ever_delay = ever_delay

        self._margin = capacity

    async def acquire(self) -> None:
        """Acquire a token."""

        while True:
            async with self._lock:
                if self._margin > 0:
                    self._margin -= 1

                    break

            await anyio.sleep(self._ever_delay)

    async def release(self) -> None:
        """Release a token."""

        async with self._lock:
            if self._margin < self._capacity:
                self._margin += 1

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.release()
