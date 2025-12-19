"""Type aliases for broker module.

This module provides Python 3.12+ type aliases for better type hints
and code readability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .messages import Message

__all__ = (
    "MessageHandler",
    "MessageId",
    "QueueName",
    "Receipt",
)


# Python 3.12+ type aliases for better readability
type MessageHandler[T] = Callable[[Message[T]], Awaitable[None]]
type QueueName = str
type Receipt = str
type MessageId = str
