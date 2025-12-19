"""Abstract interfaces for broker implementations.

This module provides the abstract base classes that define the standard
interface for all broker implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .config import ConsumeOptions, PublishOptions
from .messages import Message, MessageBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

__all__ = (
    "AsyncBroker",
    "Broker",
)


class Broker[T](ABC):
    """Abstract base class for message queue brokers.

    Provides standard interface following industry patterns (AMQP, Celery, AWS SQS).
    All operations are async for consistency.

    Example:
        >>> class MyBroker(Broker[dict]):
        ...     async def publish(self, queue: str, body: dict, options=None) -> str:
        ...         # Implementation
        ...         return "msg-id"
        ...
        ...     async def consume(self, queue: str, options=None) -> list[Message[dict]]:
        ...         # Implementation
        ...         return []
    """

    @abstractmethod
    async def publish(
        self,
        queue: str,
        body: T,
        options: PublishOptions | None = None,
    ) -> str:
        """Publish a message to a queue.

        Args:
            queue: Queue name.
            body: Message payload.
            options: Publishing options.

        Returns:
            Message ID.

        Raises:
            QueueFullError: If queue is at capacity.
            MessageValidationError: If message validation fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self,
        queue: str,
        options: ConsumeOptions | None = None,
    ) -> list[Message[T]]:
        """Consume messages from a queue.

        Args:
            queue: Queue name.
            options: Consumption options.

        Returns:
            List of messages.

        Raises:
            QueueEmptyError: If queue is empty and no wait timeout.
        """
        raise NotImplementedError

    @abstractmethod
    async def ack(self, receipt: str, queue: str) -> bool:
        """Acknowledge successful message processing.

        Args:
            receipt: Message receipt handle.
            queue: Queue name.

        Returns:
            True if acknowledged successfully.
        """
        raise NotImplementedError

    @abstractmethod
    async def nack(
        self,
        receipt: str,
        queue: str,
        requeue: bool = False,
    ) -> bool:
        """Negative acknowledge failed message processing.

        Args:
            receipt: Message receipt handle.
            queue: Queue name.
            requeue: Whether to requeue the message.

        Returns:
            True if nacked successfully.
        """
        raise NotImplementedError

    @abstractmethod
    async def purge(self, queue: str) -> int:
        """Purge all messages from a queue.

        Args:
            queue: Queue name.

        Returns:
            Number of messages purged.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_queue_stats(self, queue: str) -> dict[str, Any]:
        """Get queue statistics.

        Args:
            queue: Queue name.

        Returns:
            Dictionary with stats (count, depth, etc.).
        """
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to broker."""
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to broker."""
        raise NotImplementedError

    async def __aenter__(self) -> Broker[T]:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()

    async def publish_batch(
        self,
        queue: str,
        bodies: list[T],
        options: PublishOptions | None = None,
    ) -> list[str]:
        """Publish multiple messages in batch.

        Args:
            queue: Queue name.
            bodies: List of message payloads.
            options: Publishing options (applied to all).

        Returns:
            List of message IDs.
        """
        return [await self.publish(queue, body, options) for body in bodies]

    async def consume_batch(
        self,
        queue: str,
        max_messages: int = 100,
        options: ConsumeOptions | None = None,
    ) -> MessageBatch[T]:
        """Consume messages as a batch.

        Args:
            queue: Queue name.
            max_messages: Maximum messages to consume.
            options: Consumption options.

        Returns:
            MessageBatch containing messages and metadata.
        """
        if options is None:
            options = ConsumeOptions(max_messages=max_messages)
        else:
            options.max_messages = min(options.max_messages, max_messages)

        messages = await self.consume(queue, options)
        return MessageBatch(
            messages=messages,
            queue=queue,
            total_count=len(messages),
            has_more=False,
        )


class AsyncBroker[T](Broker[T]):
    """Extended broker with streaming and subscription support.

    Provides additional methods for event-driven processing patterns.
    """

    @abstractmethod
    async def stream(
        self,
        queue: str,
        options: ConsumeOptions | None = None,
    ) -> AsyncIterator[Message[T]]:
        """Stream messages from a queue.

        Args:
            queue: Queue name.
            options: Consumption options.

        Yields:
            Messages as they arrive.

        Example:
            >>> async for message in broker.stream("tasks"):
            ...     await process(message)
        """
        raise NotImplementedError

    @abstractmethod
    async def subscribe(
        self,
        queues: list[str],
        callback: Callable[[Message[T]], Awaitable[None]],
        options: ConsumeOptions | None = None,
    ) -> None:
        """Subscribe to multiple queues with callback.

        Args:
            queues: List of queue names.
            callback: Async message handler.
            options: Consumption options.

        Example:
            >>> async def handler(msg):
            ...     await process(msg)
            >>> await broker.subscribe(["q1", "q2"], handler)
        """
        raise NotImplementedError

    @abstractmethod
    async def move(
        self,
        message: Message[T],
        target_queue: str,
        preserve_receipt: bool = True,
    ) -> bool:
        """Move message to another queue.

        Args:
            message: Message to move.
            target_queue: Target queue name.
            preserve_receipt: Keep original receipt.

        Returns:
            True if moved successfully.
        """
        raise NotImplementedError

    @abstractmethod
    async def extend_visibility_timeout(
        self,
        receipt: str,
        queue: str,
        timeout: int,
    ) -> bool:
        """Extend visibility timeout for a message.

        Args:
            receipt: Message receipt handle.
            queue: Queue name.
            timeout: New visibility timeout in seconds.

        Returns:
            True if extended successfully.
        """
        raise NotImplementedError
