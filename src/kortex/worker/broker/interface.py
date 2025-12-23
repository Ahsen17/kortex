"""Broker abstract base class for message queue operations.

This module defines the abstract base class (ABC) for message brokers that support
common message queue operations including producing, consuming, and listening to
multiple message queues with flexible message routing.

The broker design supports:
- Message production to named queues with message keys
- Message consumption from specific queues with key-based filtering
- Multi-queue listening for concurrent message processing
- Context manager support for resource management
- Async and sync operation modes
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Self

from msgspec import json

from .integration import BrokerHealth

__all__ = (
    "AsyncBroker",
    "BrokerMessage",
)


@dataclass
class BrokerMessage[T]:
    """Container for messages retrieved from the broker.

    Attributes:
        queue: The name of the queue the message was retrieved from
        key: The message key used for routing/filtering
        payload: The actual message data
        timestamp: Optional timestamp when the message was produced
        metadata: Optional additional metadata about the message
    """

    queue: str
    payload: T
    key: str | None = None
    timestamp: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"BrokerMessage(queue={self.queue!r}, key={self.key!r}, payload={self.payload!r})"

    def to_json(self) -> str:
        """Convert the message to a JSON string."""

        return json.encode(self).decode("utf-8")

    def to_bytes(self) -> bytes:
        """Convert the message to json bytes"""

        return json.encode(self)

    @classmethod
    def from_json(cls, data: str) -> Self:
        """Create a BrokerMessage from a JSON string."""

        return json.decode(data, type=cls)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Create a BrokerMessage from json bytes."""

        return json.decode(data, type=cls)


class AsyncBroker[T](ABC):
    """Abstract base class for message brokers.

    This class defines the interface for message brokers that support
    producing messages to queues and consuming messages from queues.
    Implementations should provide concrete implementations for all
    abstract methods.

    The broker supports multiple queues identified by queue names, and
    within each queue, messages can be filtered by keys. This allows
    for flexible message routing and consumption patterns.

    Example:
        >>> async with AsyncBroker() as broker:
        ...     await broker.produce("tasks", "process", {"data": "value"})
        ...     async for msg in broker.listen(["tasks"]):
        ...         print(msg.payload)
    """

    # Class attributes for configuration

    @abstractmethod
    async def produce(
        self,
        queue: str,
        payload: T,
        priority: int | None = None,
        ttl: float | None = None,
        key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Produce a message to a specific queue.

        This method adds a message to the specified queue with the given key.
        The message can optionally have a priority and time-to-live (TTL).

        Args:
            queue: The name of the queue to produce to
            key: The message key for routing/filtering
            payload: The message data to produce
            priority: Optional priority level (higher = more urgent)
            ttl: Optional time-to-live in seconds before message expires
            **kwargs: Additional implementation-specific parameters

        Raises:
            QueueFullError: If the queue is full and cannot accept more messages
            BrokerConnectionError: If the broker is not connected
            ValueError: If queue name or key is invalid

        Example:
            >>> await broker.produce("events", key="user.login", payload={"user_id": 42}, priority=5)
        """

    @abstractmethod
    async def consume(
        self,
        queue: str,
        key: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> BrokerMessage[T] | None:
        """Consume a message from a specific queue.

        Retrieves a single message from the specified queue, optionally filtered
        by key. If no key is specified, any message from the queue is returned.

        Args:
            queue: The name of the queue to consume from
            key: Optional message key to filter by
            timeout: Optional timeout in seconds (uses default if None)
            **kwargs: Additional implementation-specific parameters

        Returns:
            A BrokerMessage instance if a message is available, None if timeout
            occurs and no message is available

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected

        Example:
            >>> message = await broker.consume("events", "user.login", timeout=5.0)
            >>> if message:
            ...     print(message.payload)
        """

    @abstractmethod
    async def purge(self, queue: str, key: str | None = None, **kwargs: Any) -> int:
        """Remove all messages from a queue (optionally filtered by key).

        Args:
            queue: The name of the queue to purge
            key: Optional message key to filter which messages to remove
            **kwargs: Additional implementation-specific parameters

        Returns:
            Number of messages removed

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def queue_size(self, queue: str, key: str | None = None, **kwargs: Any) -> int:
        """Get the number of messages in a queue.

        Args:
            queue: The name of the queue to check
            key: Optional message key to filter count
            **kwargs: Additional implementation-specific parameters

        Returns:
            Number of messages in the queue

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def create_queue(self, queue: str, **kwargs: Any) -> None:
        """Create a new queue.

        Args:
            queue: The name of the queue to create
            **kwargs: Additional implementation-specific parameters (e.g., size limits)

        Raises:
            QueueExistsError: If the queue already exists
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def delete_queue(self, queue: str, **kwargs: Any) -> None:
        """Delete an existing queue.

        Args:
            queue: The name of the queue to delete
            **kwargs: Additional implementation-specific parameters

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def list_queues(self, **kwargs: Any) -> list[str]:
        """List all available queues.

        Args:
            **kwargs: Additional implementation-specific parameters

        Returns:
            List of queue names

        Raises:
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def list_keys(self, queue: str, **kwargs: Any) -> list[str]:
        """List all message keys in a queue.

        Args:
            queue: The name of the queue
            **kwargs: Additional implementation-specific parameters

        Returns:
            List of message keys in the queue

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def connect(self, **kwargs: Any) -> None:
        """Establish connection to the broker.

        Args:
            **kwargs: Connection parameters (host, port, credentials, etc.)

        Raises:
            BrokerConnectionError: If connection fails
            ValueError: If required parameters are missing
        """

    @abstractmethod
    async def disconnect(self, **kwargs: Any) -> None:
        """Disconnect from the broker.

        Args:
            **kwargs: Additional implementation-specific parameters
        """

    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if the broker is currently connected.

        Returns:
            True if connected, False otherwise
        """

    @abstractmethod
    async def health_check(self) -> BrokerHealth:
        """Perform a health check on the broker.

        Returns:
            Dictionary containing health status information

        Example:
            >>> status = broker.health_check()
            >>> print(status["status"])  # "healthy" or "unhealthy"
        """

    async def __aenter__(self) -> Self:
        """Enter context manager for sync usage.

        Returns:
            Self instance after connecting
        """

        await self.connect()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: Any,
    ) -> None:
        """Exit context manager for sync usage.

        Args:
            _exc_type: Exception type (if any)
            _exc_val: Exception value (if any)
            _exc_tb: Exception traceback (if any)
        """

        await self.disconnect()
