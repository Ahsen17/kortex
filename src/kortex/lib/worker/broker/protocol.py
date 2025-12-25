from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Self

from .message import Message

if TYPE_CHECKING:
    from .integration import BrokerHealth


__all__ = ("AsyncBrokerABC",)


class AsyncBrokerABC[T: Message](ABC):
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

    @abstractmethod
    async def produce(self, message: T) -> None:
        """Produce a message to a specific queue.

        This method adds a message to the specified queue with the given key.
        The message can optionally have a priority and time-to-live (TTL).

        Args:
            message: The message to produce

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
    ) -> Message | None:
        """Consume a message from a specific queue.

        Retrieves a single message from the specified queue, optionally filtered
        by key. If no key is specified, any message from the queue is returned.

        Args:
            queue: The name of the queue to consume from
            key: Optional message key to filter by
            timeout: Optional timeout in seconds (uses default if None)

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
    async def purge(self, queue: str, key: str | None = None) -> int:
        """Remove all messages from a queue (optionally filtered by key).

        Args:
            queue: The name of the queue to purge
            key: Optional message key to filter which messages to remove

        Returns:
            Number of messages removed

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def queue_size(self, queue: str, key: str | None = None) -> int:
        """Get the number of messages in a queue.

        Args:
            queue: The name of the queue to check
            key: Optional message key to filter count

        Returns:
            Number of messages in the queue

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def create_queue(self, queue: str) -> None:
        """Create a new queue.

        Args:
            queue: The name of the queue to create

        Raises:
            QueueExistsError: If the queue already exists
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def delete_queue(self, queue: str) -> None:
        """Delete an existing queue.

        Args:
            queue: The name of the queue to delete

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def list_queues(self) -> list[str]:
        """List all available queues.

        Returns:
            List of queue names

        Raises:
            BrokerConnectionError: If the broker is not connected
        """

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker.

        Raises:
            BrokerConnectionError: If connection fails
            ValueError: If required parameters are missing
        """

    @abstractmethod
    async def disconnect(self, force: bool | None = None) -> None:
        """Disconnect from the broker.

        Args:
            force: Whether to force disconnect even if there are pending messages
        """

    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if the broker is currently connected.

        Returns:
            True if connected, False otherwise
        """

    @abstractmethod
    async def health_check(self) -> "BrokerHealth":
        """Perform a health check on the broker.

        Returns:
            Dictionary containing health status information
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
        """Exit context manager for sync usage."""

        await self.disconnect()
