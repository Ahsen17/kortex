"""Valkey-based async broker implementation.

This module provides an AsyncBroker implementation using Valkey as the backend.
It uses Valkey's list data structure to implement queues and pub/sub for message routing.
"""

import inspect
from collections.abc import Awaitable
from time import time
from typing import Any

from msgspec import json
from valkey.asyncio import Redis

from kortex.worker.broker.config import QueueConfig
from kortex.worker.broker.exception import (
    BrokerConnectionError,
    QueueExistsError,
    QueueNotFoundError,
)
from kortex.worker.broker.integration import BrokerHealth
from kortex.worker.broker.interface import AsyncBroker, BrokerMessage

__all__ = ("AsyncValkeyBroker",)


class _AsyncRedisClient:
    """Type-safe wrapper for Redis async client.

    Valkey's type stubs return Union[Awaitable[T], T] for all methods to support
    both sync and async modes. This wrapper provides proper type hints for async
    usage, centralizing the type ignore comments in one place.
    """

    def __init__(self, client: Redis) -> None:
        self._client = client

    @staticmethod
    async def await_if_needed[T](obj: Awaitable[T] | T) -> T:
        """Await if needed."""

        if inspect.iscoroutine(obj):
            return await obj  # type: ignore

        return obj  # type: ignore[return-value]

    async def ping(self) -> bool:
        return await self.await_if_needed(self._client.ping())

    async def info(self, section: str | None = None) -> dict[str, Any]:
        return await self.await_if_needed(self._client.info(section))

    async def lpush(self, name: str, *values: Any) -> int:
        return await self.await_if_needed(self._client.lpush(name, *values))

    async def rpush(self, name: str, *values: Any) -> int:
        return await self.await_if_needed(self._client.rpush(name, *values))

    async def expire(self, name: str, seconds: int) -> int:
        return await self.await_if_needed(self._client.expire(name, seconds))

    async def exists(self, *names: str) -> int:
        return await self.await_if_needed(self._client.exists(*names))

    async def blpop(self, keys: list[str], timeout: int = 0) -> tuple[bytes, bytes] | None:
        result = await self.await_if_needed(self._client.blpop(keys, timeout=timeout))

        # blpop returns tuple[str, str] or tuple[bytes, bytes]
        # Convert to tuple[bytes, bytes]
        if isinstance(result, (list, tuple)):
            first = result[0] if isinstance(result[0], bytes) else result[0].encode("utf-8")
            second = result[1] if isinstance(result[1], bytes) else result[1].encode("utf-8")
            return (first, second)

        return None  # type: ignore

    async def lpop(self, name: str) -> bytes | None:
        result = await self.await_if_needed(self._client.lpop(name))
        if result is None:
            return None

        if isinstance(result, bytes):
            return result

        if isinstance(result, str):
            return result.encode("utf-8")

        # lpop without count returns str|bytes, with count returns list
        # We don't use count, so this should be str|bytes
        return None

    async def llen(self, name: str) -> int:
        return await self.await_if_needed(self._client.llen(name))

    async def delete(self, *names: str) -> int:
        return await self.await_if_needed(self._client.delete(*names))

    async def keys(self, pattern: str) -> list[bytes]:
        return await self.await_if_needed(self._client.keys(pattern))

    async def aclose(self) -> None:
        await self._client.aclose()


class AsyncValkeyBroker[T](AsyncBroker[T]):
    """Asynchronous Valkey-based message broker.

    Uses Valkey lists for queue storage and key-based filtering.
    Each queue is stored as a Valkey list with messages serialized as JSON.

    Queue naming convention:
    - Base queue: `queue:{queue_name}`
    - Key-specific queue: `queue:{queue_name}:{key}`

    Attributes:
        client: The Valkey async client
        config: Optional queue configuration
    """

    def __init__(
        self,
        client: Redis | None = None,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        config: QueueConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the AsyncValkeyBroker.

        Args:
            client: Pre-configured Redis client (optional)
            host: Valkey server host (ignored if client provided)
            port: Valkey server port (ignored if client provided)
            db: Valkey database number (ignored if client provided)
            password: Valkey password (ignored if client provided)
            config: Queue configuration
            **kwargs: Additional arguments passed to Redis client
        """
        self._raw_client = client
        self._client: _AsyncRedisClient | None = None
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._kwargs = kwargs
        self._config = config
        self._connected = False

    async def connect(self, **kwargs: Any) -> None:
        """Establish connection to Valkey.

        Args:
            **kwargs: Connection parameters (host, port, password, etc.)

        Raises:
            BrokerConnectionError: If connection fails
        """
        try:
            if self._raw_client is None:
                # Use provided kwargs or instance attributes
                host = kwargs.get("host", self._host)
                port = kwargs.get("port", self._port)
                db = kwargs.get("db", self._db)
                password = kwargs.get("password", self._password)

                self._raw_client = Redis(
                    host=host,
                    port=port,
                    db=db,
                    password=password,
                    **self._kwargs,
                    **{k: v for k, v in kwargs.items() if k not in ("host", "port", "db", "password")},
                )

            self._client = _AsyncRedisClient(self._raw_client)

            # Test connection
            await self._client.ping()
            self._connected = True

        except Exception as e:
            raise BrokerConnectionError(f"Failed to connect to Valkey: {e}") from e

    async def disconnect(self, **kwargs: Any) -> None:
        """Disconnect from Valkey.

        Args:
            **kwargs: Additional parameters (unused)
        """
        if self._client:
            await self._client.aclose()
            self._client = None
            self._raw_client = None

        self._connected = False

    async def is_connected(self) -> bool:
        """Check if connected to Valkey.

        Returns:
            True if connected, False otherwise
        """
        if not self._connected or self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:  # noqa: BLE001
            self._connected = False
            return False

    async def health_check(self) -> BrokerHealth:
        """Perform health check on Valkey connection.

        Returns:
            BrokerHealth with status information
        """
        start_time = time()
        try:
            if not await self.is_connected():
                return BrokerHealth(
                    status="unhealthy",
                    error=BrokerConnectionError("Not connected"),
                )

            if self._client is None:
                raise BrokerConnectionError("Not connected")
            await self._client.ping()
            response_time = (time() - start_time) * 1000

            # Get server info
            info = await self._client.info("server")
            version = info.get("valkey_version", "unknown") if info else "unknown"

            # Get connected clients
            clients = await self._client.info("clients")
            connected_clients = clients.get("connected_clients", 0) if clients else 0

            return BrokerHealth(
                status="healthy",
                response_time_ms=response_time,
                version=version,
                connected_clients=connected_clients,
            )
        except Exception as e:  # noqa: BLE001
            return BrokerHealth(
                status="unhealthy",
                response_time_ms=(time() - start_time) * 1000,
                error=e,
            )

    def _get_queue_name(self, queue: str, key: str | None = None) -> str:
        """Generate Valkey key for a queue.

        Args:
            queue: Queue name
            key: Optional message key

        Returns:
            Valkey key string
        """
        if key:
            return f"queue:{queue}:{key}"
        return f"queue:{queue}"

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

        Args:
            queue: The name of the queue to produce to
            payload: The message data to produce
            priority: Optional priority level (higher = more urgent)
            ttl: Optional time-to-live in seconds before message expires
            key: The message key for routing/filtering
            **kwargs: Additional implementation-specific parameters

        Raises:
            BrokerConnectionError: If the broker is not connected
            ValueError: If queue name is invalid
        """
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        if not queue:
            raise ValueError("Queue name cannot be empty")

        # Create message container
        message: dict[str, Any] = {
            "payload": payload,
            "key": key,
            "priority": priority,
            "timestamp": kwargs.get("timestamp"),
        }

        # Serialize message
        message_bytes = json.encode(message)

        # Determine target queue(s)
        queue_name = self._get_queue_name(queue, key)

        # Use LPUSH for priority (higher priority = push to front)
        # If priority is specified, we push to front, otherwise to back
        if priority is not None and priority > 0:
            await self._client.lpush(queue_name, message_bytes)
        else:
            await self._client.rpush(queue_name, message_bytes)

        # Set TTL if specified
        if ttl is not None:
            await self._client.expire(queue_name, int(ttl))

    async def consume(
        self,
        queue: str,
        key: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> BrokerMessage[T] | None:
        """Consume a message from a specific queue.

        Args:
            queue: The name of the queue to consume from
            key: Optional message key to filter by
            timeout: Optional timeout in seconds (uses default if None)
            **kwargs: Additional implementation-specific parameters

        Returns:
            A BrokerMessage instance if a message is available, None if timeout

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue, key)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue_name}' does not exist")

        # Use BLPOP for blocking pop with timeout
        if timeout is not None:
            result = await self._client.blpop([queue_name], timeout=int(timeout))
        else:
            result = await self._client.lpop(queue_name)

        if result is None:
            return None

        # Parse message - result is tuple[bytes, bytes] or bytes
        message_bytes = result[1] if isinstance(result, tuple) else result
        # Ensure bytes for json.decode
        if isinstance(message_bytes, str):
            message_bytes = message_bytes.encode("utf-8")
        elif not isinstance(message_bytes, bytes):
            # Should not happen, but handle gracefully
            raise ValueError(f"Unexpected message type: {type(message_bytes)}")

        message_data = json.decode(message_bytes, type=dict[str, Any])

        return BrokerMessage(
            queue=queue,
            key=key,
            payload=message_data.get("payload"),  # type: ignore[arg-type]
            timestamp=message_data.get("timestamp"),
            metadata={
                "priority": message_data.get("priority"),
            },
        )

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
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue, key)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue_name}' does not exist")

        # Get queue length
        length = await self._client.llen(queue_name)

        # Delete the queue
        if length > 0:
            await self._client.delete(queue_name)

        return length

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
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue, key)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue_name}' does not exist")

        return await self._client.llen(queue_name)

    async def create_queue(self, queue: str, **kwargs: Any) -> None:
        """Create a new queue.

        Args:
            queue: The name of the queue to create
            **kwargs: Additional implementation-specific parameters (e.g., size limits)

        Raises:
            QueueExistsError: If the queue already exists
            BrokerConnectionError: If the broker is not connected
        """
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        # Check if queue already exists
        queue_name = self._get_queue_name(queue)
        exists = await self._client.exists(queue_name)
        if exists:
            raise QueueExistsError(f"Queue '{queue}' already exists")

        # Create empty queue by pushing and popping a dummy value
        # This ensures the key exists with the correct type
        await self._client.rpush(queue_name, "dummy")
        await self._client.lpop(queue_name)

    async def delete_queue(self, queue: str, **kwargs: Any) -> None:
        """Delete an existing queue.

        Args:
            queue: The name of the queue to delete
            **kwargs: Additional implementation-specific parameters

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue}' does not exist")

        await self._client.delete(queue_name)

    async def list_queues(self, **kwargs: Any) -> list[str]:
        """List all available queues.

        Args:
            **kwargs: Additional implementation-specific parameters

        Returns:
            List of queue names

        Raises:
            BrokerConnectionError: If the broker is not connected
        """
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        # Get all keys matching queue pattern
        keys = await self._client.keys("queue:*")

        # Extract unique queue names (remove key suffixes and duplicates)
        queues: set[str] = set()
        for key in keys:
            key_str = key.decode("utf-8")
            # Format: queue:{queue_name} or queue:{queue_name}:{key}
            parts = key_str.split(":", 2)
            if len(parts) >= 2:
                queues.add(parts[1])

        return sorted(queues)

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
        if not self._connected or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        # Check if base queue exists
        base_queue = self._get_queue_name(queue)
        base_exists = await self._client.exists(base_queue)

        # Find all key-specific queues
        pattern = f"queue:{queue}:*"
        keys = await self._client.keys(pattern)

        result: set[str] = set()

        # Add base queue indicator if it has messages
        if base_exists and await self._client.llen(base_queue) > 0:
            result.add("default")

        # Extract keys from key-specific queues
        for key in keys:
            key_str = key.decode("utf-8")
            parts = key_str.split(":", 2)
            if len(parts) >= 3:
                result.add(parts[2])

        return sorted(result)

    @property
    def client(self) -> Redis:
        """Get the underlying Valkey client.

        Returns:
            The Valkey async client

        Raises:
            RuntimeError: If not connected
        """
        if self._raw_client is None:
            raise RuntimeError("Client not initialized. Call connect() first.")
        return self._raw_client
