import inspect
import time
from collections.abc import Awaitable
from typing import Any

import anyio
from valkey.asyncio import BlockingConnectionPool
from valkey.asyncio import Valkey as AsyncValkey

from ..constant import BROKER_QUEUE_PREFIX, DEFAULT_QUEUE_SIZE, DEFAULT_TIMEOUT, DEFAULT_TTL, MESSAGE_KEY_SEPERATOR
from ..exception import BrokerConnectionError, QueueExistsError, QueueFullError, QueueNotFoundError
from ..integration import BrokerHealth
from ..message import Message
from ..protocol import AsyncBrokerABC


class _asyncValkeyClient:  # noqa: N801
    """Type-safe wrapper for Redis async client.

    Valkey's type stubs return Union[Awaitable[T], T] for all methods to support
    both sync and async modes. This wrapper provides proper type hints for async
    usage, centralizing the type ignore comments in one place.
    """

    def __init__(self, client: AsyncValkey) -> None:
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
        match await self.await_if_needed(self._client.lpop(name)):
            case str() as result:
                return result.encode("utf-8")
            case _:
                return None

        # lpop without count returns str|bytes, with count returns list
        # We don't use count, so this should be str|bytes

    async def llen(self, name: str) -> int:
        return await self.await_if_needed(self._client.llen(name))

    async def delete(self, *names: str) -> int:
        return await self.await_if_needed(self._client.delete(*names))

    async def keys(self, pattern: str) -> list[bytes]:
        return await self.await_if_needed(self._client.keys(pattern))

    async def aclose(self) -> None:
        await self._client.aclose()


class AsyncValkeyBroker[T: Message](AsyncBrokerABC[T]):
    """Asynchronous Valkey-based message broker.

    Uses Valkey lists for queue storage and key-based filtering.
    Each queue is stored as a Valkey list with messages serialized as JSON.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
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
            **kwargs: Additional keyword arguments passed to Valkey client
        """

        self._host = host
        self._port = port
        self._password = password
        self._db = db
        self._args = kwargs

        self._raw_client: AsyncValkey | None = None
        self._client: _asyncValkeyClient | None = None

        self._lock = anyio.Lock()
        self._contented = False

    @property
    def client(self) -> AsyncValkey:
        """Get the underlying Valkey client.

        Returns:
            The Valkey async client

        Raises:
            RuntimeError: If not connected
        """

        if self._raw_client is None:
            raise RuntimeError("Client not initialized. Call connect() first.")
        return self._raw_client

    async def set_connected(self, state: bool) -> None:
        async with self._lock:
            self._contented = state

    async def is_connected(self) -> bool:
        """Check if connected to Valkey.

        Returns:
            True if connected, False otherwise
        """

        async with self._lock:
            if not self._contented or self._client is None:
                return False

        try:
            await self._client.ping()
            return True

        except Exception:  # noqa: BLE001
            await self.set_connected(False)
            return False

    async def connect(self) -> None:
        """Establish connection to Valkey.

        Raises:
            BrokerConnectionError: If connection fails
        """

        if await self.is_connected():
            return

        try:
            if self._raw_client is None:
                # Create async client from conn pool
                self._raw_client = AsyncValkey.from_pool(
                    connection_pool=BlockingConnectionPool(
                        host=self._host,
                        port=self._port,
                        password=self._password,
                        db=self._db,
                    )
                )

            if self._client is None:
                self._client = _asyncValkeyClient(self._raw_client)

            await self._client.ping()

            await self.set_connected(True)

        except Exception as e:
            raise BrokerConnectionError(
                "Failed to connect to Valkey",
            ) from e

    async def disconnect(self, force: bool | None = None) -> None:
        """Disconnect from Valkey."""

        if not await self.is_connected():
            return

        if self._client:
            await self._client.aclose()

            self._client = None
            self._raw_client = None

        await self.set_connected(False)

    async def health_check(self) -> BrokerHealth:
        """Perform health check on Valkey connection.

        Returns:
            BrokerHealth with status information
        """

        start_time = time.time()

        if not await self.is_connected() or self._client is None:
            return BrokerHealth(
                status="unhealthy",
                error=BrokerConnectionError("Not connected."),
            )

        try:
            await self._client.ping()
            resp_time = (time.perf_counter() - start_time) * 1000

            # Get server info
            info = await self._client.info("server")
            version = info.get("valkey_version", "unknown") if info else "unknown"

            # Get connected clients
            clients = await self._client.info("clients")
            connected_clients = clients.get("connected_clients", 0) if clients else 0

            return BrokerHealth(
                status="healthy",
                response_time_ms=resp_time,
                version=version,
                connected_clients=connected_clients,
            )

        except Exception as e:  # noqa: BLE001
            return BrokerHealth(
                status="unhealthy",
                response_time_ms=(time.perf_counter() - start_time) * 1000,
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
            return f"{BROKER_QUEUE_PREFIX}:{queue}:{key}"
        return f"{BROKER_QUEUE_PREFIX}:{queue}"

    async def produce(self, message: T) -> None:
        """Produce a message to a specific queue.

        Args:
            message: The message to produce

        Raises:
            BrokerConnectionError: If the broker is not connected
            ValueError: If queue name is invalid
            QueueFullError: If the queue is full
        """

        if not await self.is_connected() or self._client is None:
            raise BrokerConnectionError("Not connected.")

        if not message.queue:
            raise ValueError("Queue name")

        if (
            await self.queue_size(
                message.queue,
                message.key,
            )
            > DEFAULT_QUEUE_SIZE
        ):
            raise QueueFullError(f"Queue '{message.queue}' is full.")

        queue_name = self._get_queue_name(message.queue, message.key)

        if (priority := message.priority) is not None and priority > 0:
            await self._client.lpush(queue_name, message.to_jsonb())
        else:
            await self._client.rpush(queue_name, message.to_jsonb())

        # Set TTL
        await self._client.expire(
            queue_name,
            int(message.ttl or DEFAULT_TTL),
        )

    async def consume(
        self,
        queue: str,
        key: str | None = None,
        timeout: float | None = None,
    ) -> Message | None:
        """Consume a message from a specific queue.

        Args:
            queue: The name of the queue to consume from
            key: Optional message key to filter by
            timeout: Optional timeout in seconds (uses default if None)

        Returns:
            A BrokerMessage instance if a message is available, None if timeout

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

        if not await self.is_connected() or self._client is None:
            raise BrokerConnectionError("Not connected.")

        queue_name = self._get_queue_name(queue, key)

        # Check if queue exists
        if not await self._client.exists(queue_name):
            raise QueueNotFoundError(f"Queue '{queue}' does not exist.")

        # Use BLPOP for blocking pop with timeout
        result = await self._client.blpop(
            [queue_name],
            timeout=int(timeout or DEFAULT_TIMEOUT),
        )

        if result is None:
            return None

        # Parse message - result is tuple[bytes, bytes] or bytes
        msg_bytes = result[1] if isinstance(result, tuple) else result

        return Message.from_jsonb(msg_bytes)

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

        if not await self.is_connected() or self._client is None:
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

        if not await self.is_connected() or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue, key)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue_name}' does not exist")

        return await self._client.llen(queue_name)

    async def create_queue(self, queue: str) -> None:
        """Create a new queue.

        Args:
            queue: The name of the queue to create

        Raises:
            QueueExistsError: If the queue already exists
            BrokerConnectionError: If the broker is not connected
        """

        if not await self.is_connected() or self._client is None:
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

    async def delete_queue(self, queue: str) -> None:
        """Delete an existing queue.

        Args:
            queue: The name of the queue to delete

        Raises:
            QueueNotFoundError: If the queue does not exist
            BrokerConnectionError: If the broker is not connected
        """

        if not await self.is_connected() or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        queue_name = self._get_queue_name(queue)

        # Check if queue exists
        exists = await self._client.exists(queue_name)
        if not exists:
            raise QueueNotFoundError(f"Queue '{queue}' does not exist")

        await self._client.delete(queue_name)

    async def list_queues(self) -> list[str]:
        """List all available queues.

        Returns:
            List of queue names

        Raises:
            BrokerConnectionError: If the broker is not connected
        """

        if not await self.is_connected() or self._client is None:
            raise BrokerConnectionError("Not connected to Valkey")

        # Get all keys matching queue pattern
        keys = await self._client.keys("queue:*")

        # Extract unique queue names (remove key suffixes and duplicates)
        queues: set[str] = set()
        for key in keys:
            key_str = key.decode("utf-8")
            # Format: queue:{queue_name} or queue:{queue_name}:{key}
            parts = key_str.split(MESSAGE_KEY_SEPERATOR, 3)
            if len(parts) >= 3:
                queues.add(parts[2])

        return sorted(queues)
