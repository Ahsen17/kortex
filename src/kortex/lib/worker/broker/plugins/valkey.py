"""Valkey-based message queue broker implementation.

This module provides a concrete implementation of the broker interface using
Valkey Streams as the underlying message queue mechanism.

Uses valkey.asyncio.Valkey for native async/await support.

Valkey Streams provide:
- Reliable message delivery with acknowledgment
- Consumer groups for parallel processing
- Pending message tracking and recovery
- Stream-based message storage with automatic cleanup
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from typing import TYPE_CHECKING, Any

from valkey.asyncio import Valkey
from valkey.exceptions import ValkeyError

from ..config import ConsumeOptions, PublishOptions
from ..exceptions import QueueError
from ..interfaces import AsyncBroker
from ..messages import Message, MessageStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

__all__ = ("ValkeyBroker",)


class ValkeyBroker(AsyncBroker[Any]):
    """Valkey Streams-based message queue broker using async client.

    Uses valkey.asyncio.Valkey for native async/await support.

    Features:
    - Stream-based message storage
    - Consumer groups for parallel processing
    - Automatic message acknowledgment
    - Pending message recovery
    - Visibility timeout support
    - Stream trimming for automatic cleanup

    Example:
        >>> broker = ValkeyBroker(host="localhost", port=6379)
        >>> await broker.connect()
        >>> msg_id = await broker.publish("tasks", {"action": "process"})
        >>> messages = await broker.consume("tasks")
        >>> await broker.ack(messages[0].receipt, "tasks")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        client_name: str = "kortex-broker",
        **kwargs: Any,
    ) -> None:
        """Initialize Valkey broker.

        Args:
            host: Valkey server host
            port: Valkey server port
            db: Database number
            password: Connection password
            socket_timeout: Socket timeout
            socket_connect_timeout: Socket connect timeout
            client_name: Client name for monitoring
            **kwargs: Additional Valkey client arguments
        """
        self._client = Valkey(
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            client_name=client_name,
            decode_responses=True,
            **kwargs,
        )
        self._connected = False
        self._stream_max_length = 10000  # Max messages per stream

    async def connect(self) -> None:
        """Establish connection to Valkey."""
        try:
            # Test connection
            await self._client.ping()
            self._connected = True
        except ValkeyError as e:
            raise QueueError(f"Failed to connect to Valkey: {e}") from e

    async def disconnect(self) -> None:
        """Close connection to Valkey."""
        if self._connected:
            with contextlib.suppress(Exception):
                await self._client.close()
            self._connected = False

    async def publish(
        self,
        queue: str,
        body: Any,
        options: PublishOptions | None = None,
    ) -> str:
        """Publish a message to a Valkey stream.

        Args:
            queue: Stream name
            body: Message payload (will be JSON serialized)
            options: Publishing options

        Returns:
            Message ID from Valkey

        Raises:
            QueueError: If publish fails
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Prepare message data
            message_data = {
                "body": body,
                "attributes": options.attributes if options else {},
                "priority": options.priority if options else 0,
                "deduplication_id": options.deduplication_id if options else None,
                "timestamp": time.time(),
            }

            # Serialize to JSON
            serialized = json.dumps(message_data)

            # Add to stream with max length limit
            msg_id = await self._client.xadd(
                queue,
                {"data": serialized},
                maxlen=self._stream_max_length,
                approximate=True,
            )

            return str(msg_id)

        except ValkeyError as e:
            raise QueueError(f"Failed to publish message: {e}") from e

    async def consume(
        self,
        queue: str,
        options: ConsumeOptions | None = None,
    ) -> list[Message[Any]]:
        """Consume messages from Valkey stream.

        Args:
            queue: Stream name
            options: Consumption options

        Returns:
            List of messages
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            if options is None:
                options = ConsumeOptions()

            # Wait timeout in milliseconds
            wait_timeout = options.wait_timeout if options.wait_timeout else 1
            block = int(wait_timeout * 1000) if wait_timeout > 0 else 0

            # Max messages to consume
            count = options.max_messages

            # Read from stream
            streams = await self._client.xread(
                {queue: "0"},
                count=count,
                block=block,
            )

            if not streams:
                return []

            # Parse messages
            messages = []
            for _, stream_messages in streams:
                for msg_id, msg_data in stream_messages:
                    message = self._parse_message(msg_id, msg_data, queue)
                    if message:
                        messages.append(message)

            return messages

        except ValkeyError as e:
            raise QueueError(f"Failed to consume messages: {e}") from e

    async def consume_with_consumer_group(
        self,
        queue: str,
        consumer_group: str,
        consumer_name: str | None = None,
        options: ConsumeOptions | None = None,
    ) -> list[Message[Any]]:
        """Consume messages using consumer groups.

        This provides reliable message delivery with automatic acknowledgment tracking.

        Args:
            queue: Stream name
            consumer_group: Consumer group name
            consumer_name: Consumer instance name (auto-generated if None)
            options: Consumption options

        Returns:
            List of messages with receipts
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            if options is None:
                options = ConsumeOptions()

            # Auto-generate consumer name if not provided
            if consumer_name is None:
                consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"

            # Ensure consumer group exists
            with contextlib.suppress(ValkeyError):
                await self._client.xgroup_create(
                    queue,
                    consumer_group,
                    id="0",
                    mkstream=True,
                )

            # Wait timeout
            wait_timeout = options.wait_timeout if options.wait_timeout else 1
            block = int(wait_timeout * 1000) if wait_timeout > 0 else 0

            # Read from stream using consumer group
            streams = await self._client.xreadgroup(
                consumer_group,
                consumer_name,
                {queue: ">"},
                count=options.max_messages,
                block=block,
            )

            if not streams:
                return []

            # Parse messages
            messages = []
            for _, stream_messages in streams:
                for msg_id, msg_data in stream_messages:
                    message = self._parse_message(msg_id, msg_data, queue)
                    if message:
                        message.receipt = msg_id
                        message.status = MessageStatus.CONSUMED
                        messages.append(message)

            return messages

        except ValkeyError as e:
            raise QueueError(f"Failed to consume with consumer group: {e}") from e

    async def ack(self, receipt: str, queue: str) -> bool:
        """Acknowledge message processing.

        Uses the "default" consumer group for messages consumed without a group.
        For messages consumed with consumer groups, use ack_with_group().

        Args:
            receipt: Message ID to acknowledge
            queue: Stream name

        Returns:
            True if acknowledged
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Ensure default group exists
            with contextlib.suppress(ValkeyError):
                await self._client.xgroup_create(
                    queue,
                    "default",
                    id="0",
                    mkstream=True,
                )

            result = await self._client.xack(
                queue,
                "default",
                receipt,
            )
            return result > 0
        except ValkeyError as e:
            raise QueueError(f"Failed to acknowledge message: {e}") from e

    async def ack_with_group(self, receipt: str, queue: str, group: str) -> bool:
        """Acknowledge message with specific consumer group.

        Args:
            receipt: Message ID to acknowledge
            queue: Stream name
            group: Consumer group name

        Returns:
            True if acknowledged
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            result = await self._client.xack(
                queue,
                group,
                receipt,
            )
            return result > 0
        except ValkeyError as e:
            raise QueueError(f"Failed to acknowledge message: {e}") from e

    async def nack(
        self,
        receipt: str,
        queue: str,
        requeue: bool = False,
    ) -> bool:
        """Negative acknowledge message processing.

        Uses the "default" consumer group for messages consumed without a group.
        For messages consumed with consumer groups, use ack_with_group().

        Args:
            receipt: Message ID to negative acknowledge
            queue: Stream name
            requeue: Whether to requeue the message (not used in Valkey)

        Returns:
            True if nacked successfully
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Ensure default group exists
            with contextlib.suppress(ValkeyError):
                await self._client.xgroup_create(
                    queue,
                    "default",
                    id="0",
                    mkstream=True,
                )

            # For Valkey, nack = acknowledge to remove from pending
            # Note: requeue parameter is not used as Valkey handles requeue
            # through the pending message mechanism
            result = await self._client.xack(
                queue,
                "default",
                receipt,
            )
            return result > 0
        except ValkeyError as e:
            raise QueueError(f"Failed to nack message: {e}") from e

    async def purge(self, queue: str) -> int:
        """Purge all messages from a stream.

        Args:
            queue: Stream name

        Returns:
            Number of messages purged
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Get current length
            length = await self._client.xlen(queue)

            # Delete all messages by trimming to 0
            if length > 0:
                await self._client.xtrim(queue, maxlen=0, approximate=True)

            return length
        except ValkeyError as e:
            raise QueueError(f"Failed to purge queue: {e}") from e

    async def get_queue_stats(self, queue: str) -> dict[str, Any]:
        """Get queue statistics.

        Args:
            queue: Stream name

        Returns:
            Dictionary with queue statistics
        """
        if not self._connected:
            return {}

        try:
            # Basic stream info
            length = await self._client.xlen(queue)

            # Get stream info (might fail if stream doesn't exist)
            try:
                info = await self._client.xinfo_stream(queue)
            except ValkeyError:
                info = {}

            # Get pending messages info (might fail if group doesn't exist)
            try:
                pending = await self._client.xpending(
                    queue,
                    "default-group",
                )
            except ValkeyError:
                pending = 0

            return {
                "length": length,
                "stream_info": info,
                "pending": pending,
            }
        except ValkeyError:
            # Stream might not exist
            return {"length": 0, "pending": 0}

    async def stream(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        queue: str,
        options: ConsumeOptions | None = None,
    ) -> AsyncIterator[Message[Any]]:
        """Stream messages from Valkey.

        Note: This is an async generator, not a coroutine.
        Streams messages starting from the current time, continuing with new messages.

        Args:
            queue: Stream name
            options: Consumption options

        Yields:
            Messages as they arrive
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        if options is None:
            options = ConsumeOptions()

        wait_timeout = options.wait_timeout if options.wait_timeout else 1
        block = int(wait_timeout * 1000) if wait_timeout > 0 else 0

        # Track last message ID to avoid duplicates
        # Use "$" to start from new messages only
        last_id = "$"

        while True:
            try:
                streams = await self._client.xread(
                    {queue: last_id},
                    count=options.max_messages,
                    block=block,
                )

                if not streams:
                    continue

                for _, stream_messages in streams:
                    for msg_id, msg_data in stream_messages:
                        message = self._parse_message(msg_id, msg_data, queue)
                        if message:
                            yield message
                            # Update last_id to this message ID for next iteration
                            # Use the ID directly since xread returns messages AFTER this ID
                            last_id = msg_id

            except ValkeyError:
                break
            except asyncio.CancelledError:
                break

    async def subscribe(
        self,
        queues: list[str],
        callback: Callable[[Message[Any]], Awaitable[None]],
        options: ConsumeOptions | None = None,
    ) -> None:
        """Subscribe to multiple queues with callback.

        Args:
            queues: List of queue names
            callback: Async message handler
            options: Consumption options
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        if options is None:
            options = ConsumeOptions()

        # Create consumer group for each queue
        consumer_group = f"subscribe-group-{uuid.uuid4().hex[:8]}"
        consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"

        # Ensure groups exist
        for queue in queues:
            with contextlib.suppress(ValkeyError):
                await self._client.xgroup_create(
                    queue,
                    consumer_group,
                    id="0",
                    mkstream=True,
                )

        wait_timeout = options.wait_timeout if options.wait_timeout else 1
        block = int(wait_timeout * 1000) if wait_timeout > 0 else 0

        while True:
            try:
                # Read from all queues
                stream_dict = dict.fromkeys(queues, ">")
                streams = await self._client.xreadgroup(
                    consumer_group,
                    consumer_name,
                    stream_dict,
                    count=options.max_messages,
                    block=block,
                )

                if not streams:
                    continue

                for stream_name, stream_messages in streams:
                    for msg_id, msg_data in stream_messages:
                        message = self._parse_message(msg_id, msg_data, stream_name)
                        if message:
                            message.receipt = msg_id
                            await callback(message)
                            # Auto-ack after callback
                            await self.ack_with_group(msg_id, stream_name, consumer_group)

            except ValkeyError:
                break
            except asyncio.CancelledError:
                break

    async def move(
        self,
        message: Message[Any],
        target_queue: str,
        preserve_receipt: bool = True,
    ) -> bool:
        """Move message to another queue.

        Args:
            message: Message to move
            target_queue: Target queue name
            preserve_receipt: Keep original receipt (not used in current implementation)

        Returns:
            True if moved successfully
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Publish to target queue
            options = PublishOptions(
                attributes=message.attributes,
                priority=message.attributes.get("priority", 0),
            )

            msg_id = await self.publish(target_queue, message.body, options)

            # Acknowledge original message if it has a receipt
            if message.receipt:
                await self.ack(message.receipt, message.queue)

            return bool(msg_id)
        except Exception as e:
            raise QueueError(f"Failed to move message: {e}") from e

    async def extend_visibility_timeout(
        self,
        receipt: str,
        queue: str,
        timeout: int,
    ) -> bool:
        """Extend visibility timeout for a message.

        Args:
            receipt: Message receipt handle
            queue: Stream name
            timeout: New visibility timeout in seconds

        Returns:
            True if extended successfully
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Claim the message with new idle time
            claims = await self._client.xclaim(
                queue,
                "default-group",
                f"consumer-{uuid.uuid4().hex[:8]}",
                min_idle_time=timeout * 1000,  # Convert to milliseconds
                message_ids=[receipt],
            )
            return len(claims) > 0
        except ValkeyError:
            return False

    async def claim_pending(
        self,
        queue: str,
        consumer_group: str,
        consumer_name: str,
        min_idle_time: int = 30000,
    ) -> list[Message[Any]]:
        """Claim pending messages that have exceeded idle time.

        Args:
            queue: Stream name
            consumer_group: Consumer group name
            consumer_name: Consumer name to claim to
            min_idle_time: Minimum idle time in milliseconds

        Returns:
            List of claimed messages
        """
        if not self._connected:
            raise QueueError("Not connected to Valkey")

        try:
            # Get pending messages
            pending_info = await self._client.xpending_range(
                queue,
                consumer_group,
                "-",
                "+",
                100,
            )

            if not pending_info:
                return []

            # pending_info is a list of dicts with keys: message_id, consumer, time_since_delivered, times_delivered
            # Filter messages that have been idle too long
            messages_to_claim = [
                pending["message_id"]
                for pending in pending_info
                if pending["time_since_delivered"] >= min_idle_time
            ]

            if not messages_to_claim:
                return []

            # Claim the messages
            claimed = await self._client.xclaim(
                queue,
                consumer_group,
                consumer_name,
                min_idle_time=min_idle_time,
                message_ids=messages_to_claim,
            )

            # Parse claimed messages
            result = []
            for msg_id, msg_data in claimed:
                message = self._parse_message(msg_id, msg_data, queue)
                if message:
                    message.receipt = msg_id
                    result.append(message)

            return result

        except ValkeyError as e:
            raise QueueError(f"Failed to claim pending messages: {e}") from e

    def _parse_message(
        self,
        msg_id: str,
        msg_data: dict[str, str],
        queue: str,
    ) -> Message[Any] | None:
        """Parse Valkey stream message into Message object.

        Args:
            msg_id: Valkey message ID
            msg_data: Raw message data
            queue: Queue/stream name

        Returns:
            Message object or None if parsing fails
        """
        try:
            # Get the data field
            data_str = msg_data.get("data")
            if not data_str:
                return None

            # Parse JSON
            parsed = json.loads(data_str)

            # Create message
            message = Message[Any](
                id=str(uuid.uuid4()),
                body=parsed.get("body"),
                queue=queue,
                status=MessageStatus.PUBLISHED,
                receipt=msg_id,
                attributes=parsed.get("attributes", {}),
            )

            # Add priority if present
            if "priority" in parsed:
                message.attributes["priority"] = parsed["priority"]

            # Add timestamp
            if "timestamp" in parsed:
                message.attributes["timestamp"] = parsed["timestamp"]

            # Add deduplication_id if present
            if parsed.get("deduplication_id"):
                message.attributes["deduplication_id"] = parsed["deduplication_id"]

            return message

        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    @property
    def client(self) -> Valkey:
        """Get the underlying Valkey client."""
        return self._client

    @property
    def is_connected(self) -> bool:
        """Check if connected to Valkey."""
        return self._connected
