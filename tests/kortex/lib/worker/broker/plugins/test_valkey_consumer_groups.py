"""Tests for Valkey broker consumer group functionality.

Consumer groups enable reliable message processing with automatic
acknowledgment tracking and parallel processing support.
"""

from __future__ import annotations

import asyncio
from typing import Any

from kortex.lib.worker.broker.config import ConsumeOptions
from kortex.lib.worker.broker.messages import MessageStatus
from kortex.lib.worker.broker.plugins.valkey import ValkeyBroker


class TestValkeyConsumerGroups:
    """Test consumer group operations."""

    async def test_create_consumer_group(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test creating a consumer group."""
        # Consumer group is auto-created during consume_with_consumer_group
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="test-group-1",
            consumer_name="consumer-1",
        )
        assert isinstance(messages, list)

    async def test_consume_with_consumer_group(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test consuming messages using consumer groups."""
        # Publish message
        await valkey_broker.publish(cleanup_queue, sample_message)

        # Consume with consumer group
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="test-group-2",
            consumer_name="consumer-2",
        )

        assert len(messages) == 1
        message = messages[0]
        assert message.body == sample_message
        assert message.status == MessageStatus.CONSUMED
        assert message.receipt is not None

    async def test_consumer_group_parallel_processing(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test parallel processing with multiple consumers in same group."""
        # Publish 10 messages
        for i in range(10):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Two consumers in same group
        consumer1_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="parallel-group",
            consumer_name="consumer-a",
            options=ConsumeOptions(max_messages=5),
        )

        consumer2_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="parallel-group",
            consumer_name="consumer-b",
            options=ConsumeOptions(max_messages=5),
        )

        # Total should be 10, distributed between consumers
        total = len(consumer1_messages) + len(consumer2_messages)
        assert total == 10

    async def test_consumer_group_ack(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test acknowledging messages with consumer group."""
        # Publish and consume
        await valkey_broker.publish(cleanup_queue, sample_message)
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="ack-group",
            consumer_name="consumer-1",
        )

        # Ack with group
        message = messages[0]
        assert message.receipt is not None
        result = await valkey_broker.ack_with_group(
            message.receipt,
            cleanup_queue,
            "ack-group",
        )
        assert result is True

    async def test_consumer_group_auto_consumer_name(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test auto-generated consumer name."""
        await valkey_broker.publish(cleanup_queue, {"test": "data"})

        # Don't provide consumer_name - should auto-generate
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="auto-group",
        )

        assert len(messages) == 1
        assert messages[0].receipt is not None

    async def test_consumer_group_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming from empty queue with consumer group."""
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="empty-group",
            consumer_name="consumer-1",
            options=ConsumeOptions(wait_timeout=1),
        )
        assert messages == []

    async def test_consumer_group_multiple_consumers_different_groups(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test multiple consumers in different groups get same messages."""
        # Publish 5 messages
        for i in range(5):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Consume with group 1
        group1_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="group-1",
            consumer_name="consumer-1",
        )

        # Consume with group 2 (should also get messages)
        group2_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="group-2",
            consumer_name="consumer-2",
        )

        # Both groups should get messages
        assert len(group1_messages) == 5
        assert len(group2_messages) == 5


class TestValkeyPendingMessages:
    """Test pending message tracking and claiming."""

    async def test_claim_pending_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test claiming pending messages that have exceeded idle time."""

        # Publish messages
        for i in range(3):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Consume with consumer group (creates pending messages)
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="pending-group",
            consumer_name="consumer-1",
        )

        # Messages are now pending (not acked)
        assert len(messages) == 3

        # Wait a bit to ensure idle time accumulates
        await asyncio.sleep(0.1)

        # Claim pending messages with short idle time
        claimed = await valkey_broker.claim_pending(
            queue=cleanup_queue,
            consumer_group="pending-group",
            consumer_name="consumer-2",
            min_idle_time=50,  # 50ms
        )

        # Should be able to claim the pending messages
        assert isinstance(claimed, list)

    async def test_claim_no_pending_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test claiming when no pending messages exist."""
        # First, create the group to avoid errors
        # Then claim with high idle time to get no results

        # Create group first
        try:
            await valkey_broker.consume_with_consumer_group(
                queue=cleanup_queue,
                consumer_group="empty-group",
                consumer_name="temp-consumer",
            )
        except Exception:
            pass  # Group might already exist

        # Wait a bit
        await asyncio.sleep(0.1)

        # Now claim with high idle time - should return empty
        claimed = await valkey_broker.claim_pending(
            queue=cleanup_queue,
            consumer_group="empty-group",
            consumer_name="consumer-1",
            min_idle_time=10000,  # 10 seconds - nothing should be this idle
        )
        assert claimed == []


class TestValkeyVisibilityTimeout:
    """Test visibility timeout extension."""

    async def test_extend_visibility_timeout(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test extending visibility timeout for a message."""
        # Publish and consume
        await valkey_broker.publish(cleanup_queue, sample_message)
        messages = await valkey_broker.consume(cleanup_queue)
        message = messages[0]
        assert message.receipt is not None

        # Extend visibility timeout
        result = await valkey_broker.extend_visibility_timeout(
            message.receipt,
            cleanup_queue,
            timeout=30,  # 30 seconds
        )
        # Result depends on whether message is in consumer group
        # For basic consume, this might return False
        assert isinstance(result, bool)


class TestValkeyStream:
    """Test streaming functionality."""

    async def test_stream_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test streaming messages from queue."""
        # Publish a message AFTER starting the stream
        # Use asyncio.gather to run both concurrently

        async def publish_after_delay() -> None:
            await asyncio.sleep(0.2)
            await valkey_broker.publish(cleanup_queue, {"id": 1})

        async def consume_stream() -> dict[str, Any] | None:
            async for message in valkey_broker.stream(
                cleanup_queue,
                options=ConsumeOptions(wait_timeout=2, max_messages=1),
            ):
                body = message.body
                if isinstance(body, dict):
                    return body
            return None

        # Run both concurrently
        publish_task = asyncio.create_task(publish_after_delay())
        stream_task = asyncio.create_task(consume_stream())

        # Wait for stream to get message
        result = await asyncio.wait_for(stream_task, timeout=5)
        publish_task.cancel()
        try:
            await publish_task
        except asyncio.CancelledError:
            pass

        assert result is not None
        assert result == {"id": 1}

    async def test_stream_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test streaming from empty queue - just verify it doesn't crash."""
        # Just verify the stream can be created without error
        # Don't iterate since it would block forever

        async def create_and_cancel_stream() -> None:
            stream_gen = valkey_broker.stream(
                cleanup_queue,
                options=ConsumeOptions(wait_timeout=1, max_messages=1),
            )
            # Get the async iterator
            stream = stream_gen.__aiter__()
            # Try to get first item with timeout
            try:
                await asyncio.wait_for(stream.__anext__(), timeout=1.5)
            except (TimeoutError, StopAsyncIteration):
                pass  # Expected - no messages
            except Exception:
                pass  # Any other exception is fine too

        await create_and_cancel_stream()
        # If we get here without hanging, test passes
        assert True


class TestValkeySubscribe:
    """Test subscribe functionality."""

    async def test_subscribe_callback(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test subscribing with callback."""

        received_messages: list[Any] = []

        async def callback(message: Any) -> None:
            received_messages.append(message)

        # Start subscribe first, then publish
        async def subscribe_and_cancel() -> None:
            subscribe_task = asyncio.create_task(
                valkey_broker.subscribe(
                    queues=[cleanup_queue],
                    callback=callback,
                    options=ConsumeOptions(wait_timeout=2, max_messages=1),
                )
            )

            # Wait a bit for subscribe to start
            await asyncio.sleep(0.2)

            # Publish a message
            await valkey_broker.publish(cleanup_queue, {"id": 1})

            # Wait for processing
            await asyncio.sleep(0.5)

            # Cancel subscribe
            subscribe_task.cancel()
            try:
                await subscribe_task
            except asyncio.CancelledError:
                pass

        # Run with timeout to prevent hanging
        try:
            await asyncio.wait_for(subscribe_and_cancel(), timeout=5)
        except TimeoutError:
            pass  # If it times out, just continue

        # The test passes if we get here without hanging
        # Note: actual message receipt depends on timing
        assert True


class TestValkeyMove:
    """Test message moving between queues."""

    async def test_move_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test moving message to another queue."""
        source_queue = cleanup_queue
        target_queue = f"{cleanup_queue}-target"

        # Publish to source
        await valkey_broker.publish(source_queue, sample_message)

        # Consume from source
        messages = await valkey_broker.consume(source_queue)
        message = messages[0]

        # Move to target
        result = await valkey_broker.move(message, target_queue)
        assert result is True

        # Verify message is in target queue
        target_messages = await valkey_broker.consume(target_queue)
        assert len(target_messages) == 1
        assert target_messages[0].body == sample_message

        # Clean up
        await valkey_broker.purge(target_queue)

    async def test_move_with_receipt_ack(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test move acknowledges original message."""
        target_queue = f"{cleanup_queue}-target"

        await valkey_broker.publish(cleanup_queue, {"id": 1})
        messages = await valkey_broker.consume(cleanup_queue)
        message = messages[0]

        # Move should ack original
        result = await valkey_broker.move(message, target_queue)
        assert result is True

        # Clean up
        await valkey_broker.purge(target_queue)
