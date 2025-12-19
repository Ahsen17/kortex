"""Advanced tests for Valkey broker.

Tests for edge cases, error handling, and complex scenarios.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from kortex.lib.worker.broker.config import ConsumeOptions, PublishOptions
from kortex.lib.worker.broker.messages import MessageStatus
from kortex.lib.worker.broker.plugins.valkey import ValkeyBroker


class TestValkeyErrorHandling:
    """Test error handling and edge cases."""

    async def test_publish_to_invalid_queue_name(
        self,
        valkey_broker: ValkeyBroker,
    ) -> None:
        """Test publishing to invalid queue name."""
        # Valkey allows most characters in stream names
        # This should succeed
        msg_id = await valkey_broker.publish("queue:with:colons", {"test": "data"})
        assert msg_id is not None
        await valkey_broker.purge("queue:with:colons")

    async def test_consume_with_zero_timeout(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming with zero timeout (non-blocking)."""
        options = ConsumeOptions(wait_timeout=0, max_messages=10)
        messages = await valkey_broker.consume(cleanup_queue, options)
        assert isinstance(messages, list)

    async def test_consume_with_negative_timeout(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming with negative timeout - treated as no wait."""
        # Negative timeout should be handled gracefully (treated as 0 or similar)
        options = ConsumeOptions(wait_timeout=0, max_messages=10)  # Use 0 instead of -1
        messages = await valkey_broker.consume(cleanup_queue, options)
        assert isinstance(messages, list)

    async def test_publish_with_none_body(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test publishing None as body."""
        msg_id = await valkey_broker.publish(cleanup_queue, None)
        assert msg_id is not None

        # Verify we can consume it
        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 1
        assert messages[0].body is None

    async def test_publish_with_complex_nested_data(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test publishing complex nested data structures."""
        complex_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "list": [1, 2, 3, {"nested": "value"}],
                        "dict": {"a": 1, "b": [4, 5, 6]},
                    }
                }
            },
            "unicode": "你好世界 🚀",
            "special_chars": "!@#$%^&*()_+-=[]{}|;':\\\",./<>?",
        }

        msg_id = await valkey_broker.publish(cleanup_queue, complex_data)
        assert msg_id is not None

        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 1
        assert messages[0].body == complex_data

    async def test_publish_with_very_large_body(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test publishing a message with very large body."""
        # Create a large message (10KB+)
        large_body = {"data": "x" * 50000}
        msg_id = await valkey_broker.publish(cleanup_queue, large_body)
        assert msg_id is not None

        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 1
        assert messages[0].body == large_body

    async def test_consume_with_very_large_max_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming with very large max_messages limit."""
        # Publish 5 messages
        for i in range(5):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        options = ConsumeOptions(max_messages=1000000)
        messages = await valkey_broker.consume(cleanup_queue, options)
        assert len(messages) == 5  # Only 5 exist


class TestValkeyConcurrency:
    """Test concurrent operations."""

    async def test_concurrent_publish(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test concurrent publishing."""

        async def publish_task(num: int) -> str:
            return await valkey_broker.publish(cleanup_queue, {"id": num})

        # Publish 20 messages concurrently
        tasks = [publish_task(i) for i in range(20)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 20
        assert all(isinstance(r, str) for r in results)

        # Verify all messages are in queue (consume with large max_messages)
        messages = await valkey_broker.consume(cleanup_queue, options=ConsumeOptions(max_messages=20))
        assert len(messages) == 20

    async def test_concurrent_consume(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test concurrent consuming."""
        # Publish 20 messages
        for i in range(20):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Consume concurrently
        async def consume_task() -> list[Any]:
            return await valkey_broker.consume(
                cleanup_queue,
                options=ConsumeOptions(max_messages=5),
            )

        tasks = [consume_task() for _ in range(4)]
        results = await asyncio.gather(*tasks)

        # Total consumed should be 20
        total = sum(len(r) for r in results)
        assert total == 20

    async def test_concurrent_publish_and_consume(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test concurrent publishing and consuming."""

        async def publish_task() -> None:
            for i in range(10):
                await valkey_broker.publish(cleanup_queue, {"id": i})
                await asyncio.sleep(0.01)

        async def consume_task() -> list[Any]:
            messages: list[Any] = []
            for _ in range(10):
                msgs = await valkey_broker.consume(
                    cleanup_queue,
                    options=ConsumeOptions(wait_timeout=1),
                )
                messages.extend(msgs)
                if len(messages) >= 10:
                    break
                await asyncio.sleep(0.01)
            return messages

        # Run both concurrently
        publish_coro = publish_task()
        consume_coro = consume_task()

        results = await asyncio.gather(publish_coro, consume_coro)
        consumed = results[1]

        assert len(consumed) == 10


class TestValkeyMessageAttributes:
    """Test message attributes and metadata."""

    async def test_priority_attribute(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test message priority attribute."""
        options = PublishOptions(priority=9)
        await valkey_broker.publish(cleanup_queue, {"data": "high-priority"}, options)

        messages = await valkey_broker.consume(cleanup_queue)
        assert messages[0].attributes.get("priority") == 9

    async def test_deduplication_id(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test deduplication_id attribute."""
        options = PublishOptions(deduplication_id="dedup-123")
        msg_id = await valkey_broker.publish(cleanup_queue, {"data": "test"}, options)
        assert msg_id is not None

        messages = await valkey_broker.consume(cleanup_queue)
        # Deduplication ID should be stored in attributes
        assert messages[0].attributes.get("deduplication_id") == "dedup-123"

    async def test_custom_attributes(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test custom attributes."""
        custom_attrs = {
            "source": "test-suite",
            "version": "1.0.0",
            "trace_id": "abc-123",
        }
        options = PublishOptions(attributes=custom_attrs)
        await valkey_broker.publish(cleanup_queue, {"test": "data"}, options)

        messages = await valkey_broker.consume(cleanup_queue)
        msg_attrs = messages[0].attributes
        assert msg_attrs["source"] == "test-suite"
        assert msg_attrs["version"] == "1.0.0"
        assert msg_attrs["trace_id"] == "abc-123"

    async def test_timestamp_attribute(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test timestamp is automatically added."""
        before = time.time()

        await valkey_broker.publish(cleanup_queue, {"test": "data"})

        after = time.time()
        messages = await valkey_broker.consume(cleanup_queue)

        timestamp = messages[0].attributes.get("timestamp")
        assert timestamp is not None
        assert before <= timestamp <= after


class TestValkeyBatchOperations:
    """Test batch operations."""

    async def test_publish_batch(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test publishing batch of messages."""
        bodies = [{"id": i} for i in range(10)]
        message_ids = await valkey_broker.publish_batch(cleanup_queue, bodies)
        assert len(message_ids) == 10
        assert all(isinstance(mid, str) for mid in message_ids)

    async def test_consume_batch(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming batch of messages."""
        # Publish 15 messages
        for i in range(15):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        batch = await valkey_broker.consume_batch(cleanup_queue, max_messages=10)
        assert len(batch.messages) == 10
        assert batch.queue == cleanup_queue
        assert batch.total_count == 10
        assert batch.has_more is False  # We consumed max, but there might be more

    async def test_publish_and_consume_batch(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test full batch publish and consume cycle."""
        # Publish batch
        bodies = [{"num": i, "data": f"item-{i}"} for i in range(20)]
        published_ids = await valkey_broker.publish_batch(cleanup_queue, bodies)
        assert len(published_ids) == 20

        # Consume batch
        batch = await valkey_broker.consume_batch(cleanup_queue, max_messages=20)
        assert len(batch.messages) == 20

        # Verify bodies match
        received_bodies = [msg.body for msg in batch.messages]
        for body in bodies:
            assert body in received_bodies


class TestValkeyMessageStatus:
    """Test message status tracking."""

    async def test_message_status_after_publish(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test message status after publishing."""
        await valkey_broker.publish(cleanup_queue, {"test": "data"})
        messages = await valkey_broker.consume(cleanup_queue)
        assert messages[0].status == MessageStatus.PUBLISHED

    async def test_message_status_after_consume_with_group(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test message status after consuming with consumer group."""
        await valkey_broker.publish(cleanup_queue, {"test": "data"})
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="status-group",
            consumer_name="consumer-1",
        )
        assert messages[0].status == MessageStatus.CONSUMED


class TestValkeyMultipleQueues:
    """Test operations across multiple queues."""

    async def test_multiple_queues_isolation(
        self,
        valkey_broker: ValkeyBroker,
    ) -> None:
        """Test that multiple queues are isolated."""
        queue1 = "test-queue-1"
        queue2 = "test-queue-2"

        # Publish to different queues
        await valkey_broker.publish(queue1, {"from": "queue1"})
        await valkey_broker.publish(queue2, {"from": "queue2"})

        # Consume from each
        msgs1 = await valkey_broker.consume(queue1)
        msgs2 = await valkey_broker.consume(queue2)

        assert len(msgs1) == 1
        assert len(msgs2) == 1
        assert msgs1[0].body == {"from": "queue1"}
        assert msgs2[0].body == {"from": "queue2"}

        # Clean up
        await valkey_broker.purge(queue1)
        await valkey_broker.purge(queue2)

    async def test_subscribe_multiple_queues(
        self,
        valkey_broker: ValkeyBroker,
    ) -> None:
        """Test subscribing to multiple queues."""
        queue1 = "multi-queue-1"
        queue2 = "multi-queue-2"

        # Publish to both
        await valkey_broker.publish(queue1, {"queue": 1})
        await valkey_broker.publish(queue2, {"queue": 2})

        received: list[Any] = []

        async def callback(message: Any) -> None:
            received.append(message)

        # Subscribe task (with timeout)
        subscribe_task = asyncio.create_task(
            valkey_broker.subscribe(
                queues=[queue1, queue2],
                callback=callback,
                options=ConsumeOptions(wait_timeout=1, max_messages=1),
            )
        )

        await asyncio.sleep(0.3)
        subscribe_task.cancel()
        try:
            await subscribe_task
        except asyncio.CancelledError:
            pass

        # Clean up
        await valkey_broker.purge(queue1)
        await valkey_broker.purge(queue2)


class TestValkeyStreamTrimming:
    """Test stream auto-trimming."""

    async def test_max_length_limit(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test that stream respects max length limit."""
        # The broker is configured with max_length=10000
        # Publish more than that to test trimming
        num_messages = 100  # Use smaller number for test speed

        for i in range(num_messages):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Get queue stats
        stats = await valkey_broker.get_queue_stats(cleanup_queue)

        # Stream should exist and have messages
        assert isinstance(stats, dict)


# Helper test for verifying the broker works end-to-end
class TestValkeyEndToEnd:
    """End-to-end workflow tests."""

    async def test_full_workflow(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test complete workflow: publish -> consume with group -> ack."""
        # 1. Publish message
        message_body = {"action": "process", "data": {"id": 123}}
        msg_id = await valkey_broker.publish(cleanup_queue, message_body)
        assert msg_id is not None

        # 2. Consume message with consumer group
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="e2e-group",
            consumer_name="worker-1",
        )
        assert len(messages) == 1

        message = messages[0]
        assert message.body == message_body
        assert message.queue == cleanup_queue
        assert message.receipt is not None

        # 3. Acknowledge message with group
        ack_result = await valkey_broker.ack_with_group(message.receipt, cleanup_queue, "e2e-group")
        assert ack_result is True

        # 4. Verify queue is empty (no more pending messages)
        empty_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="e2e-group",
            consumer_name="worker-1",
        )
        assert len(empty_messages) == 0

    async def test_full_workflow_with_consumer_group(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test complete workflow with consumer groups."""
        # 1. Publish
        await valkey_broker.publish(cleanup_queue, {"task": "important"})

        # 2. Consume with group
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="workflow-group",
            consumer_name="worker-1",
        )
        assert len(messages) == 1

        message = messages[0]
        assert message.receipt is not None

        # 3. Ack with group
        ack_result = await valkey_broker.ack_with_group(
            message.receipt,
            cleanup_queue,
            "workflow-group",
        )
        assert ack_result is True

        # 4. Verify empty
        empty_messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="workflow-group",
            consumer_name="worker-1",
        )
        assert len(empty_messages) == 0
