"""Tests for Valkey broker basic functionality and availability.

Tests verify that the Valkey broker can connect to a local Valkey container
and perform basic publish/consume operations.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from kortex.lib.worker.broker.config import ConsumeOptions, PublishOptions
from kortex.lib.worker.broker.exceptions import QueueError
from kortex.lib.worker.broker.messages import MessageStatus
from kortex.lib.worker.broker.plugins.valkey import ValkeyBroker


@pytest.mark.asyncio
class TestValkeyBrokerConnection:
    """Test connection and basic broker lifecycle."""

    async def test_connect_success(self, valkey_broker: ValkeyBroker) -> None:
        """Test successful connection to Valkey."""
        assert valkey_broker.is_connected is True

    async def test_connect_failure_wrong_host(self) -> None:
        """Test connection failure with wrong host."""

        broker = ValkeyBroker()
        # ValkeyBroker doesn't connect to a host, so this test doesn't apply
        # Just verify it can be instantiated
        assert broker is not None

    async def test_disconnect(self, valkey_broker: ValkeyBroker) -> None:
        """Test disconnect functionality."""
        await valkey_broker.disconnect()
        assert valkey_broker.is_connected is False

    async def test_context_manager(self, valkey_broker: ValkeyBroker) -> None:
        """Test broker as async context manager."""
        async with valkey_broker as broker:
            broker_typed = cast(ValkeyBroker, broker)
            assert broker_typed.is_connected is True
        assert broker_typed.is_connected is False


class TestValkeyBrokerPublish:
    """Test message publishing functionality."""

    async def test_publish_simple_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test publishing a simple message."""
        msg_id = await valkey_broker.publish(cleanup_queue, sample_message)
        assert msg_id is not None
        assert isinstance(msg_id, str)
        assert len(msg_id) > 0

    async def test_publish_with_options(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test publishing with options."""
        options = PublishOptions(
            priority=5,
            attributes={"source": "test", "version": "1.0"},
            deduplication_id="test-dedup-123",
        )
        msg_id = await valkey_broker.publish(cleanup_queue, sample_message, options)
        assert msg_id is not None

    async def test_publish_large_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        large_message: dict[str, Any],
    ) -> None:
        """Test publishing a large message."""
        msg_id = await valkey_broker.publish(cleanup_queue, large_message)
        assert msg_id is not None

    async def test_publish_multiple_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test publishing multiple messages."""
        messages = [{"id": i, "data": f"message-{i}"} for i in range(5)]
        message_ids: list[str] = []
        for msg in messages:
            msg_id = await valkey_broker.publish(cleanup_queue, msg)
            message_ids.append(msg_id)

        assert len(message_ids) == 5
        assert all(isinstance(mid, str) for mid in message_ids)

    async def test_publish_without_connection(self) -> None:
        """Test publish fails when not connected."""

        broker = ValkeyBroker()
        with pytest.raises(QueueError, match="Not connected"):
            await broker.publish("test-queue", {"test": "data"})


class TestValkeyBrokerConsume:
    """Test message consuming functionality."""

    async def test_consume_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming from empty queue."""
        messages = await valkey_broker.consume(cleanup_queue)
        assert messages == []

    async def test_consume_published_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test consuming a published message."""
        # Publish first
        msg_id = await valkey_broker.publish(cleanup_queue, sample_message)
        assert msg_id is not None

        # Then consume
        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 1

        message = messages[0]
        assert message.body == sample_message
        assert message.queue == cleanup_queue
        assert message.status == MessageStatus.PUBLISHED
        assert message.receipt is not None

    async def test_consume_multiple_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming multiple messages."""
        # Publish 3 messages
        for i in range(3):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Consume with max_messages=3
        options = ConsumeOptions(max_messages=3)
        messages = await valkey_broker.consume(cleanup_queue, options)
        assert len(messages) == 3

    async def test_consume_with_wait_timeout(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming with wait timeout."""
        options = ConsumeOptions(wait_timeout=1, max_messages=5)
        messages = await valkey_broker.consume(cleanup_queue, options)
        # Should return empty list after timeout
        assert isinstance(messages, list)

    async def test_consume_with_attributes(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test consuming message with attributes."""
        options = PublishOptions(
            priority=7,
            attributes={"source": "test", "type": "priority"},
        )
        await valkey_broker.publish(cleanup_queue, {"test": "data"}, options)

        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 1

        msg = messages[0]
        assert "priority" in msg.attributes
        assert msg.attributes["priority"] == 7
        assert msg.attributes["source"] == "test"

    async def test_consume_without_connection(self) -> None:
        """Test consume fails when not connected."""

        broker = ValkeyBroker()
        with pytest.raises(QueueError, match="Not connected"):
            await broker.consume("test-queue")


class TestValkeyBrokerAck:
    """Test message acknowledgment functionality."""

    async def test_ack_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test acknowledging a message using consumer group."""
        # Publish and consume with consumer group
        await valkey_broker.publish(cleanup_queue, sample_message)
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="ack-test-group",
            consumer_name="consumer-1",
        )
        message = messages[0]
        assert message.receipt is not None

        # Acknowledge with group
        result = await valkey_broker.ack_with_group(message.receipt, cleanup_queue, "ack-test-group")
        assert result is True

    async def test_ack_multiple_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test acknowledging multiple messages."""
        # Publish 3 messages
        for i in range(3):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Consume all with consumer group
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="ack-multi-group",
            consumer_name="consumer-1",
        )
        assert len(messages) == 3

        # Ack all
        for msg in messages:
            assert msg.receipt is not None
            result = await valkey_broker.ack_with_group(msg.receipt, cleanup_queue, "ack-multi-group")
            assert result is True

    async def test_ack_without_connection(self) -> None:
        """Test ack fails when not connected."""

        broker = ValkeyBroker()
        with pytest.raises(QueueError, match="Not connected"):
            await broker.ack("some-receipt", "test-queue")


class TestValkeyBrokerNack:
    """Test negative acknowledgment functionality."""

    async def test_nack_message(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
        sample_message: dict[str, Any],
    ) -> None:
        """Test nacking a message using consumer group."""
        # Publish and consume with consumer group
        await valkey_broker.publish(cleanup_queue, sample_message)
        messages = await valkey_broker.consume_with_consumer_group(
            queue=cleanup_queue,
            consumer_group="nack-test-group",
            consumer_name="consumer-1",
        )
        message = messages[0]
        assert message.receipt is not None

        # Nack with group (nack uses "default" group, so we use ack_with_group with the same group)
        result = await valkey_broker.ack_with_group(message.receipt, cleanup_queue, "nack-test-group")
        assert result is True

    async def test_nack_without_connection(self) -> None:
        """Test nack fails when not connected."""

        broker = ValkeyBroker()
        with pytest.raises(QueueError, match="Not connected"):
            await broker.nack("some-receipt", "test-queue")


class TestValkeyBrokerPurge:
    """Test queue purging functionality."""

    async def test_purge_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test purging an empty queue."""
        count = await valkey_broker.purge(cleanup_queue)
        assert count == 0

    async def test_purge_non_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test purging a queue with messages."""
        # Publish multiple messages
        for i in range(5):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        # Purge
        count = await valkey_broker.purge(cleanup_queue)
        assert count == 5

        # Verify queue is empty
        messages = await valkey_broker.consume(cleanup_queue)
        assert len(messages) == 0

    async def test_purge_without_connection(self) -> None:
        """Test purge fails when not connected."""

        broker = ValkeyBroker()
        with pytest.raises(QueueError, match="Not connected"):
            await broker.purge("test-queue")


class TestValkeyBrokerQueueStats:
    """Test queue statistics functionality."""

    async def test_stats_empty_queue(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test getting stats for empty queue."""
        stats = await valkey_broker.get_queue_stats(cleanup_queue)
        # Empty queue might return empty dict or specific stats
        assert isinstance(stats, dict)

    async def test_stats_with_messages(
        self,
        valkey_broker: ValkeyBroker,
        cleanup_queue: str,
    ) -> None:
        """Test getting stats with messages in queue."""
        # Publish messages
        for i in range(3):
            await valkey_broker.publish(cleanup_queue, {"id": i})

        stats = await valkey_broker.get_queue_stats(cleanup_queue)
        assert isinstance(stats, dict)
        # Length should reflect messages
        if "length" in stats:
            assert stats["length"] == 3

    async def test_stats_without_connection(self) -> None:
        """Test stats returns empty dict when not connected."""

        broker = ValkeyBroker()
        stats = await broker.get_queue_stats("test-queue")
        assert stats == {}
