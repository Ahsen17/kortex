"""Test fixtures for Valkey broker tests."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest

from kortex.lib.worker.broker.plugins.valkey import ValkeyBroker


@pytest.fixture
async def valkey_broker() -> AsyncGenerator[ValkeyBroker, None]:
    """Create and connect a valkey broker instance for testing.

    Uses valkey broker to avoid external dependencies.
    """
    broker = ValkeyBroker(host="valkey", port=6379)
    await broker.connect()
    try:
        yield broker
    finally:
        await broker.disconnect()


@pytest.fixture
async def cleanup_queue(valkey_broker: ValkeyBroker) -> AsyncGenerator[str, None]:
    """Create a unique test queue and clean it up after test.

    Returns:
        Unique queue name for testing.
    """
    queue_name = f"test-queue-{uuid.uuid4().hex[:8]}"
    try:
        yield queue_name
    finally:
        # Clean up any messages in the queue
        try:
            await valkey_broker.purge(queue_name)
        except Exception:
            pass  # Queue might not exist


@pytest.fixture
def sample_message() -> dict:
    """Sample message body for testing."""
    return {
        "action": "test",
        "data": {"id": 123, "name": "test-message"},
        "metadata": {"priority": 5, "source": "test-fixture"},
    }


@pytest.fixture
def large_message() -> dict:
    """Large message body for testing."""
    return {
        "items": [{"id": i, "data": f"item-{i}"} for i in range(100)],
        "metadata": {"batch": True, "count": 100},
    }
