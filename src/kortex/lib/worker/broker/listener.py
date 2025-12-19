"""Multi-queue listener for concurrent message processing.

This module provides a high-level listener that manages concurrent consumption
from multiple queues with independent configurations and automatic retry handling.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .config import ConsumeOptions, QueueConfig, RetryPolicy
from .messages import Message, MessageStatus

if TYPE_CHECKING:
    from .interfaces import AsyncBroker
    from .types import MessageHandler, QueueName

__all__ = ("MultiQueueListener",)


class MultiQueueListener:
    """Standard multi-queue listener with enhanced features.

    Manages concurrent consumption from multiple queues with:
    - Independent configurations per queue
    - Automatic retry and dead letter handling
    - Flow control and backpressure
    - Metrics and monitoring

    Example:
        >>> listener = MultiQueueListener()
        >>> config = QueueConfig("tasks", concurrency=5, max_retries=3)
        >>> listener.add_queue(config, handler)
        >>> await listener.start()
    """

    def __init__(self) -> None:
        """Initialize the listener."""
        self._queues: dict[QueueName, QueueConfig] = {}
        self._handlers: dict[QueueName, MessageHandler[Any]] = {}
        self._tasks: dict[QueueName, asyncio.Task[Any]] = {}
        self._running = False
        self._broker: AsyncBroker[Any] | None = None
        self._semaphores: dict[QueueName, asyncio.Semaphore] = {}
        self._metrics: dict[QueueName, dict[str, Any]] = {}

    def add_queue(
        self,
        config: QueueConfig,
        handler: MessageHandler[Any],
    ) -> None:
        """Add a queue to listen to.

        Args:
            config: Queue configuration.
            handler: Message processing handler.
        """
        self._queues[config.name] = config
        self._handlers[config.name] = handler
        self._semaphores[config.name] = asyncio.Semaphore(config.concurrency)
        self._metrics[config.name] = {
            "processed": 0,
            "failed": 0,
            "retried": 0,
            "dead_letter": 0,
        }

    def set_broker(self, broker: AsyncBroker[Any]) -> None:
        """Set the broker instance.

        Args:
            broker: Async broker.
        """
        self._broker = broker

    async def start(self) -> None:
        """Start listening to all configured queues."""
        if self._broker is None:
            raise RuntimeError("Broker not set. Call set_broker() first.")

        if not self._queues:
            raise RuntimeError("No queues configured. Call add_queue() first.")

        self._running = True
        for queue_name in self._queues:
            self._tasks[queue_name] = asyncio.create_task(self._consume_queue(queue_name))

    async def stop(self) -> None:
        """Stop all listeners gracefully."""
        self._running = False
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def _consume_queue(self, queue_name: str) -> None:
        """Internal: Consume from a single queue."""
        if self._broker is None:
            return

        config = self._queues[queue_name]
        handler = self._handlers[queue_name]
        semaphore = self._semaphores[queue_name]
        retry_policy = config.retry_policy or RetryPolicy(max_retries=config.max_retries)

        while self._running:
            try:
                async with semaphore:
                    options = ConsumeOptions(
                        max_messages=config.batch_size,
                        wait_timeout=1,
                        visibility_timeout=config.visibility_timeout,
                    )
                    messages = await self._broker.consume(queue_name, options)

                    if messages:
                        for message in messages:
                            await self._process_message(message, handler, retry_policy, config)
                    else:
                        await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001 - Broad exception catching is intentional for resilience
                # Log error and continue
                self._metrics[queue_name].setdefault("errors", 0)
                self._metrics[queue_name]["errors"] += 1
                await asyncio.sleep(1)

    async def _process_message(
        self,
        message: Message[Any],
        handler: MessageHandler[Any],
        retry_policy: RetryPolicy,
        config: QueueConfig,
    ) -> None:
        """Process a single message with retry logic."""
        if self._broker is None:
            return

        queue_name = message.queue
        receipt = message.receipt

        try:
            # Update delivery info
            if message.delivery_info.first_delivery_time is None:
                message.delivery_info.first_delivery_time = datetime.now(UTC)
            message.delivery_info.last_delivery_time = datetime.now(UTC)
            message.delivery_info.delivery_count += 1

            # Process message
            message.status = MessageStatus.PROCESSING
            await handler(message)

            # Acknowledge success
            if receipt:
                await self._broker.ack(receipt, queue_name)
            message.status = MessageStatus.ACKED
            self._metrics[queue_name]["processed"] += 1

        except Exception:  # noqa: BLE001 - Broad exception catching is intentional for retry logic
            message.status = MessageStatus.NACKED
            # Error details would be set by the actual exception in real implementations
            message.delivery_info.error_details = {
                "type": "ProcessingError",
                "str": "Message processing failed",
            }

            # Check if should retry
            if message.should_retry(retry_policy.max_retries):
                message.increment_retry()
                delay = retry_policy.get_delay(message.delivery_info.retry_count)

                if receipt:
                    await self._broker.nack(receipt, queue_name, requeue=True)

                self._metrics[queue_name]["retried"] += 1
                await asyncio.sleep(delay)
            else:
                # Max retries exceeded - dead letter or drop
                if receipt:
                    await self._broker.nack(receipt, queue_name, requeue=False)

                if config.dead_letter_queue:
                    await self._broker.move(message, config.dead_letter_queue)
                    message.status = MessageStatus.DEAD_LETTER
                    self._metrics[queue_name]["dead_letter"] += 1
                else:
                    self._metrics[queue_name]["failed"] += 1

    async def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all managed queues.

        Returns:
            Dictionary mapping queue names to metrics.
        """
        if self._broker is None:
            return {}

        stats = {}
        for queue_name in self._queues:
            queue_stats = await self._broker.get_queue_stats(queue_name)
            stats[queue_name] = {
                "broker": queue_stats,
                "listener": self._metrics[queue_name].copy(),
            }
        return stats

    async def get_metrics(self) -> dict[str, dict[str, int]]:
        """Get listener metrics only.

        Returns:
            Dictionary of processing metrics.
        """
        return {name: metrics.copy() for name, metrics in self._metrics.items()}

    @property
    def is_running(self) -> bool:
        """Check if listener is running."""
        return self._running

    @property
    def managed_queues(self) -> list[str]:
        """Get list of managed queue names."""
        return list(self._queues.keys())

    @property
    def total_processed(self) -> int:
        """Get total processed messages across all queues."""
        return sum(m["processed"] for m in self._metrics.values())

    @property
    def total_failed(self) -> int:
        """Get total failed messages across all queues."""
        return sum(m["failed"] for m in self._metrics.values())
