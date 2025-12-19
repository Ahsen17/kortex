"""Configuration classes for broker operations.

This module provides configuration dataclasses for publishing, consuming,
queue behavior, and retry policies.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

__all__ = (
    "ConsumeOptions",
    "PublishOptions",
    "QueueConfig",
    "RetryPolicy",
)


@dataclass
class RetryPolicy:
    """Configuration for message retry behavior.

    Follows exponential backoff with jitter pattern for optimal retry timing.

    Attributes:
        max_retries: Maximum retry attempts.
        backoff_factor: Multiplier for exponential backoff (e.g., 2.0).
        max_delay: Maximum delay in seconds.
        min_delay: Minimum delay in seconds.
        jitter: Add random variation to delay (0.0-1.0).
        retryable_exceptions: Exception types that trigger retry.
    """

    max_retries: int = 3
    backoff_factor: float = 2.0
    max_delay: int = 3600
    min_delay: int = 1
    jitter: bool = True
    retryable_exceptions: list[type[Exception]] | None = None

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt.

        Args:
            attempt: Retry attempt number (0-based).

        Returns:
            Delay in seconds.
        """
        # Exponential backoff: base * factor^attempt
        delay = min(
            self.min_delay * (self.backoff_factor**attempt),
            self.max_delay,
        )

        # Add jitter if enabled
        if self.jitter:
            delay *= random.uniform(0.9, 1.1)  # noqa: S311 - Random is fine for jitter

        return delay


@dataclass
class PublishOptions:
    """Options for publishing messages.

    Attributes:
        delay_seconds: Delay before message becomes available.
        priority: Message priority (0-9, higher = more important).
        deduplication_id: Idempotency key for deduplication.
        time_to_live: Message TTL in seconds.
        retry_policy: Custom retry policy for this message.
        processing_timeout: Max processing time before timeout.
        dead_letter_queue: DLQ name for failed messages.
        attributes: Additional metadata.
    """

    delay_seconds: int = 0
    priority: int = 0
    deduplication_id: str | None = None
    time_to_live: int | None = None
    retry_policy: RetryPolicy | None = None
    processing_timeout: int | None = None
    dead_letter_queue: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumeOptions:
    """Options for consuming messages.

    Attributes:
        max_messages: Maximum messages per consume call.
        wait_timeout: Long polling wait time in seconds.
        visibility_timeout: Time message is hidden after consumption.
        prefetch_count: Messages to prefetch.
        long_polling: Enable long polling.
        include_attributes: Include message attributes in result.
        filter_expression: Message filter expression.
    """

    max_messages: int = 10
    wait_timeout: int | None = None
    visibility_timeout: int | None = None
    prefetch_count: int | None = None
    long_polling: bool = True
    include_attributes: bool = True
    filter_expression: str | None = None


@dataclass
class QueueConfig:
    """Configuration for queue behavior.

    Attributes:
        name: Queue name.
        concurrency: Maximum concurrent consumers.
        batch_size: Messages per batch.
        processing_timeout: Batch processing timeout.
        retry_policy: Default retry policy.
        visibility_timeout: Default visibility timeout.
        dead_letter_queue: DLQ name.
        max_retries: Maximum retry attempts.
    """

    name: str
    concurrency: int = 1
    batch_size: int = 10
    processing_timeout: int | None = None
    retry_policy: RetryPolicy | None = None
    visibility_timeout: int | None = None
    dead_letter_queue: str | None = None
    max_retries: int = 3
