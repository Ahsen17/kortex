"""Message models and lifecycle management.

This module provides the core message structures for the broker,
including message status tracking, delivery information, and batch operations.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

__all__ = (
    "DeliveryInfo",
    "Message",
    "MessageBatch",
    "MessageStatus",
)


class MessageStatus(Enum):
    """Message lifecycle status.

    Tracks the complete lifecycle of a message from publication to final state.
    """

    PUBLISHED = "published"  # Message created and queued
    QUEUED = "queued"  # Available for consumption
    CONSUMED = "consumed"  # Retrieved by consumer
    PROCESSING = "processing"  # Being processed by handler
    ACKED = "acked"  # Successfully processed
    NACKED = "nacked"  # Processing failed
    DEAD_LETTER = "dead_letter"  # Moved to dead letter queue
    EXPIRED = "expired"  # TTL expired


@dataclass
class DeliveryInfo:
    """Delivery information for a message.

    Tracks the delivery history and retry state of a message, providing
    complete visibility into message processing attempts.

    Attributes:
        delivery_count: Total number of delivery attempts.
        first_delivery_time: When the message was first delivered.
        last_delivery_time: When the message was last delivered.
        processing_deadline: Deadline for processing completion.
        retry_count: Number of retry attempts.
        error_details: Information about processing failures.
    """

    delivery_count: int = 0
    first_delivery_time: datetime | None = None
    last_delivery_time: datetime | None = None
    processing_deadline: datetime | None = None
    retry_count: int = 0
    error_details: dict[str, Any] | None = None

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"DeliveryInfo(count={self.delivery_count}, "
            f"retry={self.retry_count}, "
            f"errors={self.error_details is not None})"
        )

    def is_expired(self, max_age: int = 86400) -> bool:
        """Check if message is too old.

        Args:
            max_age: Maximum age in seconds (default: 24 hours).

        Returns:
            True if message age exceeds max_age.
        """
        if self.first_delivery_time is None:
            return False
        age = datetime.now(UTC) - self.first_delivery_time
        return age.total_seconds() > max_age

    def is_processing_timeout(self) -> bool:
        """Check if processing deadline has passed."""
        if self.processing_deadline is None:
            return False
        return datetime.now(UTC) > self.processing_deadline


@dataclass
class Message[T]:
    """Represents a message in the queue.

    Standard message structure following industry patterns (AMQP, SQS, Celery).

    Attributes:
        id: Unique identifier (UUID).
        body: Message payload.
        queue: Queue name.
        status: Current message status.
        delivery_info: Delivery and retry tracking.
        attributes: Metadata (priority, type, etc.).
        receipt: Handle for acknowledgment operations.
        created_at: Message creation timestamp.
        expires_at: Optional expiration timestamp.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    body: T = field(default=None)  # type: ignore
    queue: str = field(default="")
    status: MessageStatus = field(default=MessageStatus.PUBLISHED)
    delivery_info: DeliveryInfo = field(default_factory=DeliveryInfo)
    attributes: dict[str, Any] = field(default_factory=dict)
    receipt: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None

    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(UTC) > self.expires_at

    def should_retry(self, max_retries: int) -> bool:
        """Check if message should be retried.

        Args:
            max_retries: Maximum retry attempts.

        Returns:
            True if retry count is below maximum.
        """
        return self.delivery_info.retry_count < max_retries

    def increment_retry(self) -> None:
        """Increment retry count and update delivery info."""
        self.delivery_info.retry_count += 1
        self.delivery_info.last_delivery_time = datetime.now(UTC)
        self.status = MessageStatus.QUEUED

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"Message(id={self.id[:8]}..., queue={self.queue}, "
            f"status={self.status.value}, retry={self.delivery_info.retry_count})"
        )

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"Message(id={self.id}, queue={self.queue}, "
            f"status={self.status.value}, body={self.body!r})"
        )


@dataclass
class MessageBatch[T]:
    """Batch of messages for efficient processing.

    Attributes:
        messages: List of messages.
        queue: Queue name.
        total_count: Total messages available (may exceed len(messages)).
        has_more: Indicates if more messages are available.
    """

    messages: list[Message[T]]
    queue: str
    total_count: int = 0
    has_more: bool = False
