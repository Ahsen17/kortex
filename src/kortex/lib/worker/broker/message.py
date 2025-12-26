import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Self
from uuid import UUID

from msgspec import json

from .enum import MessageStatus

__all__ = (
    "Message",
    "MessageDeliveryInfo",
)


@dataclass
class MessageDeliveryInfo:
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "delivery_count": self.delivery_count,
            "first_delivery_time": self.first_delivery_time.isoformat() if self.first_delivery_time else None,
            "last_delivery_time": self.last_delivery_time.isoformat() if self.last_delivery_time else None,
            "processing_deadline": self.processing_deadline.isoformat() if self.processing_deadline else None,
            "retry_count": self.retry_count,
            "error_details": self.error_details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            delivery_count=data["delivery_count"],
            first_delivery_time=datetime.fromisoformat(data["first_delivery_time"])
            if data["first_delivery_time"]
            else None,
            last_delivery_time=datetime.fromisoformat(data["last_delivery_time"])
            if data["last_delivery_time"]
            else None,
            processing_deadline=datetime.fromisoformat(data["processing_deadline"])
            if data["processing_deadline"]
            else None,
            retry_count=data["retry_count"],
            error_details=data["error_details"],
        )


@dataclass
class Message:
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

    id: UUID = field(default_factory=uuid.uuid4)
    name: str = field(default="")
    body: dict[str, Any] | None = field(default=None)
    queue: str = field(default="")
    key: str | None = None
    priority: int | None = None
    ttl: float | None = None
    status: MessageStatus = field(default=MessageStatus.PUBLISHED)
    delivery_info: MessageDeliveryInfo = field(default_factory=MessageDeliveryInfo)
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
            f"Message(id={self.id.hex[:8]}..., queue={self.queue}, "
            f"status={self.status.value}, retry={self.delivery_info.retry_count})"
        )

    def __repr__(self) -> str:
        """Detailed representation."""

        return f"Message(id={self.id}, queue={self.queue}, status={self.status.value})"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id.hex,
            "body": self.body,
            "queue": self.queue,
            "key": self.key,
            "priority": self.priority,
            "ttl": self.ttl,
            "status": self.status.value,
            "delivery_info": self.delivery_info.to_dict(),
            "attributes": self.attributes,
            "receipt": self.receipt,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            id=UUID(data["id"]),
            body=data["body"],
            queue=data["queue"],
            key=data["key"],
            priority=data["priority"],
            ttl=data["ttl"],
            status=MessageStatus(data["status"]),
            delivery_info=MessageDeliveryInfo.from_dict(data["delivery_info"]),
            attributes=data["attributes"],
            receipt=data["receipt"],
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data["expires_at"] else None,
        )

    def to_json(self) -> str:
        return json.encode(self.to_dict()).decode("utf-8")

    @classmethod
    def from_json(cls, data: str) -> Self:
        return cls.from_dict(json.decode(data))

    def to_jsonb(self) -> bytes:
        return json.encode(self.to_dict())

    @classmethod
    def from_jsonb(cls, data: bytes) -> Self:
        return cls.from_dict(json.decode(data))
