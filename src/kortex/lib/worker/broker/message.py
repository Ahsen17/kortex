import uuid
from dataclasses import dataclass, field
from typing import Any, Self
from uuid import UUID

from msgspec import json

__all__ = ("Message",)


@dataclass
class Message:
    """Represents a message in the queue.

    Standard message structure following industry patterns (AMQP, SQS, Celery).

    Attributes:
        id: Unique identifier (UUID).
        payload: Message payload.
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
    payload: dict[str, Any] | None = field(default=None)
    queue: str = field(default="")
    key: str | None = None
    priority: int | None = None
    ttl: float | None = None

    def __str__(self) -> str:
        """String representation for debugging."""

        return f"Message(id={self.id.hex[:8]}, queue={self.queue}, key={self.key}, "

    def __repr__(self) -> str:
        """Detailed representation."""

        return f"Message(id={self.id}, queue={self.queue}, key={self.key})"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id.hex,
            "payload": self.payload,
            "queue": self.queue,
            "key": self.key,
            "priority": self.priority,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            id=UUID(data["id"]),
            payload=data["payload"],
            queue=data["queue"],
            key=data["key"],
            priority=data["priority"],
            ttl=data["ttl"],
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
