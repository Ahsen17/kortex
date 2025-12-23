"""Message models for parallel task framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from msgspec import json

from .enum import TaskStatus

__all__ = (
    "TaskMessage",
    "TaskResult",
)


@dataclass
class TaskMessage:
    """Task message containing execution context.

    Extends broker message with task-specific metadata.
    """

    # Core message fields
    queue: str
    payload: dict[str, Any]

    # Task identification
    task_id: UUID = field(default_factory=uuid4)
    task_name: str = ""

    # Task state
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3

    # Timing information
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    scheduled_at: datetime | None = None
    executed_at: datetime | None = None
    completed_at: datetime | None = None

    # Execution results
    result: dict[str, Any] | None = None
    error: str | None = None

    # Metadata
    worker_id: str | None = None
    priority: int = 0
    ttl: int | None = None
    timeout: int | None = None

    # Key for routing (optional)
    key: str | None = None
    timestamp: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "queue": self.queue,
            "payload": self.payload,
            "id": self.task_id,
            "name": self.task_name,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at if self.scheduled_at else None,
            "executed_at": self.executed_at if self.executed_at else None,
            "completed_at": self.completed_at if self.completed_at else None,
            "result": self.result,
            "error": self.error,
            "worker_id": self.worker_id,
            "priority": self.priority,
            "ttl": self.ttl,
            "timeout": self.timeout,
            "key": self.key,
            "timestamp": self.timestamp,
            "meta": self.metadata,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.encode(self.to_dict()).decode("utf-8")

    def to_bytes(self) -> bytes:
        """Convert to JSON bytes."""
        return json.encode(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskMessage:
        """Create from dictionary."""
        # Parse datetime fields
        created_at = data.get("created_at")
        created_at = datetime.fromisoformat(created_at) if created_at else datetime.now(UTC)

        scheduled_at = data.get("scheduled_at")
        if scheduled_at:
            scheduled_at = datetime.fromisoformat(scheduled_at)

        executed_at = data.get("executed_at")
        if executed_at:
            executed_at = datetime.fromisoformat(executed_at)

        completed_at = data.get("completed_at")
        if completed_at:
            completed_at = datetime.fromisoformat(completed_at)

        return cls(
            queue=data.get("queue", "default"),
            payload=data.get("payload", {}),
            task_id=data.get("id", ""),
            task_name=data.get("name", ""),
            status=TaskStatus(data.get("status", TaskStatus.PENDING.value)),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            created_at=created_at,
            scheduled_at=scheduled_at,
            executed_at=executed_at,
            completed_at=completed_at,
            result=data.get("result"),
            error=data.get("error"),
            worker_id=data.get("worker_id"),
            priority=data.get("priority", 0),
            ttl=data.get("ttl"),
            timeout=data.get("timeout"),
            key=data.get("key"),
            timestamp=data.get("timestamp"),
            metadata=data.get("meta", {}),
        )

    @classmethod
    def from_json(cls, data: str) -> TaskMessage:
        """Create from JSON string."""
        decoded = json.decode(data, type=dict[str, Any])
        return cls.from_dict(decoded)

    @classmethod
    def from_bytes(cls, data: bytes) -> TaskMessage:
        """Create from JSON bytes."""
        decoded = json.decode(data, type=dict[str, Any])
        return cls.from_dict(decoded)


@dataclass
class TaskResult:
    """Container for task execution result."""

    task_id: UUID
    status: TaskStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    executed_at: datetime | None = None
    completed_at: datetime | None = None

    def is_success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.SUCCESS

    def is_failed(self) -> bool:
        """Check if task failed permanently."""
        return self.status == TaskStatus.FAILED

    def is_completed(self) -> bool:
        """Check if task is completed (success or failed)."""
        return self.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.REVOKED)
