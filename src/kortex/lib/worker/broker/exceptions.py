"""Exception hierarchy for broker operations.

This module provides a clear exception hierarchy for different types of
queue-related errors, following Python best practices.
"""

from __future__ import annotations

__all__ = (
    "CircuitBreakerOpenError",
    "MessageValidationError",
    "QueueEmptyError",
    "QueueError",
    "QueueFullError",
    "RetryExhaustedError",
    "VisibilityTimeoutError",
)


class QueueError(Exception):
    """Base exception for all queue operations."""


class QueueFullError(QueueError):
    """Queue is at capacity and cannot accept more messages."""


class QueueEmptyError(QueueError):
    """Queue is empty and has no messages to consume."""


class MessageValidationError(QueueError):
    """Message validation failed during publishing or processing."""


class RetryExhaustedError(QueueError):
    """All retry attempts have been exhausted for a message."""


class VisibilityTimeoutError(QueueError):
    """Message visibility timeout has expired during processing."""


class CircuitBreakerOpenError(QueueError):
    """Circuit breaker is open, rejecting requests temporarily."""
