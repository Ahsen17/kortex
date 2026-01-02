"""Exception hierarchy for broker operations.

This module provides a clear exception hierarchy for different types of
queue-related errors, following Python best practices.
"""

from __future__ import annotations

__all__ = (
    "BrokerError",
    "CircuitBreakerOpenError",
    "MessageValidationError",
    "QueueEmptyError",
    "QueueFullError",
    "RetryExhaustedError",
    "VisibilityTimeoutError",
)


class BrokerError(Exception):
    """Base exception for all queue broker-related operations."""


class BrokerConnectionError(BrokerError):
    """Raised when there are connection issues with the broker."""


class BrokerTimeoutError(BrokerError):
    """Raised when an operation times out."""


class QueueNotFoundError(BrokerError):
    """Raised when a queue does not exist."""


class QueueExistsError(BrokerError):
    """Raised when trying to create a queue that already exists."""


class QueueFullError(BrokerError):
    """Queue is at capacity and cannot accept more messages."""


class QueueEmptyError(BrokerError):
    """Queue is empty and has no messages to consume."""


class MessageValidationError(BrokerError):
    """Message validation failed during publishing or processing."""


class RetryExhaustedError(BrokerError):
    """All retry attempts have been exhausted for a message."""


class VisibilityTimeoutError(BrokerError):
    """Message visibility timeout has expired during processing."""


class CircuitBreakerOpenError(BrokerError):
    """Circuit breaker is open, rejecting requests temporarily."""
