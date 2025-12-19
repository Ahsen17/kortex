"""Broker framework aggregator module.

This module provides a unified import point for all broker-related components.
It re-exports all classes, types, and exceptions from the modularized broker
framework for backward compatibility and convenience.

The broker framework is split into logical modules:
- types.py: Type aliases for better type hints
- exceptions.py: Exception hierarchy
- messages.py: Message models and lifecycle management
- config.py: Configuration dataclasses
- interfaces.py: Abstract broker interfaces
- listener.py: Multi-queue listener implementation

This aggregator module ensures backward compatibility while allowing internal
modularization for better code organization and maintainability.
"""

from __future__ import annotations

from .config import ConsumeOptions, PublishOptions, QueueConfig, RetryPolicy
from .exceptions import (
    CircuitBreakerOpenError,
    MessageValidationError,
    QueueEmptyError,
    QueueError,
    QueueFullError,
    RetryExhaustedError,
    VisibilityTimeoutError,
)
from .interfaces import AsyncBroker, Broker
from .listener import MultiQueueListener
from .messages import DeliveryInfo, Message, MessageBatch, MessageStatus
from .types import MessageHandler, MessageId, QueueName, Receipt

__all__ = (
    "AsyncBroker",
    "Broker",
    "CircuitBreakerOpenError",
    "ConsumeOptions",
    "DeliveryInfo",
    "Message",
    "MessageBatch",
    "MessageHandler",
    "MessageId",
    "MessageStatus",
    "MessageValidationError",
    "MultiQueueListener",
    "PublishOptions",
    "QueueConfig",
    "QueueEmptyError",
    "QueueError",
    "QueueFullError",
    "QueueName",
    "Receipt",
    "RetryExhaustedError",
    "RetryPolicy",
    "VisibilityTimeoutError",
)
