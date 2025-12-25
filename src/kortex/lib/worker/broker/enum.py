from enum import StrEnum, auto

__all__ = ("MessageStatus",)


class MessageStatus(StrEnum):
    """Message lifecycle status.

    Tracks the complete lifecycle of a message from publication to final state.
    """

    PUBLISHED = auto()
    """Message created and queued"""

    QUEUED = auto()
    """Available for consumption"""

    CONSUMED = auto()
    """Retrieved by consumer"""

    PROCESSING = auto()
    """Being processed by handler"""

    ACKED = auto()
    """Successfully processed"""

    NACKED = auto()
    """Processing failed"""

    DEAD_LETTER = auto()
    """Moved to dead letter queue"""

    EXPIRED = auto()
    """TTL expired"""
