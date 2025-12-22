from enum import StrEnum, auto


class EntityType(StrEnum):
    """Entity type enumeration."""

    HUMANITY = auto()
    """Humanity type."""

    EVENT = auto()
    """Event type."""

    OTHER = auto()
    """Other type."""
