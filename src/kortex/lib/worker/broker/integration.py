from dataclasses import dataclass
from typing import Literal

__all__ = ("BrokerHealth",)


@dataclass
class BrokerHealth:
    """Broker health status."""

    status: Literal["healthy", "unhealthy"] = "unhealthy"
    response_time_ms: float = -1.0
    version: str = "unknown"
    connected_clients: int = 0
    error: Exception | None = None
