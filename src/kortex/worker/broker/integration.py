from typing import Literal

from kortex.base.types import BaseStruct

__all__ = ("BrokerHealth",)


class BrokerHealth(BaseStruct):
    """Broker health status."""

    status: Literal["healthy", "unhealthy"]
    response_time_ms: float = -1.0
    version: str = "unknown"
    connected_clients: int = 0
    error: Exception | None = None
