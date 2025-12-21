from typing import Final

BROKER_QUEUE_PREFIX: Final[str] = "broker:queue"

MESSAGE_KEY_SEPERATOR: Final[str] = ":"

DEFAULT_QUEUE_SIZE: Final[int] = 10000
DEFAULT_TIMEOUT: Final[float] = 10.0
DEFAULT_TTL: Final[float] = 3600.0  # 1 hour

HEALTH_CHECK_INTERVAL: Final[float] = 30.0  # 30 seconds
HEALTH_CHECK_KEY: Final[str] = "broker:health:check"
