from msgspec import field

from .base.types import BaseStruct

__all__ = ("OpenaiProvider",)


class OpenaiProvider(BaseStruct):
    """Openai API server provider."""

    provider: str = field(default="not-provider")
    base_url: str = field(default="not-provided")
    api_key: str = field(default="not-provided")
