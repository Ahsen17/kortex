from dataclasses import dataclass

from .openai import OpenAIChat

__all__ = ("MimoChat",)


@dataclass
class MimoChat(OpenAIChat):
    """Mimo chat."""

    name: str = "Mimo"
    provider: str = "Mimo"

    base_url = "https://api.xiaomimimo.com/v1"
