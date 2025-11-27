from dataclasses import dataclass
from typing import Self

from agno.models.openai import OpenAILike
from httpx import URL

from ..config import OpenAIProviderConfig

__all__ = ("AgentChat",)


@dataclass
class AgentChat(OpenAILike):
    """A class for to interact with any provider using the OpenAI API schema.

    Args:
        id (str): The id of the OpenAI model to use. Defaults to "not-provided".
        name (str): The name of the OpenAI model to use. Defaults to "not-provided".
        base_url (str): The base URL to use. Defaults to "None".
        api_key (str): The API key to use. Defaults to "None".
    """

    id: str = "not-provided"
    name: str = "not-provided"

    base_url: str | URL | None = None
    api_key: str | None = None

    default_role_map = {
        "system": "system",
        "user": "user",
        "assistant": "assistant",
        "tool": "tool",
    }

    @classmethod
    def create(cls, provider: OpenAIProviderConfig, id: str) -> Self:  # noqa: A002
        """Create a default instance of the AgentChat model."""

        return cls(
            id=id,
            name=provider.provider,
            base_url=provider.base_url,
            api_key=provider.api_key,
        )
