import os
from dataclasses import dataclass

from agno.models.openai import OpenAILike
from httpx import URL

__all__ = ("OpenAIChat",)


@dataclass
class OpenAIChat(OpenAILike):
    """Base OpenAI like chat."""

    id: str = "not-provided"
    name: str = "not-provided"
    provider: str = ""

    base_url: str | URL | None = None
    api_key: str | None = None

    timeout: float | None = None
    max_retries: int | None = None

    enable_thinking: bool | None = None

    default_role_map = {
        "system": "system",
        "user": "user",
        "assistant": "assistant",
        "tool": "tool",
    }

    def __post_init__(self) -> None:
        if self.base_url is None:
            self.base_url = "https://api.openai.com/v1"

        if self.api_key is None:
            self.api_key = os.getenv("OPENAI_API_KEY", "not-provided")

            if self.api_key == "not-provided":
                raise ValueError("`OPENAI_API_KEY` is not provided for `{self.provider}` service.")

        if self.enable_thinking:
            if self.extra_body is None:
                self.extra_body = {}

            self.extra_body["enable_thinking"] = self.enable_thinking
