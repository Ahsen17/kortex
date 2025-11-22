import functools
from typing import Final

from openai import AsyncOpenAI

from ..base import BaseSchema
from ..config import get_config

__all__ = ("Embedder",)

OPENAI_OP_TIMEOUT: Final[float] = 30.0

config = get_config()


class Embedder(BaseSchema):
    """Embedder for text content."""

    provider: str
    model: str

    @functools.cached_property
    def client(self) -> AsyncOpenAI:
        """Create an OpenAI client."""

        openai_provider = config.get_provider(self.provider)

        if openai_provider is None:
            raise ValueError(f"OpenAI provider `{self.provider}` not found")

        return AsyncOpenAI(
            base_url=openai_provider.base_url,
            api_key=openai_provider.api_key,
            max_retries=1,
            timeout=OPENAI_OP_TIMEOUT,
        )

    async def embed(
        self,
        content: str | list[str],
        dimensions: int = 1024,
    ) -> list[list[float]]:
        """Embed text content using OpenAI's embedding model."""

        response = await self.client.embeddings.create(
            input=content,
            model=self.model,
            dimensions=dimensions,
            timeout=OPENAI_OP_TIMEOUT,
        )

        return [data.embedding for data in response.data]
