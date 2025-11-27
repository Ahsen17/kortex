from typing import Final, Literal

from openai import AsyncOpenAI

from ..base import BaseSchema
from ..config import OpenAIProviderConfig

__all__ = ("Embedder",)

OPENAI_OP_TIMEOUT: Final[float] = 30.0


class Embedder(BaseSchema):
    """Embedder for text content."""

    model: str
    provider: OpenAIProviderConfig

    dimensions: int = 1024
    encoding_format: Literal["float", "base64"] = "float"

    async_client: AsyncOpenAI | None = None

    @property
    def aclient(self) -> AsyncOpenAI:
        """Create an OpenAI client."""

        if self.async_client:
            return self.async_client

        self.async_client = AsyncOpenAI(
            base_url=self.provider.base_url,
            api_key=self.provider.api_key,
            max_retries=1,
            timeout=OPENAI_OP_TIMEOUT,
        )

        return self.async_client

    async def embed(self, content: str | list[str]) -> list[list[float]]:
        """Embed text content using OpenAI's embedding model."""

        response = await self.aclient.embeddings.create(
            input=content,
            model=self.model,
            dimensions=self.dimensions,
            timeout=OPENAI_OP_TIMEOUT,
        )

        return [data.embedding for data in response.data]
