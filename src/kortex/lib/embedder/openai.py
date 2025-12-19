from dataclasses import dataclass
from typing import Final, Literal

from openai import AsyncOpenAI

__all__ = ("OpenAIEmbedder",)

OPENAI_OP_TIMEOUT: Final[float] = 30.0


@dataclass
class OpenAIEmbedder:
    """OpenAI based embedder."""

    id: str = "not-provided"
    organization: str | None = None

    base_url: str | None = None
    api_key: str | None = None

    dimensions: int = 1024
    encoding_format: Literal["float", "base64"] = "float"

    async_client: AsyncOpenAI | None = None

    @property
    def aclient(self) -> AsyncOpenAI:
        """Create an OpenAI client."""

        if self.async_client:
            return self.async_client

        self.async_client = AsyncOpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
            max_retries=1,
            timeout=OPENAI_OP_TIMEOUT,
        )

        return self.async_client

    async def embed(self, content: str | list[str]) -> list[list[float]]:
        """Embed text content using OpenAI's embedding model.

        Args:
            content(`str | list[str]`): The text content to embed.

        Returns:
            A list of embeddings.
        """

        response = await self.aclient.embeddings.create(
            input=content,
            model=self.id,
            dimensions=self.dimensions,
            timeout=OPENAI_OP_TIMEOUT,
        )

        return [data.embedding for data in response.data]
