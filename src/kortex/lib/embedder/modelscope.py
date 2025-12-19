from dataclasses import dataclass

from .openai import OpenAIEmbedder

__all__ = ("ModelScopeEmbedder",)


@dataclass
class ModelScopeEmbedder(OpenAIEmbedder):
    """ModelScope based embedder."""

    id: str = "Qwen/Qwen3-Embedding-8B"
    organization: str | None = "ModelScope"
