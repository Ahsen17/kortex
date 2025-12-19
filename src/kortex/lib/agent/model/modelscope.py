from dataclasses import dataclass

from .openai import OpenAIChat

__all__ = ("ModelScopeChat",)


@dataclass
class ModelScopeChat(OpenAIChat):
    """Model scope chat."""

    name: str = "ModelScope"
    provider: str = "ModelScope"

    base_url = "https://api-inference.modelscope.cn/v1"
