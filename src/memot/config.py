import functools
import os
from pathlib import Path
from typing import Final, Self

from msgspec import field, toml, yaml

from .base import BaseSchema

__all__ = (
    "BASE_DIR",
    "ApplicationConfig",
    "OpenAIProviderConfig",
    "ServerConfig",
    "get_config",
)

BASE_DIR: Final[Path] = Path.cwd()


class ServerConfig(BaseSchema):
    """Configuration for the MemoT server."""

    host: str = field(default="127.0.0.1")
    port: int = field(default=8000)
    debug: bool = field(default=True)
    reload: bool = field(default=True)


class OpenAIProviderConfig(BaseSchema):
    """Configuration for AI API."""

    provider: str = field(default="")
    base_url: str = field(default="")
    api_key: str = field(default=os.getenv("OPENAI_API_KEY", ""))


class ApplicationConfig(BaseSchema):
    """Configuration for the MemoT application."""

    server: ServerConfig = field(default_factory=ServerConfig)
    openai_providers: list[OpenAIProviderConfig] = field(default=[])

    @classmethod
    def from_file(cls, filename: str | None = None) -> Self:
        """Load the configuration from a file."""

        if filename is None:
            filename = "config.yaml"

        with (BASE_DIR / filename).open("r", encoding="utf-8") as f:
            configuration = f.read()

            suffix = f.name.split(".")[-1]

        match suffix:
            case "yaml":
                return toml.decode(configuration, type=cls)
            case "toml":
                return yaml.decode(configuration, type=cls)
            case _:
                raise ValueError(f"Unsupported configuration file format: {suffix}")

    def get_provider(self, provider: str) -> OpenAIProviderConfig | None:
        """Get the configuration for a specific AI provider."""
        for p in self.openai_providers:
            if p.provider == provider:
                return p

        return None


@functools.lru_cache(maxsize=1, typed=True)
def get_config(filename: str | None = None) -> ApplicationConfig:
    """Get the application configuration."""
    return ApplicationConfig.from_file(filename)
