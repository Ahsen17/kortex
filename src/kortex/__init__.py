import os
import sys

from rich.console import Console

from .cli import cli
from .config import AppConfig
from .server import __version__, build_banner

console = Console()


def set_enviroments() -> None:
    """Set server environments."""

    config = AppConfig.get_config()

    if provider := config.get_provider("Default"):
        os.environ.setdefault("OPENAI_PROVIDER", provider.provider)
        os.environ.setdefault("OPENAI_BASE_URL", provider.base_url)
        os.environ.setdefault("OPENAI_API_KEY", provider.api_key)


def run_cli() -> None:
    args = sys.argv[1:]

    if not args:
        sys.argv.append("--help")

    cli()


def entrypoint() -> None:
    """Run application server."""

    set_enviroments()

    console.print(f"[cyan]{build_banner(__version__)}[/cyan]")

    run_cli()
