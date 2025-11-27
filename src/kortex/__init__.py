from rich.console import Console

from ._version import __version__
from .server.banner import build_banner

console = Console()

__all__ = ("__version__",)


def entrypoint() -> None:
    console.print(build_banner(__version__))
