import typer
from fastapi_cli.exceptions import FastAPICLIException
from fastapi_cli.utils.cli import get_rich_toolkit, get_uvicorn_log_config

try:
    import uvicorn
except ImportError:  # pragma: no cover
    uvicorn = None  # type: ignore[assignment]

from ..config import AppConfig
from ..server.app import Application

app_click = typer.Typer(
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app_click.command("run")
def run_server() -> None:
    """Run kortex server with CLI."""

    config = AppConfig.get_config()

    with get_rich_toolkit() as toolkit:
        toolkit.print(f"Running server on {config.server.host}:{config.server.port} 🚀")
        toolkit.print_line()

        url = f"http://{config.server.host}:{config.server.port}"
        url_docs = f"{url}{config.server.docs_url}"

        toolkit.print(
            f"Server started at [link={url}]{url}[/]",
            f"Documentation at [link={url_docs}]{url_docs}[/]",
            tag="server",
        )

        if not uvicorn:
            raise FastAPICLIException("Could not import Uvicorn, try running 'pip install uvicorn'") from None

        toolkit.print_line()
        toolkit.print("Logs:")
        toolkit.print_line()

        uvicorn.run(
            app=Application.create(config.server).get_service,
            host=config.server.host,
            port=config.server.port,
            reload=config.server.reload,
            workers=config.server.concurrency,
            root_path=config.server.root_path,
            log_config=get_uvicorn_log_config(),
        )
