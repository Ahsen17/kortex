import typer

from .app import app_click
from .db import db_click

__all__ = ("cli",)

cli = typer.Typer(
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)

cli.add_typer(app_click)
cli.add_typer(
    db_click,
    name="database",
    help="Database manager commands.",
)
