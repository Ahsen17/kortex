from typing import cast

import typer
from advanced_alchemy.alembic.commands import AlembicCommands
from rich.console import Console

from kortex.config import BASE_DIR
from kortex.db.base import alchemy_config

console = Console()

db_click = typer.Typer(
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@db_click.command("init")
def db_initialize() -> None:
    """Initialize db migrations."""

    console.print("[cyan]Initializing database migrations...[/cyan]")

    AlembicCommands(alchemy_config()).init(
        cast("str", BASE_DIR / "db"),
        multidb=False,
        package=True,
    )

    console.print("[green]Database migrations initialized.[/green]")


@db_click.command("make-migrations")
def db_make_migrations(
    down_revision: str | None = None,
    message: str = "upgrade migration revision.",
) -> None:
    """Create a new migration revision."""

    console.print("[cyan]Create a new migration revision...[/cyan]")

    AlembicCommands(alchemy_config()).revision(
        message=message,
        autogenerate=True,
        sql=False,
        head="head",
        splice=False,
        branch_label=None,
        version_path=None,
        rev_id=None,
        depends_on=down_revision,
        process_revision_directives=None,
    )

    console.print("[green]Migration revision created.[/green]")


@db_click.command("upgrade")
def db_revision_upgrade(revision: str = "head") -> None:
    """Upgrade to target revision."""

    console.print("[cyan]Upgrading to target revision...[/cyan]")

    AlembicCommands(alchemy_config()).upgrade(revision)

    console.print("[green]Upgrade completed.[/green]")


@db_click.command("downgrade")
def db_revision_downgrade(revision: str = "head") -> None:
    """Downgrade to target revision."""

    console.print("[cyan]Downgrading to target revision...[/cyan]")

    AlembicCommands(alchemy_config()).downgrade(revision)

    console.print("[green]Downgrade completed.[/green]")


@db_click.command("show-current-revision")
def db_show_current_revision() -> None:
    """Show current revision."""

    console.print("[cyan]Showing current revision...[/cyan]")

    AlembicCommands(alchemy_config()).current(verbose=True)

    console.print("[green]Current revision shown.[/green]")
