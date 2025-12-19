import sqlalchemy as sa
from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.mixins import SlugKey
from sqlalchemy.orm import Mapped, mapped_column


class Example(UUIDAuditBase, SlugKey):
    """Example model."""

    __tablename__ = "example"

    name: Mapped[str] = mapped_column(sa.String, unique=True)
