import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from ..base import AuditMixin


class Example(AuditMixin):
    """Example model."""

    __tablename__ = "example"

    name: Mapped[str] = mapped_column(sa.String, unique=True)
