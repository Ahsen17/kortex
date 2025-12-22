import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from kortex.db import AuditMixin


class Tag(AuditMixin):
    """Tag database model."""

    __tablename__ = "fragment"

    label: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Tag label.",
    )
    description: Mapped[str] = mapped_column(
        sa.String(),
        nullable=True,
        comment="Tag description.",
    )
