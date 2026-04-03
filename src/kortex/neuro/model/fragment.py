import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from kortex.db import AuditMixin


class Fragment(AuditMixin):
    """Fragment database model."""

    __tablename__ = "fragment"

    content: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Fragment content.",
    )
