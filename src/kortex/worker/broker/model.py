from typing import Any

import sqlalchemy as sa
from advanced_alchemy.types import JsonB
from sqlalchemy.orm import Mapped, mapped_column

from kortex.db import AuditMixin

from .enum import MessageStatus

__all__ = ("MessageModel",)


class MessageModel(AuditMixin):
    """Message model."""

    __tablename__ = "message"

    body: Mapped[dict[str, Any]] = mapped_column(
        JsonB,
        nullable=False,
    )
    queue: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        default="",
    )
    status: Mapped[MessageStatus] = mapped_column(
        sa.String(),
        nullable=False,
        default=MessageStatus.PUBLISHED,
    )
    delivery_info: Mapped[dict[str, Any]] = mapped_column(
        JsonB,
        nullable=False,
    )
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JsonB,
        nullable=False,
        default={},
    )
    receipt: Mapped[str] = mapped_column(
        sa.String(),
        nullable=True,
    )
