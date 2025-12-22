from typing import Any

import sqlalchemy as sa
from advanced_alchemy.types import JsonB
from sqlalchemy.orm import Mapped, mapped_column

from kortex.db import AuditMixin

from ..enum import EntityType


class Entity(AuditMixin):
    """Entity database model."""

    __tablename__ = "entity"

    type: Mapped[EntityType] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Entity type.",
    )
    summary: Mapped[str] = mapped_column(
        sa.String(),
        nullable=False,
        comment="Entity summary.",
    )
    metadata: Mapped[dict[str, Any]] = mapped_column(
        __type_pos=JsonB,
        nullable=False,
        default={},
        comment="Entity metadata.",
    )
