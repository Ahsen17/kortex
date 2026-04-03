from kortex.db import AuditMixin


class Relationship(AuditMixin):
    """Relationship database model."""

    __tablename__ = "relationship"
