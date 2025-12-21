from kortex.db.base import AuditMixin


class _meta(AuditMixin):  # noqa: N801
    """Meta for humanity models."""

    __abstract__ = True
