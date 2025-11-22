from datetime import datetime
from typing import Any, Self
from uuid import UUID, uuid4

from msgspec import Struct, convert, field, json, to_builtins

from .abc import JsonBytes, JsonString


class BaseSchema(Struct):
    """Base class for structured data models."""

    def to_dict(
        self,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
        exclude_none: bool = False,
    ) -> dict[str, Any]:
        """Convert the struct to a dictionary."""

        if include and exclude:
            raise ValueError("Cannot specify both include and exclude")

        ret = to_builtins(self)

        attr_names = set(ret)

        if include:
            attr_names = include & attr_names

        if exclude:
            attr_names -= exclude

        if exclude_none:
            attr_names -= {name for name in attr_names if getattr(self, name, None) is None}

        return {name: ret[name] for name in attr_names if name in ret}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create an instance of the struct from a dictionary."""
        return convert(data, type=cls)

    def to_json(self) -> JsonString:
        """Convert the struct to a JSON string."""
        return json.encode(self).decode("utf-8")

    @classmethod
    def from_json(cls, data: JsonString) -> Self:
        """Create an instance of the struct from a JSON string."""
        return json.decode(data, type=cls)

    def to_jsonb(self) -> JsonBytes:
        """Convert the struct to a JSONB string."""
        return json.encode(self)

    @classmethod
    def from_jsonb(cls, data: JsonBytes) -> Self:
        """Create an instance of the struct from a JSONB string."""
        return json.decode(data, type=cls)

    @classmethod
    def json_schema(cls) -> dict[str, Any]:
        """Generate JSON schema for the struct."""

        return json.schema(cls)


class CamelizedSchema(BaseSchema, rename="camel"):
    """Base class for structured data models with camelCase field names."""


class AuditMixin(BaseSchema):
    """Mixin for audit fields."""

    id: UUID = field(default_factory=uuid4)

    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime | None = None
