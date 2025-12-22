from dataclasses import dataclass
from typing import Any

from kortex.lib.vector import QdrantVectorCollection, QdrantVectorStoreMixin


class FragmentVectorCollection(QdrantVectorCollection):
    """Fragment vector collection."""


@dataclass
class FragmentVector(QdrantVectorStoreMixin):
    """Fragment vector store."""

    # TODO: additional fields

    def to_payload(
        self,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
        exclude_none: bool = True,
    ) -> dict[str, Any]:
        """Convert the instance to a payload dictionary for vector store.

        Args:
            include(`set[str]`): Fields to include in the payload.
            exclude(`set[str]`): Fields to exclude from the payload.
            exclude_none(`bool`): Whether to exclude fields with `None` value, defaults to `True`.

        Returns:
            Payload dictionary.
        """

        return {}

    @classmethod
    def collection(cls) -> QdrantVectorCollection:
        """Get the vector collection associated with the instance."""

        return FragmentVectorCollection(collection_name="fragment")
