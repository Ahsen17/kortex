from abc import ABCMeta, abstractmethod
from typing import Any, Final, Self, overload
from uuid import UUID

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qdrant_models

from ..base import AuditMixin, BaseSchema
from .embed import Embedder

__all__ = (
    "VectorCollection",
    "VectorStoreMixin",
)

QDRANT_OP_TIMEOUT: Final[int] = 30


class VectorCollection(BaseSchema):
    collection_name: str
    vectors_config: qdrant_models.VectorParams | dict[str, qdrant_models.VectorParams] | None = None
    sparse_vectors_config: dict[str, qdrant_models.SparseVectorParams] | None = None
    on_disk_payload: bool | None = None
    optimizers_config: qdrant_models.OptimizersConfigDiff | None = None
    quantization_config: qdrant_models.QuantizationConfig | None = None


class VectorStoreMixin(AuditMixin, kw_only=True, metaclass=ABCMeta):
    """Mixin for vector store functionality using Qdrant."""

    content: str

    @abstractmethod
    def to_payload(self) -> dict[str, Any]:
        """Convert the instance to a payload dictionary for vector store."""

        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def collection(cls) -> VectorCollection:
        """Get the vector collection associated with the instance."""

        raise NotImplementedError()

    @classmethod
    async def create_collection(cls, client: AsyncQdrantClient) -> None:
        """Create the vector collection associated with the instance."""

        collection = cls.collection()

        if await client.collection_exists(
            collection_name=collection.collection_name,
            timeout=QDRANT_OP_TIMEOUT,
        ):
            return

        await client.create_collection(**collection.to_dict())

    @overload
    @classmethod
    async def query_vectors(
        cls,
        client: AsyncQdrantClient,
        with_payload: bool = True,
        with_vectors: bool = False,
        *,
        id: UUID | list[UUID],
    ) -> list[qdrant_models.Record] | None: ...

    @overload
    @classmethod
    async def query_vectors(
        cls,
        client: AsyncQdrantClient,
        with_payload: bool = True,
        with_vectors: bool = False,
        *,
        embedder: Embedder,
        content: str | list[str],
        prefetch: qdrant_models.Prefetch | None = None,
        query_filter: qdrant_models.Filter | None = None,
        limit: int = 10,
        score_threshold: float | None = None,
    ) -> list[qdrant_models.ScoredPoint] | None: ...

    @classmethod
    async def query_vectors(
        cls,
        client: AsyncQdrantClient,
        with_payload: bool = True,
        with_vectors: bool = False,
        id: UUID | list[UUID] | None = None,  # noqa: A002
        content: str | list[str] | None = None,
        embedder: Embedder | None = None,
        prefetch: qdrant_models.Prefetch | None = None,
        query_filter: qdrant_models.Filter | None = None,
        limit: int = 10,
        score_threshold: float | None = None,
    ) -> list[qdrant_models.Record] | list[qdrant_models.ScoredPoint] | None:
        """Query vectors from the vector store based on ID or content.

        Args:
            client(`AsyncQdrantClient`): Qdrant client.
            with_payload(`bool`): Whether to include payload in the response.
            with_vectors(`bool`): Whether to include vectors in the response.
            id(`UUID | list[UUID]`): ID of the vector to query.
            content(`str | list[str]`): Content of the vector to query.
            embedder(`Embedder`): Embedder to use for content query.
            prefetch(`qdrant_models.Prefetch`): Prefetch settings.
            query_filter(`qdrant_models.Filter`): Query filter.
            limit(`int`): Maximum number of results to return.
            score_threshold(`float`): Minimum score threshold.

        Returns:
            List of vectors.
                - If `id` is provided, returns a list of records.
                - If `content` is provided, returns a list of scored points.
        """

        collection = cls.collection()

        if id is not None:
            ids = [id] if isinstance(id, UUID) else id

            return await client.retrieve(
                collection_name=collection.collection_name,
                ids=[id.hex for id in ids],  # noqa: A001
                with_payload=with_payload,
                with_vectors=with_vectors,
                timeout=QDRANT_OP_TIMEOUT,
            )

        if content is not None:
            if embedder is None:
                raise ValueError("Embedder must be provided for content-based queries.")

            response = await client.query_points(
                collection_name=collection.collection_name,
                query=await embedder.embed(content),
                with_payload=True,
                with_vectors=False,
                prefetch=prefetch,
                query_filter=query_filter,
                limit=limit,
                score_threshold=score_threshold,
                timeout=QDRANT_OP_TIMEOUT,
            )

            return response.points

        return None

    async def upsert(self, client: AsyncQdrantClient) -> None:
        """Upsert the vector representation of the instance."""

        collection = self.collection()

        embedding: list[float] = []

        await client.upsert(
            collection_name=collection.collection_name,
            points=[
                qdrant_models.PointStruct(
                    id=self.id.hex,
                    vector=embedding,
                    payload=self.to_payload(),
                )
            ],
        )

    @classmethod
    async def upsert_vectors(
        cls,
        client: AsyncQdrantClient,
        embedder: Embedder,
        items: list[Self],
    ) -> None:
        """Upsert the vector representation of the instance."""

        collection = cls.collection()

        points: list[qdrant_models.PointStruct] = []

        embeddings: list[list[float]] = await embedder.embed(
            [item.content for item in items],
        )

        for item, embedding in zip(items, embeddings, strict=True):
            points.append(
                qdrant_models.PointStruct(
                    id=item.id.hex,
                    vector=embedding,
                    payload=item.to_payload(),
                )
            )

        await client.upsert(
            collection_name=collection.collection_name,
            points=points,
        )
