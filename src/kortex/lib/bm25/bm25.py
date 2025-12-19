import os
import re
import unicodedata
from collections.abc import Iterable
from multiprocessing import get_all_start_methods
from pathlib import Path
from typing import Any, Final, Literal

import jieba  # type: ignore
import numpy as np
from fastembed import SparseEmbedding
from fastembed.common.utils import iter_batch
from fastembed.parallel_processor import ParallelWorkerPool
from fastembed.sparse.bm25 import Bm25, Bm25Worker

__all__ = (
    "Bm25Chinese",
    "mrl_embeddings",
)


class Bm25Chinese(Bm25):
    """BM25 model for Chinese."""

    _stopwords_file: Final[list[Path]] = [
        Path("stopwords.txt"),
    ]

    def __init__(
        self,
        model_name: str,
        cache_dir: str | None = None,
        k: float = 1.2,
        b: float = 0.75,
        avg_len: float = 256.0,
        language: Literal["chinese"] = "chinese",
        token_max_length: int = 40,
        disable_stemmer: bool = False,
        specific_model_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.k = k
        self.b = b
        self.avg_len = avg_len
        self.language = language
        self.token_max_length = token_max_length
        self._specific_model_path = specific_model_path

        self._model_dir = Path()

        if disable_stemmer:
            self.stopwords: set[str] = set()
            self.stemmer = None
        else:
            self.stopwords = set(self._load_stopwords(self._model_dir, self.language))
            self.stemmer = None

        self.tokenizer = self._tokenizer  # type: ignore

    punctuation_regex = re.compile(r"[^\u4e00-\u9fffa-zA-Z0-9\-#]|\s+")

    @classmethod
    def set_stopwords_filepath(cls, filepath: Path | str) -> None:
        if isinstance(filepath, str):
            filepath = Path(filepath)

        for _path in filepath.glob("*.txt"):
            cls._stopwords_file.append(_path)

    @classmethod
    def _tokenizer(cls, document: str) -> list[str]:
        """Tokenize the given documents."""

        if not document.strip():
            return []

        return list(
            jieba.cut_for_search(
                # Coding normalization
                "".join(
                    unicodedata.normalize(
                        "NFD",
                        char,
                    )
                    if unicodedata.category(char) != "Mn"
                    else char
                    for char in document
                ).strip(),
            )
        )

    @classmethod
    def _load_stopwords(cls, model_dir: Path, language: str) -> list[str]:
        """Loads the stopwords from the given model directory."""

        stopwords: list[str] = []

        for file in cls._stopwords_file:
            if not file.exists():
                continue

            with file.open() as f:
                stopwords.extend(f.read().splitlines())

        return stopwords

    def _embed_documents(
        self,
        model_name: str,
        cache_dir: str,
        documents: str | Iterable[str],
        batch_size: int = 256,
        parallel: int | None = None,
        local_files_only: bool = False,
        specific_model_path: str | None = None,
    ) -> Iterable[SparseEmbedding]:
        is_small = False

        if isinstance(documents, str):
            documents = [documents]

        if isinstance(documents, list) and len(documents) < batch_size:
            is_small = True

        if parallel is None or is_small:
            for batch in iter_batch(documents, batch_size):
                yield from self.raw_embed(batch)
        else:
            if parallel == 0:
                parallel = os.cpu_count()

        start_method = "forkserver" if "forkserver" in get_all_start_methods() else "spawn"

        params = {
            "model_name": model_name,
            "cache_dir": cache_dir,
            "k": self.k,
            "b": self.b,
            "avg_len": self.avg_len,
            "language": self.language,
            "token_max_length": self.token_max_length,
            "disable_stemmer": self.disable_stemmer,
            "local_files_only": local_files_only,
            "specific_model_path": specific_model_path,
        }

        pool = ParallelWorkerPool(
            num_workers=parallel or 1,
            worker=self._get_worker_class(),
            start_method=start_method,
        )

        for batch in pool.ordered_map(iter_batch(documents, batch_size), **params):
            yield from batch

    def _stem(self, tokens: list[str]) -> list[str]:
        stemmed_tokens: list[str] = []
        for token in tokens:
            lower_token = token.lower()

            if not self.punctuation_regex.sub(" ", token).strip():
                continue

            if lower_token in self.stopwords:
                continue

            if len(token) > self.token_max_length:
                continue

            stemmed_tokens.append(token)

        return stemmed_tokens

    def raw_embed(self, documents: list[str]) -> list[SparseEmbedding]:
        """Embeds the given documents."""

        embeddings: list[SparseEmbedding] = []

        for document in documents:
            tokens = self._tokenizer(document)
            stemmed_tokens = self._stem(tokens)
            token_id2value = self._term_frequency(stemmed_tokens)
            embeddings.append(SparseEmbedding.from_dict(token_id2value))

        return embeddings

    def embed(
        self,
        documents: str | Iterable[str],
        batch_size: int = 256,
        parallel: int | None = None,
        **kwargs: Any,
    ) -> Iterable[SparseEmbedding]:
        """Embeds the given documents."""

        yield from self._embed_documents(
            model_name="",
            cache_dir=str(self.cache_dir),
            documents=documents,
            batch_size=batch_size,
            parallel=parallel,
            local_files_only=self._local_files_only,
            specific_model_path=self._specific_model_path,
        )

    def query_embed(self, query: str | Iterable[str], **kwargs: Any) -> Iterable[SparseEmbedding]:
        """Embeds the given query."""

        if isinstance(query, str):
            query = [query]

        for text in query:
            tokens = self._tokenizer(text)
            stemmed_tokens = self._stem(tokens)
            token_ids = np.array(
                list({self.compute_token_id(token) for token in stemmed_tokens}),
                dtype=np.int32,
            )
            values = np.ones_like(token_ids)

            yield SparseEmbedding(indices=token_ids, values=values)

    @classmethod
    def _get_worker_class(cls) -> type["Bm25CHWorker"]:
        """Returns the worker class for the model."""

        return Bm25CHWorker


class Bm25CHWorker(Bm25Worker):
    """Worker class for BM25 Chinese model."""

    def __init__(self, model_name: str, cache_dir: str, **kwargs: Any):
        self.model = Bm25Chinese(model_name, cache_dir, **kwargs)

    @classmethod
    def start(cls, model_name: str, cache_dir: str, **kwargs: Any) -> "Bm25CHWorker":
        return cls(model_name, cache_dir, **kwargs)

    def process(self, items: Iterable[tuple[int, Any]]) -> Iterable[tuple[int, list[SparseEmbedding]]]:
        for idx, batch in items:
            output = self.model.raw_embed(batch)

            yield idx, output


def mrl_embeddings(
    embeddings: list[list[float]],
    dimensions: int,
) -> list[list[float]]:
    """Returns a dummy MRL embedding."""

    embedding = np.array(embeddings, dtype=np.float64)
    mrl_dense = embedding[:, :dimensions]
    mrl_dense /= np.linalg.norm(mrl_dense, axis=1, keepdims=True)

    return mrl_dense.tolist()  # type: ignore
