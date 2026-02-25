"""Embedding generation and similarity search for query library.

Uses sentence-transformers for embedding generation and DuckDB VSS for search.

Usage:
    embeddings = QueryEmbeddings(manager)

    # Generate and store embedding
    embeddings.add_query(query_id="q1", text="What was total revenue?")

    # Find similar queries
    results = embeddings.find_similar("How much money did we make?", limit=5)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)

# Lazy-loaded model
_model: Any = None
_model_name = "all-MiniLM-L6-v2"
_embedding_dim = 384


def _get_cache_dir() -> str:
    """Get local cache directory for model files."""
    from pathlib import Path

    # Store next to the config directory at project root
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    cache_dir = project_root / ".cache" / "models"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir)


def _get_model() -> Any:
    """Get or load the sentence-transformers model (lazy loading)."""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer

            cache_dir = _get_cache_dir()
            logger.info(f"Loading embedding model: {_model_name} (cache: {cache_dir})")
            _model = SentenceTransformer(_model_name, cache_folder=cache_dir)
            logger.info("Embedding model loaded")
        except ImportError as e:
            raise ImportError(
                "sentence-transformers is required for similarity search. "
                "Install with: uv add sentence-transformers"
            ) from e
    return _model


def generate_embedding(text: str) -> list[float]:
    """Generate embedding for a single text.

    Args:
        text: Text to embed

    Returns:
        384-dimensional embedding vector
    """
    model = _get_model()
    embedding = model.encode([text])[0]
    return list(embedding.tolist())


def generate_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for multiple texts efficiently.

    Args:
        texts: List of texts to embed

    Returns:
        List of 384-dimensional embedding vectors
    """
    if not texts:
        return []
    model = _get_model()
    embeddings = model.encode(texts, show_progress_bar=len(texts) > 10)
    return [list(e.tolist()) for e in embeddings]


@dataclass
class SimilarQuery:
    """Result from similarity search."""

    query_id: str
    similarity: float  # 0.0 to 1.0, higher is more similar


class QueryEmbeddings:
    """Manages query embeddings in the vectors database.

    Handles embedding generation, storage, and similarity search.
    """

    def __init__(self, manager: ConnectionManager):
        """Initialize with connection manager.

        Args:
            manager: ConnectionManager with vectors database configured
        """
        self.manager = manager

    def add_query(self, query_id: str, text: str) -> None:
        """Generate and store embedding for a query.

        Args:
            query_id: Unique query identifier
            text: Text to embed (question or graph description)
        """
        embedding = generate_embedding(text)

        with self.manager.vectors_write() as conn:
            # Upsert: delete if exists, then insert
            conn.execute("DELETE FROM query_embeddings WHERE query_id = ?", [query_id])
            conn.execute(
                "INSERT INTO query_embeddings VALUES (?, ?)",
                [query_id, embedding],
            )

        logger.debug(f"Stored embedding for query {query_id}")

    def add_queries_batch(self, queries: list[tuple[str, str]]) -> None:
        """Generate and store embeddings for multiple queries.

        Args:
            queries: List of (query_id, text) tuples
        """
        if not queries:
            return

        query_ids = [q[0] for q in queries]
        texts = [q[1] for q in queries]

        embeddings = generate_embeddings_batch(texts)

        with self.manager.vectors_write() as conn:
            # Delete existing
            for query_id in query_ids:
                conn.execute("DELETE FROM query_embeddings WHERE query_id = ?", [query_id])

            # Insert all
            for query_id, embedding in zip(query_ids, embeddings, strict=True):
                conn.execute(
                    "INSERT INTO query_embeddings VALUES (?, ?)",
                    [query_id, embedding],
                )

        logger.info(f"Stored {len(queries)} embeddings")

    def remove_query(self, query_id: str) -> None:
        """Remove embedding for a query.

        Args:
            query_id: Query identifier to remove
        """
        with self.manager.vectors_write() as conn:
            conn.execute("DELETE FROM query_embeddings WHERE query_id = ?", [query_id])

    def find_similar(
        self,
        text: str,
        limit: int = 5,
        min_similarity: float = 0.0,
    ) -> list[SimilarQuery]:
        """Find queries similar to the given text.

        Args:
            text: Text to find similar queries for
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold (0.0 to 1.0)

        Returns:
            List of SimilarQuery results, ordered by similarity (descending)
        """
        query_embedding = generate_embedding(text)

        with self.manager.vectors_cursor() as cursor:
            # Use cosine distance (0 = identical, 2 = opposite)
            # Convert to similarity: 1 - (distance / 2)
            results = cursor.execute(
                f"""
                SELECT
                    query_id,
                    1 - (array_cosine_distance(embedding, ?::FLOAT[{_embedding_dim}]) / 2) as similarity
                FROM query_embeddings
                WHERE 1 - (array_cosine_distance(embedding, ?::FLOAT[{_embedding_dim}]) / 2) >= ?
                ORDER BY array_cosine_distance(embedding, ?::FLOAT[{_embedding_dim}]) ASC
                LIMIT ?
            """,
                [query_embedding, query_embedding, min_similarity, query_embedding, limit],
            ).fetchall()

        return [SimilarQuery(query_id=r[0], similarity=r[1]) for r in results]

    def count(self) -> int:
        """Get the number of stored embeddings.

        Returns:
            Count of embeddings in the database
        """
        with self.manager.vectors_cursor() as cursor:
            result = cursor.execute("SELECT COUNT(*) FROM query_embeddings").fetchone()
            return result[0] if result else 0


def build_embedding_text(
    summary: str,
    step_descriptions: list[str] | None = None,
    assumption_texts: list[str] | None = None,
    max_chars: int = 1000,
) -> str:
    """Build embedding text for a query document with smart truncation.

    Creates embedding text from a QueryDocument's semantic content, prioritizing
    the most important signals while staying within the model's token limit.

    Model constraint: all-MiniLM-L6-v2 has a 256 token limit (~1000 chars).

    Priority order:
    1. Summary (always included - primary semantic signal)
    2. Step descriptions (secondary - calculation methodology)
    3. Assumption texts (tertiary - ambiguity context)

    Text is truncated at max_chars to stay within model limits.

    Args:
        summary: LLM-generated summary of what the query calculates (required)
        step_descriptions: Descriptions of each SQL step (optional)
        assumption_texts: Human-readable assumptions (optional)
        max_chars: Maximum characters (default 1000 for ~250 tokens safety margin)

    Returns:
        Text to embed, truncated if necessary
    """
    parts = [summary]
    current_len = len(summary)

    # Add step descriptions if room
    if step_descriptions and current_len < max_chars:
        for desc in step_descriptions:
            if current_len + len(desc) + 1 < max_chars:
                parts.append(desc)
                current_len += len(desc) + 1

    # Add assumption texts if still room
    if assumption_texts and current_len < max_chars:
        for text in assumption_texts:
            if current_len + len(text) + 1 < max_chars:
                parts.append(text)
                current_len += len(text) + 1

    return " ".join(parts)


__all__ = [
    "QueryEmbeddings",
    "SimilarQuery",
    "generate_embedding",
    "generate_embeddings_batch",
    "build_embedding_text",
]
