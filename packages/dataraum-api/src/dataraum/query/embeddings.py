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


def _get_model() -> Any:
    """Get or load the sentence-transformers model (lazy loading)."""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer

            logger.info(f"Loading embedding model: {_model_name}")
            _model = SentenceTransformer(_model_name)
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
        if not self.manager.vectors_enabled:
            logger.warning("Vectors database not enabled, skipping embedding")
            return

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

        if not self.manager.vectors_enabled:
            logger.warning("Vectors database not enabled, skipping embeddings")
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
        if not self.manager.vectors_enabled:
            return

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
        if not self.manager.vectors_enabled:
            logger.warning("Vectors database not enabled")
            return []

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
        if not self.manager.vectors_enabled:
            return 0

        with self.manager.vectors_cursor() as cursor:
            result = cursor.execute("SELECT COUNT(*) FROM query_embeddings").fetchone()
            return result[0] if result else 0


def build_embedding_text_for_document(
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
        summary: LLM-generated summary of what the query calculates
        step_descriptions: Descriptions of each SQL step (optional)
        assumption_texts: Human-readable assumptions (optional)
        max_chars: Maximum characters (default 1000 for ~250 tokens safety margin)

    Returns:
        Text to embed, truncated if necessary
    """
    parts = [summary]  # Always include summary
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


def build_embedding_text_for_question(question: str) -> str:
    """Build embedding text for a natural language question.

    DEPRECATED: Use the question text directly for searches.
    This function exists for backwards compatibility.

    Args:
        question: The original question

    Returns:
        Text to embed
    """
    return question


def build_embedding_text_for_graph_output(
    summary: str,
    step_descriptions: list[str] | None = None,
    name: str | None = None,
) -> str:
    """Build embedding text from LLM-generated graph output.

    DEPRECATED: Use build_embedding_text_for_document() instead.

    Uses the summary and step descriptions generated by the graph agent,
    which provide semantic information specific to the generated SQL.

    Args:
        summary: LLM-generated summary of what the query calculates
        step_descriptions: Descriptions of each SQL step (optional)
        name: Graph name for additional context (optional)

    Returns:
        Text to embed
    """
    # Delegate to new unified function
    parts = [summary]
    if step_descriptions:
        parts.extend(step_descriptions)
    if name and name.lower() not in summary.lower():
        parts.insert(0, name)
    return " ".join(parts)


__all__ = [
    "QueryEmbeddings",
    "SimilarQuery",
    "generate_embedding",
    "generate_embeddings_batch",
    "build_embedding_text_for_document",
    "build_embedding_text_for_question",
    "build_embedding_text_for_graph_output",
]
