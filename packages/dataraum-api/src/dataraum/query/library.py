"""Query Library for storing and retrieving reusable queries.

The library enables RAG-based query reuse:
1. Search by semantic similarity (embeddings)
2. Retrieve SQL and metadata from SQLite
3. Track usage for continuous improvement

Usage:
    library = QueryLibrary(session, manager)

    # Find similar queries
    match = library.find_similar(question, source_id, min_similarity=0.8)
    if match:
        # Reuse existing query
        sql = match.entry.final_sql

    # Save a new query
    library.save(
        source_id=source_id,
        question=question,
        sql=sql,
        assumptions=assumptions,
        confidence_level=confidence_level,
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import select, update

from dataraum.core.logging import get_logger
from dataraum.query.db_models import QueryExecutionRecord, QueryLibraryEntry
from dataraum.query.document import QueryDocument
from dataraum.query.embeddings import QueryEmbeddings, build_embedding_text_for_document

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)


class QueryLibraryError(Exception):
    """Error in query library operations."""

    pass


@dataclass
class LibraryMatch:
    """A match from the query library."""

    entry: QueryLibraryEntry
    similarity: float

    def to_context(self) -> dict[str, Any]:
        """Return full document for LLM context injection.

        This provides the complete semantic document that can be used
        to inform the LLM about a similar query that was previously executed.

        Returns:
            Dictionary with all relevant query information
        """
        return {
            "query_id": self.entry.query_id,
            "summary": self.entry.summary,
            "calculation_steps": self.entry.steps_json,
            "sql": self.entry.final_sql,
            "column_mappings": self.entry.column_mappings,
            "assumptions": self.entry.assumptions,
            "similarity_score": round(self.similarity, 3),
        }


class QueryLibrary:
    """Service for managing the query library.

    Combines SQLite (via SQLAlchemy) for metadata with DuckDB for vector search.
    Requires vectors database to be enabled.
    """

    def __init__(self, session: Session, manager: ConnectionManager):
        """Initialize with database connections.

        Args:
            session: SQLAlchemy session for metadata
            manager: ConnectionManager for vectors database

        Raises:
            QueryLibraryError: If vectors database is not enabled
        """
        if not manager.vectors_enabled:
            raise QueryLibraryError(
                "Query library requires vectors database. "
                "Configure vectors_path in ConnectionConfig."
            )

        self.session = session
        self._embeddings = QueryEmbeddings(manager)

    def find_similar(
        self,
        question: str,
        source_id: str,
        min_similarity: float = 0.7,
        limit: int = 1,
    ) -> LibraryMatch | None:
        """Find the most similar query in the library.

        Args:
            question: Natural language question
            source_id: Source ID to search within
            min_similarity: Minimum similarity threshold (0.0 to 1.0)
            limit: Maximum number of matches to consider

        Returns:
            LibraryMatch if found above threshold, None otherwise
        """
        similar = self._embeddings.find_similar(
            text=question,
            limit=limit * 2,  # Get extra to filter by source
            min_similarity=min_similarity,
        )

        if not similar:
            return None

        # Filter by source and get metadata
        for sim in similar:
            entry = self._get_entry(sim.query_id)
            if entry and entry.source_id == source_id:
                logger.info(
                    f"Found similar query: {sim.query_id} (similarity: {sim.similarity:.3f})"
                )
                return LibraryMatch(entry=entry, similarity=sim.similarity)

        return None

    def find_similar_all(
        self,
        question: str,
        source_id: str,
        min_similarity: float = 0.5,
        limit: int = 5,
    ) -> list[LibraryMatch]:
        """Find all similar queries above threshold.

        Args:
            question: Natural language question
            source_id: Source ID to search within
            min_similarity: Minimum similarity threshold
            limit: Maximum number of results

        Returns:
            List of LibraryMatch ordered by similarity
        """
        similar = self._embeddings.find_similar(
            text=question,
            limit=limit * 3,
            min_similarity=min_similarity,
        )

        results = []
        for sim in similar:
            entry = self._get_entry(sim.query_id)
            if entry and entry.source_id == source_id:
                results.append(LibraryMatch(entry=entry, similarity=sim.similarity))
                if len(results) >= limit:
                    break

        return results

    def save_document(
        self,
        source_id: str,
        document: QueryDocument,
        *,
        original_question: str | None = None,
        name: str | None = None,
        description: str | None = None,
        contract_name: str | None = None,
        confidence_level: str = "GREEN",
        graph_id: str | None = None,
    ) -> QueryLibraryEntry:
        """Save a complete query document to the library.

        This is the preferred method for saving queries as it preserves
        all semantic information (summary, steps, assumptions) for
        better similarity search and context retrieval.

        Args:
            source_id: Source ID this query belongs to
            document: Complete QueryDocument with all semantic fields
            original_question: Original natural language question (if from user)
            name: Optional name for the query
            description: Optional description
            contract_name: Contract used
            confidence_level: Confidence level string
            graph_id: Graph ID if seeded from graph execution

        Returns:
            Created QueryLibraryEntry
        """
        # Build embedding text from document (includes summary + steps + assumptions)
        embedding_text = build_embedding_text_for_document(
            summary=document.summary,
            step_descriptions=document.get_step_descriptions(),
            assumption_texts=document.get_assumption_texts(),
        )

        query_id = str(uuid4())

        entry = QueryLibraryEntry(
            query_id=query_id,
            source_id=source_id,
            original_question=original_question,
            graph_id=graph_id,
            name=name,
            description=description,
            summary=document.summary,
            steps_json=[s.to_dict() for s in document.steps],
            final_sql=document.final_sql,
            column_mappings=document.column_mappings,
            assumptions=[a.to_dict() for a in document.assumptions],
            contract_name=contract_name,
            confidence_level=confidence_level,
            embedding_text=embedding_text,
            created_at=datetime.now(UTC),
        )

        self.session.add(entry)
        self.session.flush()  # Get the ID without committing

        # Store embedding
        self._embeddings.add_query(query_id, embedding_text)

        logger.info(f"Saved query document to library: {query_id}")
        return entry

    def save(
        self,
        source_id: str,
        embedding_text: str,
        sql: str,
        *,
        original_question: str | None = None,
        name: str | None = None,
        description: str | None = None,
        summary: str | None = None,
        steps: list[dict[str, str]] | None = None,
        assumptions: list[dict[str, Any]] | None = None,
        column_mappings: dict[str, str] | None = None,
        contract_name: str | None = None,
        confidence_level: str = "GREEN",
        graph_id: str | None = None,
    ) -> QueryLibraryEntry:
        """Save a query to the library.

        Note: Prefer save_document() for new code as it preserves all semantic
        information. This method is kept for backwards compatibility.

        Args:
            source_id: Source ID this query belongs to
            embedding_text: Text to use for semantic search (question or summary)
            sql: Generated SQL
            original_question: Original natural language question (if from user)
            name: Optional name for the query
            description: Optional description
            summary: Plain English description of what the query calculates
            steps: List of calculation steps [{step_id, sql, description}]
            assumptions: List of assumption dicts
            column_mappings: Column name mappings
            contract_name: Contract used
            confidence_level: Confidence level string
            graph_id: Graph ID if seeded from graph execution

        Returns:
            Created QueryLibraryEntry
        """
        query_id = str(uuid4())

        entry = QueryLibraryEntry(
            query_id=query_id,
            source_id=source_id,
            original_question=original_question,
            graph_id=graph_id,
            name=name,
            description=description,
            summary=summary,
            steps_json=steps or [],
            final_sql=sql,
            column_mappings=column_mappings or {},
            assumptions=assumptions or [],
            contract_name=contract_name,
            confidence_level=confidence_level,
            embedding_text=embedding_text,
            created_at=datetime.now(UTC),
        )

        self.session.add(entry)
        self.session.flush()  # Get the ID without committing

        # Store embedding
        self._embeddings.add_query(query_id, embedding_text)

        logger.info(f"Saved query to library: {query_id}")
        return entry

    def record_execution(
        self,
        source_id: str,
        question: str,
        sql: str,
        *,
        library_entry_id: str | None = None,
        similarity_score: float | None = None,
        success: bool = True,
        row_count: int | None = None,
        error_message: str | None = None,
        confidence_level: str = "GREEN",
        contract_name: str | None = None,
    ) -> QueryExecutionRecord:
        """Record a query execution.

        Args:
            source_id: Source ID
            question: Question asked
            sql: SQL executed
            library_entry_id: Library entry if reused
            similarity_score: Similarity if reused
            success: Whether execution succeeded
            row_count: Number of rows returned
            error_message: Error if failed
            confidence_level: Confidence level
            contract_name: Contract used

        Returns:
            Created QueryExecutionRecord
        """
        record = QueryExecutionRecord(
            execution_id=str(uuid4()),
            library_entry_id=library_entry_id,
            source_id=source_id,
            question=question,
            sql_executed=sql,
            executed_at=datetime.now(UTC),
            success=success,
            row_count=row_count,
            error_message=error_message,
            confidence_level=confidence_level,
            contract_name=contract_name,
            similarity_score=similarity_score,
        )

        self.session.add(record)

        # Update usage count if reused
        if library_entry_id:
            self.session.execute(
                update(QueryLibraryEntry)
                .where(QueryLibraryEntry.query_id == library_entry_id)
                .values(
                    usage_count=QueryLibraryEntry.usage_count + 1,
                    last_used_at=datetime.now(UTC),
                )
            )

        return record

    def get_entry(self, query_id: str) -> QueryLibraryEntry | None:
        """Get a library entry by ID."""
        return self._get_entry(query_id)

    def list_entries(
        self,
        source_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[QueryLibraryEntry]:
        """List library entries for a source."""
        stmt = (
            select(QueryLibraryEntry)
            .where(QueryLibraryEntry.source_id == source_id)
            .order_by(QueryLibraryEntry.usage_count.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def delete_entry(self, query_id: str) -> bool:
        """Delete a library entry."""
        entry = self._get_entry(query_id)
        if not entry:
            return False

        self._embeddings.remove_query(query_id)
        self.session.delete(entry)
        return True

    def count(self, source_id: str | None = None) -> int:
        """Count library entries."""
        from sqlalchemy import func

        stmt = select(func.count(QueryLibraryEntry.query_id))
        if source_id:
            stmt = stmt.where(QueryLibraryEntry.source_id == source_id)
        result = self.session.execute(stmt).scalar()
        return result or 0

    def _get_entry(self, query_id: str) -> QueryLibraryEntry | None:
        """Get entry by ID (internal helper)."""
        return self.session.get(QueryLibraryEntry, query_id)


__all__ = ["QueryLibrary", "QueryLibraryError", "LibraryMatch"]
