"""SQL Snippet Library for the Knowledge Base.

Manages the lifecycle of SQL snippets: creation, discovery, usage tracking,
and stabilization metrics.

Discovery strategies (used differently by each agent):
1. Exact key match - Graph agent for extract/constant steps (O(1) with index)
2. Expression pattern match - Graph agent for formula steps (O(N), N < 100)
3. Semantic similarity - Query agent for NL questions (vector search in DuckDB VSS)

Usage:
    library = SnippetLibrary(session, manager)

    # Graph agent: exact lookup
    snippet = library.find_by_key(
        snippet_type="extract",
        standard_field="revenue",
        statement="income_statement",
        aggregation="sum",
        schema_mapping_id="schema_abc",
    )

    # Query agent: semantic search
    snippets = library.find_by_similarity(
        text="What is our DSO?",
        schema_mapping_id="schema_abc",
        limit=5,
    )

    # Save a new snippet
    library.save_snippet(
        snippet_type="extract",
        sql="SELECT SUM(Betrag) AS value FROM typed_transactions WHERE ...",
        description="Sum of revenue from income statement",
        schema_mapping_id="schema_abc",
        standard_field="revenue",
        statement="income_statement",
        aggregation="sum",
        source="graph:dso",
        confidence=1.0,
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import select, update

from dataraum.core.logging import get_logger
from dataraum.query.snippet_models import SnippetUsageRecord, SQLSnippetRecord
from dataraum.query.snippet_utils import normalize_expression

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)


class SnippetLibraryError(Exception):
    """Error in snippet library operations."""


@dataclass
class SnippetMatch:
    """A snippet found by the discovery engine."""

    snippet: SQLSnippetRecord
    match_confidence: float  # 0.0-1.0
    match_strategy: str  # "exact_key" | "expression_pattern" | "semantic_similarity"


class SnippetLibrary:
    """Service for managing the SQL Knowledge Base.

    Combines SQLite (via SQLAlchemy) for snippet storage with optional
    DuckDB vectors for semantic search.
    """

    def __init__(
        self,
        session: Session,
        manager: ConnectionManager | None = None,
    ):
        """Initialize with database connections.

        Args:
            session: SQLAlchemy session for snippet metadata
            manager: Optional ConnectionManager for semantic search (vectors database)
        """
        self.session = session
        self._manager = manager
        self._embeddings = None

        if manager and manager.vectors_enabled:
            from dataraum.query.embeddings import QueryEmbeddings

            self._embeddings = QueryEmbeddings(manager)

    # --- Discovery ---

    def find_by_key(
        self,
        snippet_type: str,
        schema_mapping_id: str,
        *,
        standard_field: str | None = None,
        statement: str | None = None,
        aggregation: str | None = None,
        parameter_value: str | None = None,
    ) -> SnippetMatch | None:
        """Find snippet by exact semantic key.

        Used by the graph agent for extract and constant steps.

        Args:
            snippet_type: "extract" or "constant"
            schema_mapping_id: Schema mapping identifier
            standard_field: Standard field name (for extracts)
            statement: Statement type (for extracts)
            aggregation: Aggregation method (for extracts)
            parameter_value: Parameter value (for constants)

        Returns:
            SnippetMatch if found, None otherwise
        """
        stmt = select(SQLSnippetRecord).where(
            SQLSnippetRecord.snippet_type == snippet_type,
            SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
        )

        if standard_field is not None:
            stmt = stmt.where(SQLSnippetRecord.standard_field == standard_field)
        else:
            stmt = stmt.where(SQLSnippetRecord.standard_field.is_(None))

        if statement is not None:
            stmt = stmt.where(SQLSnippetRecord.statement == statement)
        else:
            stmt = stmt.where(SQLSnippetRecord.statement.is_(None))

        if aggregation is not None:
            stmt = stmt.where(SQLSnippetRecord.aggregation == aggregation)
        else:
            stmt = stmt.where(SQLSnippetRecord.aggregation.is_(None))

        if parameter_value is not None:
            stmt = stmt.where(SQLSnippetRecord.parameter_value == parameter_value)
        else:
            stmt = stmt.where(SQLSnippetRecord.parameter_value.is_(None))

        record = self.session.execute(stmt).scalar_one_or_none()
        if record is None:
            return None

        return SnippetMatch(
            snippet=record,
            match_confidence=1.0,
            match_strategy="exact_key",
        )

    def find_by_expression(
        self,
        expression: str,
        schema_mapping_id: str,
    ) -> SnippetMatch | None:
        """Find formula snippet by normalized expression pattern.

        Used by the graph agent for formula steps.

        Args:
            expression: Formula expression to match
            schema_mapping_id: Schema mapping identifier

        Returns:
            SnippetMatch if found, None otherwise
        """
        normalized, sorted_fields, _ = normalize_expression(expression)

        stmt = select(SQLSnippetRecord).where(
            SQLSnippetRecord.snippet_type == "formula",
            SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
            SQLSnippetRecord.normalized_expression == normalized,
        )

        record = self.session.execute(stmt).scalar_one_or_none()
        if record is None:
            return None

        return SnippetMatch(
            snippet=record,
            match_confidence=0.9,
            match_strategy="expression_pattern",
        )

    def find_by_similarity(
        self,
        text: str,
        schema_mapping_id: str,
        *,
        min_similarity: float = 0.5,
        limit: int = 5,
    ) -> list[SnippetMatch]:
        """Find snippets by semantic similarity.

        Used by the query agent for natural language questions.

        Args:
            text: Text to search for (question or description)
            schema_mapping_id: Schema mapping identifier
            min_similarity: Minimum similarity threshold
            limit: Maximum number of results

        Returns:
            List of SnippetMatch ordered by similarity (descending)
        """
        if self._embeddings is None:
            return []

        # Search embeddings (prefixed with "snippet:" to distinguish from query library)
        similar = self._embeddings.find_similar(
            text=text,
            limit=limit * 3,  # Over-fetch to filter by schema
            min_similarity=min_similarity,
        )

        results: list[SnippetMatch] = []
        for sim in similar:
            # Only consider snippet embeddings (prefixed with "snippet:")
            if not sim.query_id.startswith("snippet:"):
                continue

            snippet_id = sim.query_id[len("snippet:"):]
            record = self.session.get(SQLSnippetRecord, snippet_id)
            if record and record.schema_mapping_id == schema_mapping_id:
                results.append(
                    SnippetMatch(
                        snippet=record,
                        match_confidence=sim.similarity,
                        match_strategy="semantic_similarity",
                    )
                )
                if len(results) >= limit:
                    break

        return results

    def find_all_for_schema(
        self,
        schema_mapping_id: str,
        *,
        snippet_types: list[str] | None = None,
    ) -> list[SQLSnippetRecord]:
        """Find all snippets for a schema mapping.

        Args:
            schema_mapping_id: Schema mapping identifier
            snippet_types: Optional filter by snippet type(s)

        Returns:
            List of all matching snippets
        """
        stmt = select(SQLSnippetRecord).where(
            SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
        )
        if snippet_types:
            stmt = stmt.where(SQLSnippetRecord.snippet_type.in_(snippet_types))

        return list(self.session.scalars(stmt))

    # --- Persistence ---

    def save_snippet(
        self,
        snippet_type: str,
        sql: str,
        description: str,
        schema_mapping_id: str,
        source: str,
        *,
        confidence: float = 0.5,
        standard_field: str | None = None,
        statement: str | None = None,
        aggregation: str | None = None,
        parameter_value: str | None = None,
        normalized_expression: str | None = None,
        input_fields: list[str] | None = None,
        column_mappings: dict[str, str] | None = None,
        llm_model: str | None = None,
        column_hash: str | None = None,
    ) -> SQLSnippetRecord:
        """Save a new snippet or update an existing one.

        Uses upsert semantics: if a snippet with the same semantic key exists,
        updates it. Otherwise creates a new one.

        Args:
            snippet_type: "extract", "constant", "formula", or "query"
            sql: The SQL fragment
            description: Human-readable description
            schema_mapping_id: Schema mapping identifier
            source: Provenance string (e.g. "graph:dso", "query:exec_456")
            confidence: Confidence score 0.0-1.0
            standard_field: Standard field name (for extracts)
            statement: Statement type (for extracts)
            aggregation: Aggregation method (for extracts)
            parameter_value: Parameter value (for constants)
            normalized_expression: Normalized expression (for formulas)
            input_fields: Input field names (for formulas)
            column_mappings: Column mappings
            llm_model: LLM model used to generate
            column_hash: Hash for schema change invalidation

        Returns:
            The created or updated SQLSnippetRecord
        """
        # Try to find existing snippet by key
        existing: SQLSnippetRecord | None = None
        if snippet_type in ("extract", "constant"):
            match = self.find_by_key(
                snippet_type=snippet_type,
                schema_mapping_id=schema_mapping_id,
                standard_field=standard_field,
                statement=statement,
                aggregation=aggregation,
                parameter_value=parameter_value,
            )
            if match:
                existing = match.snippet
        elif snippet_type == "formula" and normalized_expression:
            # Check for existing formula with same expression
            stmt = select(SQLSnippetRecord).where(
                SQLSnippetRecord.snippet_type == "formula",
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
                SQLSnippetRecord.normalized_expression == normalized_expression,
            )
            existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update existing snippet
            existing.sql = sql
            existing.description = description
            existing.source = source
            existing.confidence = confidence
            existing.llm_model = llm_model
            existing.column_mappings = column_mappings or {}
            existing.column_hash = column_hash
            existing.updated_at = datetime.now(UTC)
            record = existing
            logger.debug(f"Updated snippet {record.snippet_id} ({snippet_type}:{standard_field})")
        else:
            # Create new snippet
            record = SQLSnippetRecord(
                snippet_id=str(uuid4()),
                snippet_type=snippet_type,
                standard_field=standard_field,
                statement=statement,
                aggregation=aggregation,
                schema_mapping_id=schema_mapping_id,
                parameter_value=parameter_value,
                normalized_expression=normalized_expression,
                input_fields=input_fields,
                sql=sql,
                description=description,
                column_mappings=column_mappings or {},
                source=source,
                llm_model=llm_model,
                confidence=confidence,
                column_hash=column_hash,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            self.session.add(record)
            logger.debug(f"Created snippet {record.snippet_id} ({snippet_type}:{standard_field})")

        # Store embedding for semantic search (query and formula snippets)
        if self._embeddings and description:
            embedding_text = description
            if standard_field:
                embedding_text = f"{standard_field}: {description}"
            record.embedding_text = embedding_text
            self._embeddings.add_query(f"snippet:{record.snippet_id}", embedding_text)

        return record

    # --- Usage Tracking ---

    def record_usage(
        self,
        execution_id: str,
        execution_type: str,
        usage_type: str,
        *,
        snippet_id: str | None = None,
        match_confidence: float = 0.0,
        sql_match_ratio: float = 0.0,
        step_id: str | None = None,
    ) -> SnippetUsageRecord:
        """Record how a snippet was used in an execution.

        Args:
            execution_id: The execution that used (or didn't use) the snippet
            execution_type: "graph" or "query"
            usage_type: "exact_reuse", "adapted", "provided_not_used", "newly_generated"
            snippet_id: The snippet ID (None for newly_generated)
            match_confidence: Confidence at discovery time
            sql_match_ratio: Similarity between generated and snippet SQL
            step_id: The step ID this usage relates to

        Returns:
            Created SnippetUsageRecord
        """
        record = SnippetUsageRecord(
            usage_id=str(uuid4()),
            execution_id=execution_id,
            execution_type=execution_type,
            snippet_id=snippet_id,
            usage_type=usage_type,
            match_confidence=match_confidence,
            sql_match_ratio=sql_match_ratio,
            step_id=step_id,
            created_at=datetime.now(UTC),
        )
        self.session.add(record)

        # Update snippet usage stats
        if snippet_id and usage_type in ("exact_reuse", "adapted"):
            self.session.execute(
                update(SQLSnippetRecord)
                .where(SQLSnippetRecord.snippet_id == snippet_id)
                .values(
                    execution_count=SQLSnippetRecord.execution_count + 1,
                    last_used_at=datetime.now(UTC),
                )
            )

        return record

    # --- Invalidation ---

    def invalidate_for_schema(self, schema_mapping_id: str) -> int:
        """Invalidate all snippets for a schema mapping.

        Called when the schema mapping changes (e.g., new data loaded).

        Args:
            schema_mapping_id: Schema mapping to invalidate

        Returns:
            Number of snippets invalidated
        """
        stmt = select(SQLSnippetRecord).where(
            SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
        )
        snippets = list(self.session.scalars(stmt))

        for snippet in snippets:
            snippet.is_validated = False

        count = len(snippets)
        if count > 0:
            logger.info(f"Invalidated {count} snippets for schema {schema_mapping_id}")
        return count

    # --- Statistics ---

    def get_stats(
        self,
        schema_mapping_id: str | None = None,
    ) -> dict[str, Any]:
        """Compute stabilization metrics for the knowledge base.

        Args:
            schema_mapping_id: Optional filter to a specific schema

        Returns:
            Dictionary with stabilization metrics
        """
        from sqlalchemy import func

        # Count snippets
        snippet_query = select(func.count(SQLSnippetRecord.snippet_id))
        if schema_mapping_id:
            snippet_query = snippet_query.where(
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id
            )
        total_snippets = self.session.execute(snippet_query).scalar() or 0

        # Count validated snippets
        validated_query = select(func.count(SQLSnippetRecord.snippet_id)).where(
            SQLSnippetRecord.execution_count > 0
        )
        if schema_mapping_id:
            validated_query = validated_query.where(
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id
            )
        validated_snippets = self.session.execute(validated_query).scalar() or 0

        # Count snippets by type
        type_query = select(
            SQLSnippetRecord.snippet_type,
            func.count(SQLSnippetRecord.snippet_id),
        ).group_by(SQLSnippetRecord.snippet_type)
        if schema_mapping_id:
            type_query = type_query.where(
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id
            )
        snippets_by_type: dict[str, int] = {
            row[0]: row[1] for row in self.session.execute(type_query).all()
        }

        # Count snippets by source prefix
        source_query = select(
            SQLSnippetRecord.source,
            func.count(SQLSnippetRecord.snippet_id),
        ).group_by(SQLSnippetRecord.source)
        if schema_mapping_id:
            source_query = source_query.where(
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id
            )
        raw_sources: dict[str, int] = {
            row[0]: row[1] for row in self.session.execute(source_query).all()
        }
        snippets_by_source: dict[str, int] = {"graph": 0, "query": 0}
        for src, count in raw_sources.items():
            prefix = src.split(":")[0] if ":" in src else src
            snippets_by_source[prefix] = snippets_by_source.get(prefix, 0) + count

        # Usage statistics
        usage_query = select(func.count(SnippetUsageRecord.usage_id))
        if schema_mapping_id:
            # Join to snippets to filter by schema
            usage_query = usage_query.join(
                SQLSnippetRecord,
                SnippetUsageRecord.snippet_id == SQLSnippetRecord.snippet_id,
                isouter=True,
            ).where(
                (SQLSnippetRecord.schema_mapping_id == schema_mapping_id)
                | (SnippetUsageRecord.snippet_id.is_(None))
            )
        total_usages = self.session.execute(usage_query).scalar() or 0

        # Count by usage type
        usage_type_query = select(
            SnippetUsageRecord.usage_type,
            func.count(SnippetUsageRecord.usage_id),
        ).group_by(SnippetUsageRecord.usage_type)
        usage_by_type: dict[str, int] = {
            row[0]: row[1] for row in self.session.execute(usage_type_query).all()
        }

        # Cache hit rate
        reused = usage_by_type.get("exact_reuse", 0) + usage_by_type.get("adapted", 0)
        generated = usage_by_type.get("newly_generated", 0)
        total_steps = reused + generated + usage_by_type.get("provided_not_used", 0)
        cache_hit_rate = reused / total_steps if total_steps > 0 else 0.0

        # Most reused snippets
        most_reused_query = (
            select(
                SQLSnippetRecord.snippet_id,
                SQLSnippetRecord.snippet_type,
                SQLSnippetRecord.standard_field,
                SQLSnippetRecord.execution_count,
            )
            .order_by(SQLSnippetRecord.execution_count.desc())
            .limit(10)
        )
        if schema_mapping_id:
            most_reused_query = most_reused_query.where(
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id
            )
        most_reused = [
            {
                "snippet_id": r[0],
                "snippet_type": r[1],
                "standard_field": r[2],
                "usage_count": r[3],
            }
            for r in self.session.execute(most_reused_query).all()
            if r[3] > 0  # Only include actually-used snippets
        ]

        return {
            "total_snippets": total_snippets,
            "validated_snippets": validated_snippets,
            "snippets_by_type": snippets_by_type,
            "snippets_by_source": snippets_by_source,
            "total_usages": total_usages,
            "usage_by_type": usage_by_type,
            "steps_from_cache": reused,
            "steps_generated_fresh": generated,
            "cache_hit_rate": round(cache_hit_rate, 3),
            "most_reused": most_reused,
        }


__all__ = ["SnippetLibrary", "SnippetLibraryError", "SnippetMatch"]
