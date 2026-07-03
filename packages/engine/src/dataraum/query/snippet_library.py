"""SQL Snippet Library — the engine-owned snippet Knowledge Base substrate.

Manages snippet lifecycle for the LIVE producer path (the GraphAgent in
``graphs/agent.py`` + ``metrics_phase``): upsert, exact-key discovery, and
usage/failure tracking. The natural-language CONSUMER discovery surface — key-based
graph search, full-graph injection, vocabulary, stats — moved to the cockpit TS tier
(the ``answer`` sub-agent, DAT-485/494) and was removed in DAT-487; the cockpit reads
the same ``sql_snippets`` substrate directly via Drizzle.

Discovery (graph agent only):
1. Exact key match — EXTRACT leaves (``find_by_key``, O(1) with index). EXTRACTs are
   the sole shared, cross-metric cache. FORMULA/CONSTANT snippets are NOT discovered
   here (DAT-646): they are composed per-metric and persisted source-scoped only for
   the cockpit reuse KB — never reused by expression shape (the old aliasing bug).

Usage:
    library = SnippetLibrary(session, workspace_id=workspace_id)

    # Exact lookup (dedup on mint)
    snippet = library.find_by_key(
        snippet_type="extract",
        standard_field="revenue",
        statement="income_statement",
        aggregation="sum",
        schema_mapping_id="schema_abc",
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

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


@dataclass
class SnippetMatch:
    """A snippet found by the discovery engine."""

    snippet: SQLSnippetRecord
    match_confidence: float  # 0.0-1.0
    match_strategy: str  # "exact_key"


class SnippetLibrary:
    """Service for managing the SQL Knowledge Base.

    Uses SQLAlchemy for snippet storage with term-based discovery
    (graph expansion, vocabulary matching) instead of embeddings.
    """

    def __init__(
        self,
        session: Session,
        *,
        workspace_id: str | None = None,
    ):
        """Initialize with database session.

        Args:
            session: SQLAlchemy session for snippet metadata
            workspace_id: Workspace id for per-row population.
                Required for paths that create new rows (snippet upsert,
                usage recording). Read-only paths (find_by_*, record_failure)
                may pass None.
        """
        self.session = session
        self.workspace_id = workspace_id

    def _require_workspace_id(self) -> str:
        if not self.workspace_id:
            raise RuntimeError(
                "SnippetLibrary write paths require workspace_id — "
                "construct with SnippetLibrary(session, workspace_id=...)."
            )
        return self.workspace_id

    # --- Discovery ---

    def find_by_id(self, snippet_id: str) -> SQLSnippetRecord | None:
        """Find a snippet by its primary key.

        Args:
            snippet_id: The snippet's unique identifier

        Returns:
            SQLSnippetRecord if found, None otherwise
        """
        return self.session.get(SQLSnippetRecord, snippet_id)

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
            SQLSnippetRecord.failure_count == 0,
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

    def retained_failure(
        self,
        snippet_type: str,
        schema_mapping_id: str,
        *,
        standard_field: str | None = None,
        statement: str | None = None,
        aggregation: str | None = None,
        parameter_value: str | None = None,
    ) -> SQLSnippetRecord | None:
        """The retained FAILED snippet for this semantic key (DAT-543), or None.

        Mirrors ``find_by_key`` but returns the row ONLY when it is flagged failed
        (``failure_count > 0``). The reuse read (``find_by_key``) excludes such rows;
        ``_build_prior_context`` uses this to feed the exact prior SQL + reason back
        to the next authoring (and the cockpit surfaces it on the ungroundable node).
        """
        rec = self._find_by_key_any(
            snippet_type=snippet_type,
            schema_mapping_id=schema_mapping_id,
            standard_field=standard_field,
            statement=statement,
            aggregation=aggregation,
            parameter_value=parameter_value,
        )
        return rec if (rec and rec.failure_count > 0) else None

    def _find_by_key_any(
        self,
        snippet_type: str,
        schema_mapping_id: str,
        *,
        standard_field: str | None = None,
        statement: str | None = None,
        aggregation: str | None = None,
        parameter_value: str | None = None,
    ) -> SQLSnippetRecord | None:
        """Find snippet by key, including failed ones. Used by save_snippet."""
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

        return self.session.execute(stmt).scalar_one_or_none()

    # --- Persistence ---

    def save_snippet(
        self,
        snippet_type: str,
        sql: str,
        description: str,
        schema_mapping_id: str,
        source: str,
        *,
        standard_field: str | None = None,
        statement: str | None = None,
        aggregation: str | None = None,
        parameter_value: str | None = None,
        normalized_expression: str | None = None,
        input_fields: list[str] | None = None,
        llm_model: str | None = None,
        column_hash: str | None = None,
        provenance: dict[str, Any] | None = None,
        failed: bool = False,
    ) -> SQLSnippetRecord:
        """Save a new snippet or update an existing one.

        Uses upsert semantics: if a snippet with the same semantic key exists,
        updates it. Otherwise creates a new one.

        ``failed=True`` retains an authored-but-unusable extract SQL (DAT-543):
        stored with ``failure_count=1`` so ``find_by_key`` (``failure_count == 0``)
        keeps it OUT of reuse, while ``_build_prior_context`` can read it back —
        the exact SQL + its ``provenance`` reason — so the next authoring revises
        precisely instead of re-deriving blind. A failed save NEVER clobbers a
        healthy existing snippet (first-writer-wins below still holds).

        Args:
            snippet_type: "extract", "constant", "formula", or "query"
            sql: The SQL fragment
            description: Human-readable description
            schema_mapping_id: Schema mapping identifier
            source: Provenance string (e.g. "graph:dso", "query:exec_456")
            standard_field: Standard field name (for extracts)
            statement: Statement type (for extracts)
            aggregation: Aggregation method (for extracts)
            parameter_value: Parameter value (for constants)
            normalized_expression: Normalized expression (for formulas)
            input_fields: Input field names (for formulas)
            llm_model: LLM model used to generate
            column_hash: Hash for schema change invalidation
            provenance: Grounding decisions (field_resolution, column_mappings_basis, etc.)

        Returns:
            The created or updated SQLSnippetRecord
        """
        # Try to find existing snippet by key (including failed ones)
        existing: SQLSnippetRecord | None = None
        if snippet_type in ("extract", "constant", "query"):
            existing = self._find_by_key_any(
                snippet_type=snippet_type,
                schema_mapping_id=schema_mapping_id,
                standard_field=standard_field,
                statement=statement,
                aggregation=aggregation,
                parameter_value=parameter_value,
            )
        elif snippet_type == "formula" and normalized_expression:
            # Per-metric identity (DAT-646): a formula snippet is unique per SOURCE
            # (``graph:{graph_id}``) + expression, NOT by expression alone. Two metrics
            # that share an arithmetic shape (``ebitda/revenue`` vs ``net_income/revenue``)
            # must NOT collapse to one row — that shape-keyed dedup was the cross-metric
            # aliasing bug. ``normalized_expression`` is retained as internal metadata.
            stmt = select(SQLSnippetRecord).where(
                SQLSnippetRecord.snippet_type == "formula",
                SQLSnippetRecord.schema_mapping_id == schema_mapping_id,
                SQLSnippetRecord.source == source,
                SQLSnippetRecord.normalized_expression == normalized_expression,
            )
            # ``.first()``, NOT ``scalar_one_or_none()``: formula rows have all-NULL
            # semantic-key columns, so ``uq_snippet_semantic_key`` (which backstops
            # extract/constant dedup) never fires for them — Postgres treats NULLs as
            # distinct. Under at-least-once activity redelivery two concurrent sessions
            # could each miss-then-insert the SAME (source, expression), leaving two
            # rows; ``scalar_one_or_none`` would then raise ``MultipleResultsFound`` on
            # the next save. These snippets are cockpit-KB-only (the engine never reads
            # them back), so taking any existing row is correct and crash-free.
            existing = self.session.execute(stmt).scalars().first()

        if existing and existing.failure_count > 0:
            # Refresh the existing failed row with this attempt. A clean success
            # heals it (failure_count → 0, reusable); another failure keeps it
            # flagged (→ 1) but records the latest SQL + reason for prior_context.
            existing.sql = sql
            existing.description = description
            existing.source = source
            existing.llm_model = llm_model
            existing.column_hash = column_hash
            existing.provenance = provenance
            existing.failure_count = 1 if failed else 0
            existing.updated_at = datetime.now(UTC)
            record = existing
        elif existing:
            # Healthy snippet — keep the original (first writer wins). A new FAILURE
            # never clobbers working SQL: we return the healthy row untouched.
            record = existing
        else:
            # Create new snippet
            record = SQLSnippetRecord(
                snippet_id=str(uuid4()),
                workspace_id=self._require_workspace_id(),
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
                source=source,
                llm_model=llm_model,
                column_hash=column_hash,
                provenance=provenance,
                execution_count=0,
                failure_count=1 if failed else 0,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            self.session.add(record)
            # Flush so a subsequent save_snippet in the same session sees it
            # via _find_by_key_* (which queries the DB, not the identity map).
            self.session.flush()
            logger.debug(
                "snippet_created",
                snippet_id=record.snippet_id,
                snippet_type=snippet_type,
                field=standard_field,
            )

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
            workspace_id=self._require_workspace_id(),
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

    def record_failure(self, snippet_ids: list[str]) -> None:
        """Increment failure_count for snippets that produced execution errors.

        Args:
            snippet_ids: Snippet IDs whose SQL failed during execution.
        """
        if not snippet_ids:
            return
        self.session.execute(
            update(SQLSnippetRecord)
            .where(SQLSnippetRecord.snippet_id.in_(snippet_ids))
            .values(failure_count=SQLSnippetRecord.failure_count + 1)
        )


__all__ = ["SnippetLibrary", "SnippetMatch"]
