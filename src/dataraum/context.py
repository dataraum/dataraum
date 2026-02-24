"""Python API for DataRaum.

Provides a simple, Jupyter-friendly interface for exploring data context.

Example:
    from dataraum import Context

    ctx = Context("./pipeline_output")
    ctx.tables                    # List of tables
    ctx.entropy.summary()         # DataFrame with entropy scores
    ctx.contracts.evaluate("aggregation_safe")
    result = ctx.query("What's the total revenue?")
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.core import ConnectionManager
    from dataraum.query.models import QueryResult


class Context:
    """Main entry point for the DataRaum Python API.

    Provides accessors for exploring data, entropy, contracts, and querying.
    """

    def __init__(self, output_dir: str | Path) -> None:
        """Initialize context from a pipeline output directory.

        Args:
            output_dir: Path to pipeline output directory
        """
        self._output_dir = Path(output_dir)
        self._manager: ConnectionManager | None = None
        self._entropy: EntropyAccessor | None = None
        self._contracts: ContractsAccessor | None = None

    @property
    def manager(self) -> ConnectionManager:
        """Get the connection manager (lazy initialization)."""
        if self._manager is None:
            from dataraum.cli.common import get_manager

            self._manager = get_manager(self._output_dir)
        return self._manager

    def close(self) -> None:
        """Close the connection manager."""
        if self._manager is not None:
            self._manager.close()
            self._manager = None

    def __enter__(self) -> Context:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()

    @property
    def tables(self) -> list[str]:
        """Get list of table names."""
        from sqlalchemy import select

        from dataraum.storage import Source, Table

        with self.manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return []

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            return [t.table_name for t in tables]

    @property
    def entropy(self) -> EntropyAccessor:
        """Get entropy accessor."""
        if self._entropy is None:
            self._entropy = EntropyAccessor(self)
        return self._entropy

    @property
    def contracts(self) -> ContractsAccessor:
        """Get contracts accessor."""
        if self._contracts is None:
            self._contracts = ContractsAccessor(self)
        return self._contracts

    def query(self, question: str, contract: str | None = None) -> QueryResultWrapper:
        """Execute a natural language query.

        Args:
            question: Natural language question
            contract: Optional contract to evaluate against

        Returns:
            QueryResultWrapper with answer, SQL, data, and confidence
        """
        from sqlalchemy import select

        from dataraum.query import answer_question
        from dataraum.storage import Source

        with self.manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                raise ValueError("No sources found in database")

            source = sources[0]

            with self.manager.duckdb_cursor() as cursor:
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract,
                    manager=self.manager,
                )

            if not result.success or not result.value:
                raise ValueError(f"Query failed: {result.error}")

            return QueryResultWrapper(result.value)

    def context_document(self) -> str:
        """Get the full context document for LLM prompts."""
        from sqlalchemy import select

        from dataraum.graphs.context import build_execution_context, format_context_for_prompt
        from dataraum.storage import Source, Table

        with self.manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found"

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            table_ids = [t.table_id for t in tables]

            with self.manager.duckdb_cursor() as cursor:
                context = build_execution_context(
                    session=session,
                    table_ids=table_ids,
                    duckdb_conn=cursor,
                )

            return format_context_for_prompt(context)


class EntropyAccessor:
    """Accessor for entropy data."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def summary(self, table_name: str | None = None) -> dict[str, Any]:
        """Get entropy summary as a dictionary.

        Args:
            table_name: Optional table to filter to

        Returns:
            Dictionary with overall readiness, dimension scores, and column details
        """
        from sqlalchemy import select

        from dataraum.entropy.db_models import (
            EntropySnapshotRecord,
        )
        from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
        from dataraum.storage import Source

        with self._ctx.manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return {"error": "No sources found"}

            source = sources[0]

            # Get snapshot
            snapshot_result = session.execute(
                select(EntropySnapshotRecord)
                .where(EntropySnapshotRecord.source_id == source.source_id)
                .order_by(EntropySnapshotRecord.snapshot_at.desc())
                .limit(1)
            )
            snapshot = snapshot_result.scalar_one_or_none()

            if not snapshot:
                return {"error": "No entropy data"}

            # Get interpretations
            interp_query = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source.source_id
            )

            if table_name:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.table_name == table_name
                )

            interp_query = interp_query.order_by(
                EntropyInterpretationRecord.table_name,
                EntropyInterpretationRecord.column_name,
            )
            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            return {
                "source": source.name,
                "overall_readiness": snapshot.overall_readiness,
                "entropy_score": snapshot.avg_entropy_score,
                "columns": [
                    {
                        "table": i.table_name,
                        "column": i.column_name,
                        "explanation": i.explanation,
                    }
                    for i in interpretations
                ],
            }

    def table(self, table_name: str) -> dict[str, Any]:
        """Get entropy details for a specific table."""
        return self.summary(table_name=table_name)


class ContractsAccessor:
    """Accessor for contract evaluation."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def list(self) -> list[dict[str, str]]:
        """List available contracts."""
        from dataraum.entropy.contracts import list_contracts

        return list_contracts()

    def evaluate(self, contract_name: str) -> dict[str, Any]:
        """Evaluate a contract.

        Args:
            contract_name: Name of the contract to evaluate

        Returns:
            Dictionary with compliance status, scores, and violations
        """
        from sqlalchemy import select

        from dataraum.entropy.contracts import evaluate_contract, get_contract
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import _network_to_column_summaries
        from dataraum.storage import Source, Table

        with self._ctx.manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return {"error": "No sources found"}

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                return {"error": "No tables found"}

            table_ids = [t.table_id for t in tables]

            # Build column summaries via network
            network_ctx = build_for_network(session, table_ids)
            column_summaries = _network_to_column_summaries(network_ctx)

            profile = get_contract(contract_name)
            if profile is None:
                return {"error": f"Contract not found: {contract_name}"}

            evaluation = evaluate_contract(column_summaries, contract_name)

            return {
                "contract": contract_name,
                "is_compliant": evaluation.is_compliant,
                "confidence_level": evaluation.confidence_level.value,
                "overall_score": evaluation.overall_score,
                "dimension_scores": evaluation.dimension_scores,
                "violations": [
                    {
                        "dimension": v.dimension,
                        "actual": v.actual,
                        "max_allowed": v.max_allowed,
                        "details": v.details,
                    }
                    for v in evaluation.violations
                ],
                "warnings": [{"details": w.details} for w in evaluation.warnings],
            }


class QueryResultWrapper:
    """Wrapper for query results with Jupyter-friendly display."""

    def __init__(self, result: QueryResult) -> None:
        self._result = result

    @property
    def answer(self) -> str:
        """Natural language answer."""
        return self._result.answer

    @property
    def sql(self) -> str | None:
        """Generated SQL."""
        return self._result.sql

    @property
    def confidence(self) -> str:
        """Confidence level."""
        return self._result.confidence_level.label

    @property
    def data(self) -> list[dict[str, Any]] | None:
        """Query result data as list of dicts."""
        return self._result.data

    @property
    def columns(self) -> list[str] | None:
        """Column names."""
        return self._result.columns

    def to_dataframe(self) -> Any:
        """Convert to pandas DataFrame (requires pandas)."""
        try:
            import pandas as pd

            if self.data and self.columns:
                return pd.DataFrame(self.data, columns=self.columns)
            return pd.DataFrame()
        except ImportError as err:
            raise ImportError("pandas is required for to_dataframe()") from err

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        html = f"<b>Answer:</b> {self.answer}<br>"
        html += f"<b>Confidence:</b> {self.confidence}<br>"
        if self.sql:
            html += f"<b>SQL:</b> <code>{self.sql}</code><br>"
        if self.data:
            html += f"<b>Rows:</b> {len(self.data)}<br>"
        return html

    def __repr__(self) -> str:
        """String representation."""
        return f"QueryResult(answer={self.answer!r}, confidence={self.confidence}, rows={len(self.data) if self.data else 0})"
