"""Python API for DataRaum.

Provides a simple, Jupyter-friendly interface for exploring data context.

Example:
    from dataraum import Context

    ctx = Context("./pipeline_output")
    ctx.tables                    # List of tables
    ctx.entropy.summary()         # Entropy scores and readiness
    ctx.contracts.evaluate("aggregation_safe")
    ctx.actions()                 # Resolution actions
    result = ctx.query("What's the total revenue?")
"""

from __future__ import annotations

import builtins
import html as html_mod
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.core import ConnectionManager
    from dataraum.query.models import QueryResult


class Context:
    """Main entry point for the DataRaum Python API.

    Provides accessors for exploring data, entropy, contracts, and querying.
    """

    def __init__(self, output_dir: str | Path, *, source: str | None = None) -> None:
        """Initialize context from a pipeline output directory.

        Args:
            output_dir: Path to pipeline output directory.
            source: Source name to use. If None, picks the source with typed tables.
        """
        self._output_dir = Path(output_dir)
        self._source_name = source
        self._manager: ConnectionManager | None = None
        self._entropy: EntropyAccessor | None = None
        self._contracts: ContractsAccessor | None = None
        self._sources: SourcesAccessor | None = None

    @property
    def manager(self) -> ConnectionManager:
        """Get the connection manager (lazy initialization)."""
        if self._manager is None:
            from dataraum.core.connections import get_manager_for_directory

            try:
                self._manager = get_manager_for_directory(self._output_dir)
            except FileNotFoundError:
                raise RuntimeError(
                    f"No pipeline output found at {self._output_dir}. "
                    "Run ctx.run() first or point to an existing output directory."
                ) from None
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

    def _resolve_source(self, session: Any) -> Any:
        """Find the active source — the one with typed tables.

        Args:
            session: SQLAlchemy session.

        Returns:
            Source object, or None if no sources exist.

        Raises:
            ValueError: If an explicit source name was given but not found.
        """
        from sqlalchemy import func, select

        from dataraum.storage import Source, Table

        sources_result = session.execute(select(Source).where(Source.archived_at.is_(None)))
        sources = sources_result.scalars().all()
        if not sources:
            return None

        # Explicit source name
        if self._source_name:
            for s in sources:
                if s.name == self._source_name:
                    return s
            available = [s.name for s in sources]
            raise ValueError(f"Source '{self._source_name}' not found. Available: {available}")

        # Pick the source that has typed tables
        for s in sources:
            count = session.execute(
                select(func.count())
                .select_from(Table)
                .where(
                    Table.source_id == s.source_id,
                    Table.layer == "typed",
                )
            ).scalar()
            if count > 0:
                return s

        # Fallback to first source
        return sources[0]

    @property
    def tables(self) -> list[str]:
        """Get list of table names."""
        from sqlalchemy import select

        from dataraum.storage import Table

        with self.manager.session_scope() as session:
            source = self._resolve_source(session)
            if not source:
                return []

            tables_result = session.execute(
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
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

    @property
    def sources(self) -> SourcesAccessor:
        """Get sources accessor for managing data sources."""
        if self._sources is None:
            self._sources = SourcesAccessor(self)
        return self._sources

    def run(
        self,
        source: str | Path | None = None,
        *,
        name: str | None = None,
        phase: str | None = None,
        contract: str = "aggregation_safe",
    ) -> RunResultWrapper:
        """Run the analysis pipeline.

        Args:
            source: Path to CSV file or directory. None uses registered sources.
            name: Name for the data source (default: derived from path).
            phase: Run only this phase and its dependencies.
            contract: Contract name for entropy thresholds (e.g., "aggregation_safe").

        Returns:
            RunResultWrapper with pipeline results summary.
        """
        import warnings

        from dataraum.pipeline.scheduler import PipelineResult
        from dataraum.pipeline.setup import setup_pipeline

        source_path = Path(source).resolve() if source else None

        setup = setup_pipeline(
            source_path=source_path,
            output_dir=self._output_dir,
            source_name=name,
            target_phase=phase,
            contract=contract,
        )

        # Drive the scheduler generator (no interactive gates — skip all)
        gen = setup.scheduler.run()

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
                warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")

                result: PipelineResult | None = None
                try:
                    next(gen)
                    while True:
                        next(gen)
                except StopIteration as e:
                    result = e.value
        finally:
            # Close the setup's own connections (not self._manager)
            setup.session.close()
            setup.manager.close()

            # Invalidate cached manager so it picks up new data
            if self._manager is not None:
                self._manager.close()
                self._manager = None

        if result is None:
            return RunResultWrapper(
                {
                    "success": False,
                    "error": "Pipeline ended without result",
                    "phases_completed": [],
                    "phases_failed": [],
                }
            )

        return RunResultWrapper(
            {
                "success": result.success,
                "phases_completed": result.phases_completed,
                "phases_failed": result.phases_failed,
                "phases_skipped": result.phases_skipped,
                "final_scores": result.final_scores,
                "error": result.error,
            }
        )

    def actions(self, contract: str | None = None) -> ActionsResultWrapper:
        """Get merged resolution actions, optionally filtered by contract.

        Args:
            contract: Optional contract name to filter violations

        Returns:
            ActionsResultWrapper with ranked actions and Jupyter rendering.
        """
        from collections import defaultdict

        from sqlalchemy import select

        from dataraum.entropy.actions import merge_actions
        from dataraum.entropy.contracts import evaluate_all_contracts
        from dataraum.entropy.db_models import EntropyObjectRecord
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import network_to_column_summaries
        from dataraum.storage import Column, Table

        with self.manager.session_scope() as session:
            source = self._resolve_source(session)
            if not source:
                return ActionsResultWrapper([])

            tables_result = session.execute(
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
            )
            tables = tables_result.scalars().all()

            if not tables:
                return ActionsResultWrapper([])

            table_ids = [t.table_id for t in tables]

            # Build column_id -> column_key mapping
            col_id_to_key: dict[str, str] = {}
            for tbl in tables:
                cols_result = session.execute(select(Column).where(Column.table_id == tbl.table_id))
                for col in cols_result.scalars().all():
                    col_id_to_key[col.column_id] = f"{tbl.table_name}.{col.column_name}"

            # Build column summaries and network context
            network_context = build_for_network(session, table_ids)
            column_summaries = network_to_column_summaries(network_context)

            # Interpretation records removed; pass empty dict
            interp_by_col: dict[str, Any] = {}

            # Get entropy objects by column
            entropy_objects_by_col: dict[str, list[Any]] = defaultdict(list)
            obj_result = session.execute(
                select(EntropyObjectRecord).where(
                    EntropyObjectRecord.source_id == source.source_id,
                )
            )
            for obj in obj_result.scalars().all():
                col_key = obj.target.removeprefix("column:")
                entropy_objects_by_col[col_key].append(obj)

            # Contract violations
            violation_dims: dict[str, list[str]] = {}
            evals = evaluate_all_contracts(column_summaries)
            target_evals = {contract: evals[contract]} if contract and contract in evals else evals
            for eval_result in target_evals.values():
                for v in eval_result.violations:
                    if v.dimension:
                        violation_dims.setdefault(v.dimension, []).extend(v.affected_columns)

            actions_list = merge_actions(
                interp_by_col,
                entropy_objects_by_col,
                violation_dims,
                network_context=network_context,
            )

            return ActionsResultWrapper(actions_list)

    def query(self, question: str, contract: str | None = None) -> QueryResultWrapper:
        """Execute a natural language query.

        Args:
            question: Natural language question
            contract: Optional contract to evaluate against

        Returns:
            QueryResultWrapper with answer, SQL, data, and confidence
        """
        from dataraum.query import answer_question

        with self.manager.session_scope() as session:
            source = self._resolve_source(session)
            if not source:
                raise ValueError("No sources found in database")

            with self.manager.duckdb_cursor() as cursor:
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract,
                )

            if not result.success or not result.value:
                raise ValueError(f"Query failed: {result.error}")

            return QueryResultWrapper(result.value)


class EntropyAccessor:
    """Accessor for entropy data."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def summary(self, table_name: str | None = None) -> EntropyResultWrapper:
        """Get entropy summary.

        Args:
            table_name: Optional table to filter to

        Returns:
            EntropyResultWrapper with readiness, dimension scores, and Jupyter rendering.
        """
        from dataraum.entropy.engine import compute_network
        from dataraum.entropy.views.query_context import network_to_column_summaries

        with self._ctx.manager.session_scope() as session:
            source = self._ctx._resolve_source(session)
            if not source:
                return EntropyResultWrapper({"error": "No sources found"})

            # Compute network from persisted records
            network_ctx = compute_network(session, source.source_id)
            if network_ctx is None:
                return EntropyResultWrapper({"error": "No entropy data"})

            # Interpretation records removed; use empty list
            interpretations: list[Any] = []

            # Build dimension scores from network + direct signals
            dimension_scores: dict[str, float] = {}
            dim_warning: str | None = None
            try:
                col_summaries = network_to_column_summaries(network_ctx)

                # Filter by table_name if requested
                if table_name:
                    col_summaries = {
                        k: v
                        for k, v in col_summaries.items()
                        if v.table_name == table_name or v.table_name.endswith(f"__{table_name}")
                    }

                dim_totals: dict[str, list[float]] = {}
                for col_summary in col_summaries.values():
                    for dim_path, score in col_summary.dimension_scores.items():
                        dim_totals.setdefault(dim_path, []).append(score)
                dimension_scores = {
                    dim: sum(scores) / len(scores) for dim, scores in dim_totals.items()
                }
            except Exception as exc:
                import warnings

                dim_warning = f"Could not compute dimension scores: {exc}"
                warnings.warn(dim_warning, stacklevel=2)

            return EntropyResultWrapper(
                {
                    "source": source.name,
                    "overall_readiness": network_ctx.overall_readiness,
                    "entropy_score": network_ctx.avg_entropy_score,
                    "dimension_scores": dimension_scores,
                    "columns": [
                        {
                            "table": i.table_name,
                            "column": i.column_name,
                            "explanation": i.explanation,
                        }
                        for i in interpretations
                    ],
                }
            )

    def details(self, table_name: str, column_name: str) -> dict[str, Any]:
        """Get full detector evidence for a specific column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            Dictionary with detector-level evidence and scores
        """
        from sqlalchemy import select

        from dataraum.entropy.db_models import EntropyObjectRecord

        with self._ctx.manager.session_scope() as session:
            source = self._ctx._resolve_source(session)
            if not source:
                return {"error": "No sources found"}

            target = f"column:{table_name}.{column_name}"

            objects_result = session.execute(
                select(EntropyObjectRecord).where(
                    EntropyObjectRecord.source_id == source.source_id,
                    EntropyObjectRecord.target == target,
                )
            )
            objects = objects_result.scalars().all()

            return {
                "table": table_name,
                "column": column_name,
                "target": target,
                "detectors": [
                    {
                        "detector_id": obj.detector_id,
                        "layer": obj.layer,
                        "dimension": obj.dimension,
                        "sub_dimension": obj.sub_dimension,
                        "score": obj.score,
                        "evidence": obj.evidence,
                        "resolution_options": obj.resolution_options,
                    }
                    for obj in objects
                ],
            }

    def table(self, table_name: str) -> EntropyResultWrapper:
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

    def evaluate(self, contract_name: str) -> ContractResultWrapper:
        """Evaluate a contract.

        Args:
            contract_name: Name of the contract to evaluate

        Returns:
            ContractResultWrapper with compliance status and Jupyter rendering.
        """
        from sqlalchemy import select

        from dataraum.entropy.contracts import evaluate_contract, get_contract
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import network_to_column_summaries
        from dataraum.storage import Table

        with self._ctx.manager.session_scope() as session:
            source = self._ctx._resolve_source(session)
            if not source:
                return ContractResultWrapper({"error": "No sources found"})

            tables_result = session.execute(
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
            )
            tables = tables_result.scalars().all()

            if not tables:
                return ContractResultWrapper({"error": "No tables found"})

            table_ids = [t.table_id for t in tables]

            # Build column summaries via network
            network_ctx = build_for_network(session, table_ids)
            column_summaries = network_to_column_summaries(network_ctx)

            profile = get_contract(contract_name)
            if profile is None:
                return ContractResultWrapper({"error": f"Contract not found: {contract_name}"})

            evaluation = evaluate_contract(column_summaries, contract_name)

            return ContractResultWrapper(
                {
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
            )


class SourcesAccessor:
    """Accessor for source management."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def list(self) -> builtins.list[dict[str, Any]]:
        """List registered data sources.

        Returns:
            List of source info dicts with name, type, status, path.
        """
        from dataraum.core.credentials import CredentialChain
        from dataraum.sources.manager import SourceManager

        with self._ctx.manager.session_scope() as session:
            sm = SourceManager(session=session, credential_chain=CredentialChain())
            return [
                {
                    "name": s.name,
                    "type": s.source_type,
                    "status": s.status,
                    "path": s.path,
                    "columns": len(s.columns) if s.columns else 0,
                }
                for s in sm.list_sources()
            ]

    def add(self, name: str, path: str | Path) -> dict[str, Any]:
        """Register a file source.

        Args:
            name: Source name (lowercase, a-z/0-9/_, starts with letter).
            path: Path to data file.

        Returns:
            Dict with source info or error.
        """
        from dataraum.core.credentials import CredentialChain
        from dataraum.sources.manager import SourceManager

        with self._ctx.manager.session_scope() as session:
            sm = SourceManager(session=session, credential_chain=CredentialChain())
            result = sm.add_file_source(name, str(Path(path).resolve()))

            if not result.success:
                return {"error": result.error}

            info = result.value
            assert info is not None
            return {
                "name": info.name,
                "type": info.source_type,
                "status": info.status,
                "columns": len(info.columns) if info.columns else 0,
            }

    def remove(self, name: str, purge: bool = False) -> str:
        """Remove (archive) a source.

        Args:
            name: Source name to remove.
            purge: If True, hard-delete the source record.

        Returns:
            Confirmation message.
        """
        from dataraum.core.credentials import CredentialChain
        from dataraum.sources.manager import SourceManager

        with self._ctx.manager.session_scope() as session:
            sm = SourceManager(session=session, credential_chain=CredentialChain())
            result = sm.remove_source(name, purge=purge)

            if not result.success:
                raise ValueError(result.error)

            return result.value or "Removed"


# ---------------------------------------------------------------------------
# Result wrappers with Jupyter HTML rendering
# ---------------------------------------------------------------------------


def _score_color(score: float) -> str:
    """Return CSS color for an entropy score."""
    if score < 0.2:
        return "#2ecc71"  # green
    if score < 0.5:
        return "#f39c12"  # amber
    return "#e74c3c"  # red


class EntropyResultWrapper:
    """Wrapper for entropy summary with Jupyter rendering."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> Any:
        """Dict-like keys access."""
        return self._data.keys()

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like get."""
        return self._data.get(key, default)

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        if "error" in self._data:
            return f"<b style='color:#e74c3c'>{html_mod.escape(self._data['error'])}</b>"

        readiness = self._data.get("overall_readiness", "unknown")
        score = self._data.get("entropy_score", 0)
        source = self._data.get("source", "")
        color = _score_color(score)

        parts = [
            f"<h3>Entropy: {html_mod.escape(source)}</h3>",
            f"<b>Readiness:</b> <span style='color:{color}'>"
            f"{html_mod.escape(str(readiness).upper())}</span> "
            f"(score: {score:.3f})<br>",
        ]

        dims = self._data.get("dimension_scores", {})
        if dims:
            parts.append("<b>Dimensions:</b><table><tr><th>Dimension</th><th>Score</th></tr>")
            for dim, dscore in sorted(dims.items(), key=lambda x: -x[1]):
                if dscore > 0:
                    dc = _score_color(dscore)
                    parts.append(
                        f"<tr><td>{html_mod.escape(dim)}</td>"
                        f"<td style='color:{dc}'>{dscore:.3f}</td></tr>"
                    )
            parts.append("</table>")

        cols = self._data.get("columns", [])
        if cols:
            parts.append(f"<b>Columns analyzed:</b> {len(cols)}")

        return "\n".join(parts)

    def __repr__(self) -> str:
        if "error" in self._data:
            return f"EntropyResult(error={self._data['error']!r})"
        return (
            f"EntropyResult(readiness={self._data.get('overall_readiness')!r}, "
            f"score={self._data.get('entropy_score', 0):.3f}, "
            f"columns={len(self._data.get('columns', []))})"
        )


class ContractResultWrapper:
    """Wrapper for contract evaluation with Jupyter rendering."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> Any:
        """Dict-like keys access."""
        return self._data.keys()

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like get."""
        return self._data.get(key, default)

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        if "error" in self._data:
            return f"<b style='color:#e74c3c'>{html_mod.escape(self._data['error'])}</b>"

        name = self._data.get("contract", "")
        compliant = self._data.get("is_compliant", False)
        score = self._data.get("overall_score", 0)
        status_color = "#2ecc71" if compliant else "#e74c3c"
        status_text = "PASS" if compliant else "FAIL"

        parts = [
            f"<h3>Contract: {html_mod.escape(name)}</h3>",
            f"<b>Status:</b> <span style='color:{status_color}'>{status_text}</span> "
            f"(score: {score:.2f})<br>",
        ]

        dims = self._data.get("dimension_scores", {})
        if dims:
            parts.append("<b>Dimensions:</b><table><tr><th>Dimension</th><th>Score</th></tr>")
            for dim, dscore in sorted(dims.items()):
                dc = _score_color(dscore)
                parts.append(
                    f"<tr><td>{html_mod.escape(dim)}</td>"
                    f"<td style='color:{dc}'>{dscore:.3f}</td></tr>"
                )
            parts.append("</table>")

        violations = self._data.get("violations", [])
        if violations:
            parts.append(f"<b style='color:#e74c3c'>Violations ({len(violations)}):</b><ul>")
            for v in violations:
                parts.append(f"<li>{html_mod.escape(v.get('details', ''))}</li>")
            parts.append("</ul>")

        return "\n".join(parts)

    def __repr__(self) -> str:
        if "error" in self._data:
            return f"ContractResult(error={self._data['error']!r})"
        status = "PASS" if self._data.get("is_compliant") else "FAIL"
        return (
            f"ContractResult(contract={self._data.get('contract')!r}, "
            f"status={status}, score={self._data.get('overall_score', 0):.2f})"
        )


class ActionsResultWrapper:
    """Wrapper for resolution actions with Jupyter rendering."""

    def __init__(self, actions: list[dict[str, Any]]) -> None:
        self._actions = actions

    def __len__(self) -> int:
        return len(self._actions)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._actions)

    def __getitem__(self, index: int) -> dict[str, Any]:
        return self._actions[index]

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        if not self._actions:
            return "<b>No resolution actions found.</b>"

        parts = [f"<h3>Resolution Actions ({len(self._actions)})</h3>"]
        parts.append(
            "<table><tr><th>#</th><th>Action</th><th>Score</th><th>Effort</th><th>Columns</th></tr>"
        )

        for i, action in enumerate(self._actions, 1):
            score = action.get("priority_score", 0)
            effort = action.get("effort", "?")
            cols = len(action.get("affected_columns", []))
            parts.append(
                f"<tr><td>{i}</td>"
                f"<td>{html_mod.escape(action.get('action', ''))}</td>"
                f"<td>{score:.3f}</td>"
                f"<td>{html_mod.escape(str(effort))}</td>"
                f"<td>{cols}</td></tr>"
            )

        parts.append("</table>")
        return "\n".join(parts)

    def __repr__(self) -> str:
        return f"ActionsResult(count={len(self._actions)})"


class RunResultWrapper:
    """Wrapper for pipeline run results with Jupyter rendering."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> Any:
        """Dict-like keys access."""
        return self._data.keys()

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like get."""
        return self._data.get(key, default)

    @property
    def success(self) -> bool:
        """Whether the pipeline succeeded."""
        return bool(self._data.get("success", False))

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        success = self._data.get("success", False)
        color = "#2ecc71" if success else "#e74c3c"
        status = "Success" if success else "Failed"

        completed = self._data.get("phases_completed", [])
        failed = self._data.get("phases_failed", [])

        parts = [
            f"<h3>Pipeline: <span style='color:{color}'>{status}</span></h3>",
            f"<b>Completed:</b> {len(completed)} phases<br>",
        ]

        if failed:
            escaped_failed = ", ".join(html_mod.escape(p) for p in failed)
            parts.append(f"<b style='color:#e74c3c'>Failed:</b> {escaped_failed}<br>")

        error = self._data.get("error")
        if error:
            parts.append(f"<b>Error:</b> {html_mod.escape(error)}<br>")

        return "\n".join(parts)

    def __repr__(self) -> str:
        status = "success" if self.success else "failed"
        completed = len(self._data.get("phases_completed", []))
        return f"RunResult({status}, {completed} phases completed)"


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

    def _repr_html_(self) -> str:
        """Jupyter HTML representation."""
        parts = [
            "<h3>Query Result</h3>",
            f"<b>Answer:</b> {html_mod.escape(self.answer)}<br>",
            f"<b>Confidence:</b> {html_mod.escape(self.confidence)}<br>",
        ]
        if self.sql:
            parts.append(f"<b>SQL:</b> <code>{html_mod.escape(self.sql)}</code><br>")
        if self.data:
            parts.append(f"<b>Rows:</b> {len(self.data)}<br>")
        return "\n".join(parts)

    def __repr__(self) -> str:
        """String representation."""
        return f"QueryResult(answer={self.answer!r}, confidence={self.confidence}, rows={len(self.data) if self.data else 0})"
