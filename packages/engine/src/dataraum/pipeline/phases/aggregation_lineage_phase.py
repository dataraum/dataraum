"""Aggregation-lineage phase (DAT-491) — events→measure rollup discovery.

Session phase, runs in the begin_session spine after ``semantic_per_table``
(needs the fact/grain entity classification + the relationship catalog). The
LLM proposes candidate rollups; the deterministic reconciliation statistic
disposes them; reconciled lineage persists run-versioned. The phase declares
the ``temporal_behavior`` detector in ``pipeline.yaml``, so the terminal
``session_detect`` re-adjudicates stock/flow with the data-grounded
``structural_reconciliation`` witness this phase just produced.

LLM-batched per session (one proposal call over the whole table set), so the
cost is one call per begin_session run, not per column.
"""

from __future__ import annotations

from types import ModuleType

from dataraum.analysis.lineage.agent import AggregationLineageAgent
from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)
from dataraum.core.logging import get_logger
from dataraum.lifecycle import BaseRunMap
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

logger = get_logger(__name__)


@analysis_phase
class AggregationLineagePhase(BasePhase):
    """Discover events→measure aggregation lineage (LLM proposes, data disposes)."""

    @property
    def name(self) -> str:
        return "aggregation_lineage"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.lineage import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip on genuine preconditions only (a re-run must re-derive, DAT-408)."""
        if not ctx.table_ids:
            return "No tables in session scope"
        if len(ctx.table_ids) < 2:
            return "Lineage needs a measure table and an event table (≥ 2 tables)"
        return None

    def _entities_text(self, ctx: PhaseContext, run_id: str | None) -> str:
        """The fact/grain classification block from THIS run's semantic_per_table."""
        from sqlalchemy import select

        from dataraum.analysis.semantic.db_models import TableEntity
        from dataraum.storage import Table

        rows = ctx.session.execute(
            select(Table.table_name, TableEntity)
            .join(TableEntity, TableEntity.table_id == Table.table_id)
            .where(TableEntity.table_id.in_(ctx.table_ids), TableEntity.run_id == run_id)
        ).all()
        if not rows:
            return "<entities>none classified</entities>"
        lines = ["<entities>"]
        for table_name, entity in rows:
            grain = (entity.grain_columns or {}).get("columns") if entity.grain_columns else None
            lines.append(
                f'  <table name="{table_name}" entity_type="{entity.detected_entity_type}" '
                f'is_fact_table="{entity.is_fact_table}" grain_columns="{grain or []}"/>'
            )
        lines.append("</entities>")
        return "\n".join(lines)

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        from dataraum.entropy.detectors.loaders import resolve_base_runs

        session_id = ctx.require_session_id()
        run_id = ctx.require_run_id()

        # Pin the schema resolver's upstream reads: per-column annotations live
        # under the promoted add_source heads (resolved once, same convention as
        # run_detectors), while the relationship catalog this run should reason
        # over is the one ``relationships``/``semantic_per_table`` JUST wrote —
        # THIS run's, not the previously promoted session head (promote flips
        # only at the end of the spine).
        pins = resolve_base_runs(ctx.session, ctx.table_ids)
        base_runs = BaseRunMap(
            relationship_run_id=run_id,
            semantic_runs={
                table_id: pinned
                for (table_id, stage), pinned in pins.items()
                if stage == "semantic_per_column"
            },
        )
        schema = get_multi_table_schema_for_llm(
            ctx.session, ctx.table_ids, ctx.duckdb_conn, base_runs=base_runs
        )
        schema_text = format_multi_table_schema_for_prompt(schema)
        entities_text = self._entities_text(ctx, run_id)

        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")
        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        agent = AggregationLineageAgent(
            config=config, provider=provider, prompt_renderer=PromptRenderer()
        )
        proposals = agent.propose(schema_text, entities_text)
        if not proposals.success:
            return PhaseResult.failed(proposals.error or "lineage proposal failed")
        candidates = proposals.value or []

        persisted = discover_aggregation_lineage(
            ctx.session,
            ctx.duckdb_conn,
            candidates=candidates,
            table_ids=ctx.table_ids,
            session_id=session_id,
            run_id=run_id,
        )
        return PhaseResult.success(
            outputs={"candidates": len(candidates), "reconciled": persisted},
            records_processed=len(candidates),
            records_created=persisted,
            summary=f"{persisted}/{len(candidates)} lineage candidates reconciled",
        )
