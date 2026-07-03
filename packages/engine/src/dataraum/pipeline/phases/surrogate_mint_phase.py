"""Surrogate-mint phase — cure composite keys at the source (DAT-277).

Runs in the begin_session spine AFTER ``session_materialize_overlays`` (so the
user's durable teaches are folded in) and BEFORE ``enriched_views`` (so the
minted surrogate relationship is an ordinary single-column FK by the time any
consumer reads the catalog). For each of this run's surrogate-key intents
(LLM-confirmed composites, written by ``semantic_per_table``):

1. Both typed tables are re-materialized with the deterministic NULL-propagating
   hash column (``analysis/relationships/surrogate.py``) by WRAPPING the typing
   run's recipe DDL on the DAT-414 substrate — emit → store → execute, stamped
   with this run. Steady state (physical surrogate set already matches) executes
   nothing.
2. The surrogate ``Column`` rows are reconciled by ``(table_id, column_name)``
   — the name is deterministic in the component set, so ``column_id`` is stable
   across runs and teach/keeper overlays keyed on it survive.
3. ONE ordinary single-column relationship is persisted on the surrogate pair
   (``detection_method='llm'``; evidence carries the natural→surrogate
   provenance). No consumer ever sees the composite's half-key anchor.

Reconcile owns every ``_sk__*`` column on the session's typed tables: a
surrogate that is neither re-confirmed this run nor still referenced by the
kept/promoted catalog (the keeper grace window, DAT-409) is dropped — physical
and metadata. Worst case at every step is a missed mint (abstain + warning),
never a broken catalog: with no intents and no stale surrogates this phase is
a fast no-op and the working single-column pipeline is untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from dataraum.analysis.relationships.db_models import Relationship, SurrogateKeyIntent
from dataraum.analysis.relationships.evaluator import (
    compute_actual_cardinality,
    compute_introduces_duplicates,
    compute_ri_metrics,
)
from dataraum.analysis.relationships.surrogate import (
    SurrogateSpec,
    amend_typed_ddl,
    is_surrogate_column,
    surrogate_column_name,
)
from dataraum.analysis.relationships.utils import (
    load_suppressed_relationship_pairs,
    load_surrogate_key_intents,
)
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.statistics.profiler import _profile_column_stats_parallel
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import store_recipe
from dataraum.core.duckdb_naming import qualified_table
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases._column_cleanup import delete_column_dependents
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)


def _lake_fqn(table: Table) -> str:
    return f"{LAKE_CATALOG_ALIAS}.{qualified_table('typed', table.table_name)}"


@analysis_phase
class SurrogateMintPhase(BasePhase):
    """Mint surrogate-key columns for this run's confirmed composites."""

    @property
    def name(self) -> str:
        return "surrogate_mint"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.relationships import db_models

        return [db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        run_id = ctx.require_run_id()
        tables = self._typed_tables(ctx)
        if not tables:
            return PhaseResult.success(summary="no typed tables in session scope")
        tables_by_id = {t.table_id: t for t in tables}
        columns = self._columns_by_table(ctx, list(tables_by_id))

        intents = load_surrogate_key_intents(ctx.session, run_id)
        warnings: list[str] = []
        base_columns: dict[str, set[str] | None] = {}
        desired: dict[str, dict[str, SurrogateSpec]] = {}  # table_id → name → spec
        mint_jobs: list[tuple[SurrogateKeyIntent, SurrogateSpec, SurrogateSpec]] = []
        for intent in intents:
            job = self._intent_specs(intent, tables_by_id, columns, warnings)
            if job is None:
                continue
            # The pair aborts as a UNIT when either side's components are missing
            # from the physical table — a half-minted surrogate (one side only)
            # would be an orphan column no relationship can ever use.
            if not all(
                self._components_present(ctx, tables_by_id[s.table_id], s, base_columns, warnings)
                for s in job
            ):
                continue
            from_spec, to_spec = job
            desired.setdefault(from_spec.table_id, {})[from_spec.column_name] = from_spec
            desired.setdefault(to_spec.table_id, {})[to_spec.column_name] = to_spec
            mint_jobs.append((intent, from_spec, to_spec))

        # Keeper grace window (DAT-409): a surrogate the promoted / kept catalog
        # still references survives even when this run's LLM didn't re-confirm it
        # — dropping it would strand the keeper lift-up mid-flight.
        for spec in self._kept_surrogate_specs(ctx, tables_by_id, columns, warnings):
            desired.setdefault(spec.table_id, {}).setdefault(spec.column_name, spec)

        minted = 0
        for table in tables:
            specs = sorted(desired.get(table.table_id, {}).values(), key=lambda s: s.column_name)
            changed = self._reconcile_table(ctx, table, specs, columns, warnings)
            minted += changed

        for intent, from_spec, to_spec in mint_jobs:
            self._persist_surrogate_relationship(
                ctx, intent, tables_by_id, from_spec, to_spec, warnings
            )

        summary = (
            f"{len(mint_jobs)} composite(s) minted as surrogate keys, "
            f"{minted} table(s) re-materialized"
        )
        logger.info("surrogate_mint_done", intents=len(intents), jobs=len(mint_jobs))
        return PhaseResult.success(
            records_processed=len(intents),
            records_created=len(mint_jobs),
            warnings=warnings,
            summary=summary,
        )

    def _components_present(
        self,
        ctx: PhaseContext,
        table: Table,
        spec: SurrogateSpec,
        base_columns: dict[str, set[str] | None],
        warnings: list[str],
    ) -> bool:
        """Whether every component physically exists on the typed table (cached DESCRIBE)."""
        if table.table_id not in base_columns:
            try:
                rows = ctx.duckdb_conn.execute(f"DESCRIBE {_lake_fqn(table)}").fetchall()
                base_columns[table.table_id] = {r[0] for r in rows if not is_surrogate_column(r[0])}
            except Exception as e:
                warnings.append(f"{table.table_name}: typed table unreadable ({e})")
                base_columns[table.table_id] = None
        base = base_columns[table.table_id]
        if base is None:
            return False
        missing = [c for c in spec.component_names if c not in base]
        if missing:
            warnings.append(f"{table.table_name}.{spec.column_name}: missing {missing}")
            return False
        return True

    def _columns_by_table(self, ctx: PhaseContext, table_ids: list[str]) -> dict[str, list[Column]]:
        rows = ctx.session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars()
        by_table: dict[str, list[Column]] = {}
        for col in rows:
            by_table.setdefault(col.table_id, []).append(col)
        return by_table

    def _intent_specs(
        self,
        intent: SurrogateKeyIntent,
        tables_by_id: dict[str, Table],
        columns: dict[str, list[Column]],
        warnings: list[str],
    ) -> tuple[SurrogateSpec, SurrogateSpec] | None:
        """Resolve one intent to its (from, to) surrogate specs, or abstain."""
        if intent.from_table_id not in tables_by_id or intent.to_table_id not in tables_by_id:
            warnings.append(f"surrogate intent {intent.intent_digest[:8]}: table out of scope")
            return None
        names_by_id = {
            c.column_id: c.column_name
            for tid in (intent.from_table_id, intent.to_table_id)
            for c in columns.get(tid, [])
        }
        from_names: list[str] = []
        to_names: list[str] = []
        for from_id, to_id in intent.column_pairs:
            from_name, to_name = names_by_id.get(from_id), names_by_id.get(to_id)
            if from_name is None or to_name is None:
                warnings.append(
                    f"surrogate intent {intent.intent_digest[:8]}: component column vanished"
                )
                return None
            from_names.append(from_name)
            to_names.append(to_name)
        return (
            SurrogateSpec(
                table_id=intent.from_table_id,
                column_name=surrogate_column_name(from_names),
                component_names=tuple(from_names),
            ),
            SurrogateSpec(
                table_id=intent.to_table_id,
                column_name=surrogate_column_name(to_names),
                component_names=tuple(to_names),
            ),
        )

    def _kept_surrogate_specs(
        self,
        ctx: PhaseContext,
        tables_by_id: dict[str, Table],
        columns: dict[str, list[Column]],
        warnings: list[str],
    ) -> list[SurrogateSpec]:
        """Surrogate specs the kept/promoted catalog still references (DAT-409 grace).

        Sources: this run's already-materialized ``manual``/``keeper`` rows, plus
        the PROMOTED head run's defined rows (the not-yet-keeper grace window —
        ``session_write_keepers`` lifts them at the END of this run and must find
        their columns intact), minus user-rejected pairs. Provenance (the
        component names to re-mint from) comes from the row's own evidence or,
        for overlay-materialized rows (which don't copy evidence), the latest
        prior mint's row on the same pair.
        """
        run_id = ctx.require_run_id()
        table_ids = list(tables_by_id)
        surrogate_ids = {
            c.column_id
            for cols in columns.values()
            for c in cols
            if is_surrogate_column(c.column_name)
        }
        if not surrogate_ids:
            return []
        col_by_id = {c.column_id: c for cols in columns.values() for c in cols}
        suppressed = load_suppressed_relationship_pairs(ctx.session)

        run_ids = [run_id]
        promoted = head_run_id(ctx.session, "catalog", "catalog")
        if promoted and promoted != run_id:
            run_ids.append(promoted)
        rows = list(
            ctx.session.execute(
                select(Relationship).where(
                    Relationship.run_id.in_(run_ids),
                    Relationship.detection_method != "candidate",
                    Relationship.from_table_id.in_(table_ids),
                    Relationship.to_table_id.in_(table_ids),
                    Relationship.from_column_id.in_(surrogate_ids),
                )
            ).scalars()
        )

        specs: list[SurrogateSpec] = []
        for row in rows:
            if (row.from_column_id, row.to_column_id) in suppressed:
                continue
            provenance = self._surrogate_provenance(ctx, row)
            if provenance is None:
                warnings.append(
                    f"kept surrogate pair {row.from_column_id[:8]}→{row.to_column_id[:8]}: "
                    "no mint provenance recoverable — cannot re-mint if dropped"
                )
                continue
            natural_pairs: list[list[str]] = provenance["natural_pairs"]
            for table_id, col_id, names in (
                (row.from_table_id, row.from_column_id, [p[0] for p in natural_pairs]),
                (row.to_table_id, row.to_column_id, [p[1] for p in natural_pairs]),
            ):
                col = col_by_id.get(col_id)
                if col is None:
                    continue
                specs.append(
                    SurrogateSpec(
                        table_id=table_id,
                        column_name=col.column_name,
                        component_names=tuple(names),
                    )
                )
        return specs

    def _surrogate_provenance(self, ctx: PhaseContext, row: Relationship) -> dict[str, Any] | None:
        """The mint provenance for a surrogate relationship row.

        Overlay-materialized rows (manual/keeper) carry only the overlay stamp as
        evidence, so fall back to the newest prior row on the same column pair
        that DOES carry the ``surrogate`` provenance (the original mint).
        """
        evidence = row.evidence or {}
        own: Any = evidence.get("surrogate")
        if isinstance(own, dict):
            return own
        prior = ctx.session.execute(
            select(Relationship)
            .where(
                Relationship.from_column_id == row.from_column_id,
                Relationship.to_column_id == row.to_column_id,
                Relationship.detection_method == "llm",
            )
            .order_by(Relationship.detected_at.desc())
        ).scalars()
        for candidate in prior:
            surrogate = (candidate.evidence or {}).get("surrogate")
            if isinstance(surrogate, dict):
                return surrogate
        return None

    def _reconcile_table(
        self,
        ctx: PhaseContext,
        table: Table,
        specs: list[SurrogateSpec],
        columns: dict[str, list[Column]],
        warnings: list[str],
    ) -> int:
        """Bring one typed table's physical + metadata ``_sk__*`` set to ``specs``.

        Returns 1 when the physical table was re-materialized, else 0.
        """
        fqn = _lake_fqn(table)
        try:
            described = ctx.duckdb_conn.execute(f"DESCRIBE {fqn}").fetchall()
        except Exception as e:
            if specs:
                warnings.append(f"{table.table_name}: typed table unreadable, mint skipped ({e})")
            return 0
        actual_names = [r[0] for r in described]
        actual_sk = {n for n in actual_names if is_surrogate_column(n)}
        base_names = {n for n in actual_names if not is_surrogate_column(n)}

        # Abstain per-spec on vanished components (schema changed under the key).
        buildable: list[SurrogateSpec] = []
        for spec in specs:
            missing = [c for c in spec.component_names if c not in base_names]
            if missing:
                warnings.append(f"{table.table_name}.{spec.column_name}: missing {missing}")
                continue
            buildable.append(spec)

        changed = 0
        if actual_sk != {s.column_name for s in buildable}:
            changed = self._rematerialize(ctx, table, fqn, buildable, warnings)
            if changed == 0 and buildable:
                return 0  # mint failed — leave metadata untouched (abstain)

        self._reconcile_column_rows(ctx, table, buildable, columns)
        return changed

    def _rematerialize(
        self,
        ctx: PhaseContext,
        table: Table,
        fqn: str,
        specs: list[SurrogateSpec],
        warnings: list[str],
    ) -> int:
        """Emit → store → execute the amended typing DDL (DAT-414 substrate)."""
        # The table's add_source run is sealed under ONE generation head
        # (DAT-506) — that run's typed recipe is the base DDL the mint wraps.
        typing_run = head_run_id(ctx.session, f"table:{table.table_id}", GENERATION_STAGE)
        base = None
        if typing_run:
            base = ctx.session.execute(
                select(MaterializationRecipe).where(
                    MaterializationRecipe.table_id == table.table_id,
                    MaterializationRecipe.layer == "typed",
                    MaterializationRecipe.run_id == typing_run,
                )
            ).scalar_one_or_none()
        if base is None:
            warnings.append(f"{table.table_name}: no typing recipe at head — mint skipped")
            return 0
        try:
            amended = amend_typed_ddl(base.ddl, specs)
            ctx.duckdb_conn.execute(amended)
        except Exception as e:
            warnings.append(f"{table.table_name}: surrogate re-materialization failed ({e})")
            logger.warning("surrogate_mint_failed", table=table.table_name, error=str(e))
            return 0
        store_recipe(
            ctx.session,
            table_id=table.table_id,
            layer="typed",
            run_id=ctx.require_run_id(),
            target_fqn=fqn,
            ddl=amended,
            depends_on=base.depends_on,
        )
        logger.info("surrogate_minted", table=table.table_name, columns=len(specs))
        return 1

    def _reconcile_column_rows(
        self,
        ctx: PhaseContext,
        table: Table,
        specs: list[SurrogateSpec],
        columns: dict[str, list[Column]],
    ) -> None:
        """Reconcile ``Column`` rows: keep-by-name (stable column_id), drop stale, profile new.

        Mirrors the enriched-view dim-column reconcile (DAT-516): a surviving name
        keeps its ``column_id`` and its profile; only genuinely-new surrogates are
        minted + profiled; dropped ones clear their FK children first (DAT-506).
        """
        existing = {c.column_name: c for c in columns.get(table.table_id, [])}
        existing_sk = {n: c for n, c in existing.items() if is_surrogate_column(n)}
        wanted = {s.column_name for s in specs}

        removed = [c for name, c in existing_sk.items() if name not in wanted]
        if removed:
            delete_column_dependents(ctx, [c.column_id for c in removed])
            ctx.session.execute(
                delete(Column).where(Column.column_id.in_([c.column_id for c in removed]))
            )
            ctx.session.flush()

        next_pos = max((c.column_position for c in existing.values()), default=-1) + 1
        new_columns: list[Column] = []
        for spec in specs:
            if spec.column_name in existing_sk:
                continue
            col = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=spec.column_name,
                column_position=next_pos,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
            next_pos += 1
            ctx.session.add(col)
            new_columns.append(col)
            columns.setdefault(table.table_id, []).append(col)
        ctx.session.flush()

        profiled_at = datetime.now(UTC)
        for col in new_columns:
            profile = _profile_column_stats_parallel(
                duckdb_conn=ctx.duckdb_conn,
                table_name=table.table_name,
                table_duckdb_path=table.duckdb_path or table.table_name,
                column_id=col.column_id,
                column_name=col.column_name,
                resolved_type="VARCHAR",
                profiled_at=profiled_at,
                top_k=10,
            )
            if profile:
                non_null = profile.total_count - profile.null_count
                ctx.session.add(
                    StatisticalProfile(
                        profile_id=str(uuid4()),
                        column_id=col.column_id,
                        run_id=ctx.require_run_id(),
                        profiled_at=profiled_at,
                        layer="typed",
                        total_count=profile.total_count,
                        null_count=profile.null_count,
                        distinct_count=profile.distinct_count,
                        null_ratio=profile.null_ratio,
                        cardinality_ratio=profile.cardinality_ratio,
                        is_unique=(profile.distinct_count == non_null) if non_null > 0 else False,
                        is_numeric=False,
                        profile_data=profile.model_dump(mode="json"),
                    )
                )

    def _persist_surrogate_relationship(
        self,
        ctx: PhaseContext,
        intent: SurrogateKeyIntent,
        tables_by_id: dict[str, Table],
        from_spec: SurrogateSpec,
        to_spec: SurrogateSpec,
        warnings: list[str],
    ) -> None:
        """Persist THE single-column relationship on the minted surrogate pair."""
        session = ctx.session
        from_table, to_table = tables_by_id[intent.from_table_id], tables_by_id[intent.to_table_id]
        pair: list[Column | None] = [
            session.execute(
                select(Column).where(
                    Column.table_id == spec.table_id, Column.column_name == spec.column_name
                )
            ).scalar_one_or_none()
            for spec in (from_spec, to_spec)
        ]
        from_col, to_col = pair
        if from_col is None or to_col is None:
            warnings.append(
                f"surrogate intent {intent.intent_digest[:8]}: mint incomplete, "
                "no relationship persisted"
            )
            return

        from_fqn, to_fqn = _lake_fqn(from_table), _lake_fqn(to_table)
        cardinality = compute_actual_cardinality(
            from_fqn, to_fqn, from_spec.column_name, to_spec.column_name, ctx.duckdb_conn
        )
        evidence: dict[str, Any] = {
            "source": "surrogate_mint",
            "intent_digest": intent.intent_digest,
            "reasoning": intent.reasoning,
            "composite_cardinality": intent.cardinality,
            "surrogate": {
                "natural_pairs": self._natural_name_pairs(ctx, intent),
                "natural_column_ids": intent.column_pairs,
            },
        }
        try:
            evidence["introduces_duplicates"] = compute_introduces_duplicates(
                from_fqn, to_fqn, from_spec.column_name, to_spec.column_name, ctx.duckdb_conn
            )
            for key, value in compute_ri_metrics(
                from_table=from_fqn,
                from_column=from_spec.column_name,
                to_table=to_fqn,
                to_column=to_spec.column_name,
                duckdb_conn=ctx.duckdb_conn,
            ).items():
                if value is not None:
                    evidence[key] = value
        except Exception as e:  # metrics are enrichment, never a mint blocker
            logger.warning("surrogate_metrics_failed", intent=intent.intent_digest, error=str(e))

        upsert(
            session,
            Relationship,
            [
                {
                    "run_id": ctx.require_run_id(),
                    "from_table_id": intent.from_table_id,
                    "from_column_id": from_col.column_id,
                    "to_table_id": intent.to_table_id,
                    "to_column_id": to_col.column_id,
                    "relationship_type": "foreign_key",
                    "cardinality": cardinality,
                    "confidence": intent.confidence,
                    "detection_method": "llm",
                    "evidence": evidence,
                }
            ],
            index_elements=["run_id", "from_column_id", "to_column_id", "detection_method"],
        )

    def _natural_name_pairs(self, ctx: PhaseContext, intent: SurrogateKeyIntent) -> list[list[str]]:
        """The intent's component pairs as ``[from_name, to_name]`` (mint provenance)."""
        ids = {cid for pair in intent.column_pairs for cid in pair}
        names = {
            c.column_id: c.column_name
            for c in ctx.session.execute(select(Column).where(Column.column_id.in_(ids))).scalars()
        }
        return [[names.get(f, f), names.get(t, t)] for f, t in intent.column_pairs]
