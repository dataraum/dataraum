"""Type resolution engine - DuckDB SQL generation.

Generates SQL to create typed tables from raw VARCHAR tables
using TypeCandidates computed during type inference.

The quarantine pattern:
- Rows where ANY column fails TRY_CAST go to quarantine table
- This allows downstream processing on clean typed data
- Quarantined rows can be reviewed and fixed
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.analysis.typing.models import ColumnCastResult, TypeResolutionResult
from dataraum.analysis.typing.patterns import Pattern, load_pattern_config
from dataraum.analysis.typing.recipe import store_recipe
from dataraum.core.logging import get_logger
from dataraum.core.models.base import ColumnRef, DataType, Result
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)


def _resolve_pattern(
    detected_pattern: str,
    patterns_by_name: dict[str, Pattern],
) -> Pattern | None:
    """Resolve a detected pattern name to a Pattern object.

    Handles combined pattern names (e.g. "us_date+eu_slash_date+dd_mon_yy")
    produced by inference Strategy 1b. For combined names, builds a synthetic
    Pattern with a COALESCE standardization_expr that tries each format.

    Args:
        detected_pattern: Pattern name, possibly containing '+' for combined.
        patterns_by_name: Lookup of individual pattern objects.

    Returns:
        Pattern object, or None if not found.
    """
    # Simple case: single pattern name
    if "+" not in detected_pattern:
        return patterns_by_name.get(detected_pattern)

    # Combined pattern: split and look up each constituent
    names = detected_pattern.split("+")
    parts: list[Pattern] = []
    for name in names:
        p = patterns_by_name.get(name)
        if p is not None:
            parts.append(p)

    if not parts:
        return None

    # If only one resolved, just return it directly
    if len(parts) == 1:
        return parts[0]

    # Build COALESCE(TRY_STRPTIME("{col}", fmt1), TRY_STRPTIME("{col}", fmt2), ...)
    # Exprs are TRY_-normalized at Pattern construction, so each part returns
    # NULL on mismatch instead of erroring.
    coalesce_parts = []
    for p in parts:
        if p.standardization_expr:
            coalesce_parts.append(p.standardization_expr)

    if not coalesce_parts:
        return parts[0]  # No standardization exprs, fallback to first

    coalesce_expr = f"COALESCE({', '.join(coalesce_parts)})"

    # Build synthetic Pattern combining all constituents
    return Pattern(
        name=detected_pattern,
        pattern="",  # Not used for matching, only for SQL generation
        inferred_type=parts[0].inferred_type,
        standardization_expr=coalesce_expr,
    )


def reconcile_typed_table(
    session: Session,
    raw_table: Table,
    layer: str,
    duckdb_path: str,
    row_count: int,
) -> Table:
    """Find-or-create the ``layer`` Table for ``raw_table`` and refresh its stats.

    Stable typed identity (DAT-373 Option A): a re-type REUSES the existing
    ``typed`` / ``quarantine`` ``Table`` row (the typed sibling shares the raw's
    ``source_id`` and its now workspace-unique ``(table_name, layer)`` — DAT-639)
    rather than minting a fresh ``table_id``. Reusing the row keeps the typed
    Table id — and the Column ids reconciled under it — stable across teach
    replays, so other stages' per-Column rows stay attached. Only the
    ``row_count`` (and ``duckdb_path``, defensively) are refreshed in place.
    """
    existing = session.execute(
        select(Table).where(
            Table.source_id == raw_table.source_id,
            Table.table_name == raw_table.table_name,
            Table.layer == layer,
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.duckdb_path = duckdb_path
        existing.row_count = row_count
        return existing

    created = Table(
        table_id=str(uuid4()),
        source_id=raw_table.source_id,
        table_name=raw_table.table_name,
        layer=layer,
        duckdb_path=duckdb_path,
        row_count=row_count,
    )
    session.add(created)
    return created


def reconcile_typed_columns(
    session: Session,
    typed_table: Table,
    desired: Sequence[tuple[str, str | None, int, str | None, str]],
) -> dict[str, str]:
    """Reconcile ``typed_table``'s Columns to ``desired``, keeping ids stable.

    ``desired`` is the target column set as
    ``(column_name, original_name, column_position, raw_type, resolved_type)``
    tuples (``raw_type`` may be ``None`` for source-typed columns). Existing
    columns matched by ``column_name`` are UPDATED in place
    (id preserved); columns no longer desired are deleted; genuinely new
    columns are inserted. Returns ``column_name -> column_id`` for the full
    reconciled set so callers can attach typing's own derived rows.

    Keeping ids stable is what lets a re-type avoid orphaning another stage's
    per-Column rows (DAT-373 Option A).
    """
    # Local import: a module-scope import would pull the whole downstream
    # ``relationships`` package (its ``__init__`` eagerly loads the detector/
    # evaluator tree) onto every load of this upstream typing module. No cycle
    # today — just keeping that layering dependency lazy.
    from dataraum.analysis.relationships.surrogate import is_surrogate_column

    current = {c.column_name: c for c in typed_table.columns}
    desired_names = {d[0] for d in desired}

    # Delete columns no longer present in the re-typed table — but NEVER a
    # mint-owned surrogate (``_sk__*``). ``desired`` is built from the RAW
    # source's columns only, which by construction never include an engine-minted
    # surrogate (DAT-277); the surrogate mint owns their full lifecycle on the
    # typed table (``surrogate_mint_phase`` re-materializes the physical column
    # and reconciles the row, and is the ONLY writer that deletes a surrogate —
    # clearing its FK children first). Deleting one here on a re-type would
    # instead violate the FK from the surrogate relationship that still
    # references it, failing the typing phase and killing the whole re-run
    # cascade (DAT-766). Typing must not delete a column it did not create.
    for name, col in list(current.items()):
        if name in desired_names or is_surrogate_column(name):
            continue
        session.delete(col)

    column_map: dict[str, str] = {}
    for column_name, original_name, position, raw_type, resolved_type in desired:
        existing = current.get(column_name)
        if existing is not None:
            existing.original_name = original_name
            existing.column_position = position
            existing.raw_type = raw_type
            existing.resolved_type = resolved_type
            column_map[column_name] = existing.column_id
        else:
            new_id = str(uuid4())
            session.add(
                Column(
                    column_id=new_id,
                    table_id=typed_table.table_id,
                    column_name=column_name,
                    original_name=original_name,
                    column_position=position,
                    raw_type=raw_type,
                    resolved_type=resolved_type,
                )
            )
            column_map[column_name] = new_id

    return column_map


@dataclass
class ColumnTypeSpec:
    """Type specification for a column during resolution."""

    column_id: str
    column_name: str
    data_type: DataType
    pattern: Pattern | None = None
    decision_source: str = "automatic"  # 'automatic', 'manual', 'override', 'fallback'
    decision_reason: str | None = None
    candidate_confidence: float | None = None  # Confidence from best TypeCandidate
    decided_by: str | None = None  # Provenance carried from an honored manual decision


def _select_best_candidates(
    columns: list[Column],
    min_confidence: float,
    run_id: str,
) -> list[ColumnTypeSpec]:
    """Select best type candidate per column for THIS run.

    Priority:
    1. Manual TypeDecision (human override) — pins the TYPE; the best
       same-type candidate still supplies the standardization expr.
    2. Highest confidence TypeCandidate >= threshold (this run's candidates).
    3. Fallback to VARCHAR.

    Only ``decision_source == "manual"`` rows are honored as overrides:
    resolution persists its own ``automatic``/``fallback`` decisions every
    run, and honoring those froze the first run's outcome forever — a prior
    fallback-VARCHAR row blocked taught patterns from ever applying, and a
    prior automatic DATE row re-applied WITHOUT its standardization expr,
    plain-TRY_CASTing e.g. DD.MM.YYYY to an all-NULL column. Candidates are
    likewise scoped to ``run_id`` (DAT-413: runs coexist): a prior run's
    VARCHAR fallback at confidence 1.0 must not outcompete this run's real
    candidates.

    Returns ColumnTypeSpec with decision metadata for persisting TypeDecision records.
    """
    pattern_config = load_pattern_config()
    patterns_by_name = {p.name: p for p in pattern_config.get_patterns()}
    specs = []

    for col in sorted(columns, key=lambda c: c.column_position):
        candidates = sorted(
            (c for c in col.type_candidates if c.run_id == run_id),
            key=lambda c: c.confidence,
            reverse=True,
        )

        # Human override: the latest manual decision pins the TYPE. Keep the
        # best same-type candidate's pattern — honoring the type while
        # dropping its standardization expr is the destruction path.
        manual = max(
            (td for td in col.type_decisions if td.decision_source == "manual"),
            key=lambda td: td.decided_at,
            default=None,
        )
        if manual is not None:
            # The pattern search deliberately ignores min_confidence — a human
            # pinned the type, so even a weak same-type candidate's expr beats
            # a plain cast.
            pattern = None
            for cand in candidates:
                if cand.data_type == manual.decided_type and cand.detected_pattern:
                    pattern = _resolve_pattern(cand.detected_pattern, patterns_by_name)
                    if pattern is not None:
                        break
            if pattern is None:
                # No same-type candidate this run → the DDL plain-TRY_CASTs to
                # the decided type. For string-parsed types (dates) that can
                # NULL every value — surface it instead of failing silently.
                logger.warning(
                    "manual_override_no_matching_candidate",
                    column=col.column_name,
                    decided_type=manual.decided_type,
                    run_id=run_id,
                )
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType[manual.decided_type],
                    pattern=pattern,
                    decision_source="manual",
                    decision_reason=manual.decision_reason,
                    decided_by=manual.decided_by,
                )
            )
            continue

        # Find best candidate
        if candidates and candidates[0].confidence >= min_confidence:
            best = candidates[0]
            pattern = (
                _resolve_pattern(best.detected_pattern, patterns_by_name)
                if best.detected_pattern
                else None
            )
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType[best.data_type],
                    pattern=pattern,
                    decision_source="automatic",
                    decision_reason=f"Best candidate with confidence {best.confidence:.2f} (pattern: {best.detected_pattern or 'none'})",
                    candidate_confidence=best.confidence,
                )
            )
        else:
            # Fallback to VARCHAR
            best_conf = candidates[0].confidence if candidates else 0.0
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType.VARCHAR,
                    decision_source="fallback",
                    decision_reason=f"No candidate met confidence threshold {min_confidence} (best: {best_conf:.2f})",
                    candidate_confidence=best_conf if candidates else None,
                )
            )

    return specs


def _generate_typed_table_sql(
    raw_target: str,
    typed_target: str,
    specs: list[ColumnTypeSpec],
) -> str:
    """Generate CREATE TABLE with TRY_CAST per column.

    ``raw_target`` and ``typed_target`` are fully-qualified DuckDB names
    (e.g. ``lake.raw."csv__orders"`` / ``lake.typed."csv__orders"``) —
    callers compose the schema prefix; this helper does not wrap in quotes.
    """
    selects = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            # Apply standardization before cast
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            selects.append(f"TRY_CAST({expr} AS {target}) AS {col}")
        else:
            selects.append(f"TRY_CAST({col} AS {target}) AS {col}")

    return (
        f"CREATE OR REPLACE TABLE {typed_target} AS SELECT {', '.join(selects)} FROM {raw_target}"
    )


def _generate_quarantine_sql(
    raw_target: str,
    quarantine_target: str,
    specs: list[ColumnTypeSpec],
) -> str:
    """Generate quarantine table for rows where any cast fails.

    ``raw_target`` and ``quarantine_target`` are fully-qualified DuckDB names.
    """
    checks = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            checks.append(f"(TRY_CAST({expr} AS {target}) IS NULL AND {col} IS NOT NULL)")
        else:
            checks.append(f"(TRY_CAST({col} AS {target}) IS NULL AND {col} IS NOT NULL)")

    where = " OR ".join(checks) if checks else "FALSE"
    return (
        f"CREATE OR REPLACE TABLE {quarantine_target} AS "
        f"SELECT *, CURRENT_TIMESTAMP AS _quarantined_at FROM {raw_target} WHERE {where}"
    )


def resolve_types(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float,
    *,
    run_id: str,
) -> Result[TypeResolutionResult]:
    """Resolve types for a raw table using DuckDB SQL.

    1. Load TypeCandidates (from inference)
    2. Select best candidate per column
    3. Generate and execute typed table SQL
    4. Generate and execute quarantine table SQL
    5. Return stats

    Args:
        table_id: ID of the raw table to resolve
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_confidence: Minimum confidence threshold for automatic type selection

    Returns:
        Result containing TypeResolutionResult with table names and row counts
    """
    # Load table with columns and type candidates
    stmt = (
        select(Table)
        .where(Table.table_id == table_id)
        .options(
            selectinload(Table.columns).selectinload(Column.type_candidates),
            selectinload(Table.columns).selectinload(Column.type_decisions),
        )
    )
    result = session.execute(stmt)
    table = result.scalar_one_or_none()

    if not table:
        return Result.fail(f"Table not found: {table_id}")
    if table.layer != "raw":
        return Result.fail(f"Table is not a raw table: {table.layer}")
    if not table.duckdb_path:
        return Result.fail(f"Table has no DuckDB path: {table_id}")

    # Post-DAT-341: Table.duckdb_path stores the bare ``<source>__<table>``
    # name; raw/typed/quarantine all share the same bare value, the schema
    # discriminates. Compose FQN targets for cross-layer writes.
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.server.storage import LAKE_CATALOG_ALIAS

    bare = table.duckdb_path
    raw_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{bare}"'
    typed_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("typed")}."{bare}"'
    quarantine_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("quarantine")}."{bare}"'

    # Select best candidates
    specs = _select_best_candidates(table.columns, min_confidence, run_id)

    # Persist THIS run's TypeDecision for every column — upsert on
    # ``(column_id, run_id)`` (idempotent under a Temporal at-least-once
    # retry; prior runs' rows coexist untouched, DAT-413). Each run records
    # what it actually executed, including ``manual`` when it honored a
    # human override.
    raw_col_by_id = {col.column_id: col for col in table.columns}
    decided_at = datetime.now(UTC)
    raw_decision_rows: list[dict[str, Any]] = [
        {
            "column_id": spec.column_id,
            "run_id": run_id,
            "decided_type": spec.data_type.value,
            "decision_source": spec.decision_source,
            "decided_at": decided_at,
            "decided_by": spec.decided_by,
            "decision_reason": spec.decision_reason,
        }
        for spec in specs
    ]
    upsert(session, TypeDecision, raw_decision_rows, index_elements=["column_id", "run_id"])

    # Emit → store → execute (DAT-414): build the materialization DDL strings
    # first, persist them as versioned recipes stamped with ``run_id`` (so a
    # reset-to-prior-run can replay them without re-typing), then execute. The
    # executed SQL is byte-identical to before — only the persist step is new.
    typed_sql = _generate_typed_table_sql(raw_target, typed_target, specs)
    quarantine_sql = _generate_quarantine_sql(raw_target, quarantine_target, specs)
    try:
        duckdb_conn.execute(typed_sql)
        duckdb_conn.execute(quarantine_sql)
    except Exception as e:
        logger.error("type_resolution_sql_error", table=table.table_name, error=str(e))
        return Result.fail(f"SQL execution failed: {e}")

    # Get row counts
    total_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {raw_target}").fetchone()
    total_rows = total_result[0] if total_result else 0
    typed_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {typed_target}").fetchone()
    typed_rows = typed_result[0] if typed_result else 0
    quarantine_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {quarantine_target}").fetchone()
    quarantine_rows = quarantine_result[0] if quarantine_result else 0

    # Reconcile the typed + quarantine metadata records (DAT-373 Option A):
    # reuse the existing rows for this (source, table_name, layer) when a teach
    # re-types, keeping ``table_id`` + Column ids stable so other stages'
    # per-Column rows stay attached. All three layer records share the same bare
    # ``duckdb_path``; ``layer`` discriminates which schema they live in. The
    # session must be flushed before reconcile so the typed Table's existing
    # Columns are loadable (the reconcile reads ``typed_table.columns``).
    session.flush()
    typed_table_record = reconcile_typed_table(session, table, "typed", bare, typed_rows)
    quarantine_table_record = reconcile_typed_table(
        session, table, "quarantine", bare, quarantine_rows
    )
    session.flush()

    # Persist the versioned materialization recipes (DAT-414): one per produced
    # layer, keyed on the *typed* Table id (stable across re-types) and stamped
    # with this run. Both DDLs read the raw layer, so ``depends_on`` names the raw
    # FQN (layer-qualified, so the dependency-order rebuild never confuses it with
    # the same-bare-named typed/quarantine artifacts).
    store_recipe(
        session,
        table_id=typed_table_record.table_id,
        layer="typed",
        run_id=run_id,
        target_fqn=typed_target,
        ddl=typed_sql,
        depends_on=[raw_target],
    )
    store_recipe(
        session,
        table_id=quarantine_table_record.table_id,
        layer="quarantine",
        run_id=run_id,
        target_fqn=quarantine_target,
        ddl=quarantine_sql,
        depends_on=[raw_target],
    )

    # Reconcile typed columns — UPDATE in place / insert new / delete dropped,
    # so existing typed Column ids survive a re-type.
    typed_desired = [
        (
            spec.column_name,
            raw_col_by_id[spec.column_id].original_name,
            i,
            "VARCHAR",
            spec.data_type.value,
        )
        for i, spec in enumerate(specs)
    ]
    typed_column_map = reconcile_typed_columns(session, typed_table_record, typed_desired)

    # Reconcile quarantine columns (all columns kept VARCHAR + the _quarantined_at meta col).
    quarantine_desired = [
        (
            spec.column_name,
            raw_col_by_id[spec.column_id].original_name,
            i,
            "VARCHAR",
            "VARCHAR",  # Quarantine keeps original VARCHAR
        )
        for i, spec in enumerate(specs)
    ]
    quarantine_desired.append(("_quarantined_at", None, len(specs), "TIMESTAMP", "TIMESTAMP"))
    reconcile_typed_columns(session, quarantine_table_record, quarantine_desired)
    session.flush()

    # Compute per-column quarantine metrics and update raw TypeCandidates
    # BEFORE copying to typed columns, so copies include the fields.
    column_results = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            cast_expr = f"TRY_CAST({expr} AS {target})"
        else:
            cast_expr = f"TRY_CAST({col} AS {target})"

        success_result = duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {raw_target} WHERE {cast_expr} IS NOT NULL OR {col} IS NULL"
        ).fetchone()
        success = success_result[0] if success_result else 0
        failures = total_rows - success
        q_rate = failures / total_rows if total_rows > 0 else 0.0

        column_results.append(
            ColumnCastResult(
                column_id=spec.column_id,
                column_ref=ColumnRef(table_name=table.table_name, column_name=spec.column_name),
                source_type="VARCHAR",
                target_type=spec.data_type,
                success_count=success,
                failure_count=failures,
                success_rate=success / total_rows if total_rows > 0 else 1.0,
            )
        )

        # Set quarantine metrics on THIS run's raw TypeCandidate records
        # (prior runs' candidates coexist and keep their own metrics).
        raw_col = raw_col_by_id[spec.column_id]
        for tc in raw_col.type_candidates:
            if tc.run_id == run_id:
                tc.quarantine_count = failures
                tc.quarantine_rate = q_rate

    # Copy TypeDecision and TypeCandidate from raw columns to typed columns.
    # Raw columns keep originals (audit trail); typed columns get copies so
    # downstream consumers can query by typed column_id directly.
    # Note: quarantine_count/rate are already set on raw TypeCandidates above.
    #
    # Both copies are form-(a) upserts (DAT-502): TypeDecision on
    # ``(column_id, run_id)``; TypeCandidate — many-per-column — on its widened
    # identity ``(column_id, data_type, detected_pattern, run_id)``. Idempotent
    # under a Temporal success-redelivery (same run_id, no run-scoped clear),
    # while a new run's copies coexist with prior runs'. The promoted head
    # names which run is current.
    spec_by_column_id = {spec.column_id: spec for spec in specs}
    type_decision_rows: list[dict[str, Any]] = []
    type_candidate_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for raw_col in table.columns:
        target_col_id = typed_column_map.get(raw_col.column_name)
        if target_col_id is None:
            continue

        # The typed copy mirrors THIS run's executed decision (the spec) —
        # never a prior run's row off the multi-run relationship.
        col_spec = spec_by_column_id.get(raw_col.column_id)
        if col_spec is not None:
            # PK omitted so the model's Python-side default applies.
            type_decision_rows.append(
                {
                    "column_id": target_col_id,
                    "run_id": run_id,
                    "decided_type": col_spec.data_type.value,
                    "decision_source": col_spec.decision_source,
                    "decided_at": decided_at,
                    "decided_by": col_spec.decided_by,
                    "decision_reason": col_spec.decision_reason,
                }
            )

        for tc in raw_col.type_candidates:
            if tc.run_id != run_id:
                continue  # copy only THIS run's candidates (DAT-413 coexistence)
            pattern = tc.detected_pattern or ""
            key = (target_col_id, tc.data_type, pattern, run_id)
            type_candidate_rows[key] = {
                "column_id": target_col_id,
                "run_id": run_id,
                "detected_at": tc.detected_at,
                "data_type": tc.data_type,
                "confidence": tc.confidence,
                "parse_success_rate": tc.parse_success_rate,
                "failed_examples": tc.failed_examples,
                "detected_pattern": pattern,
                "pattern_match_rate": tc.pattern_match_rate,
                "detected_unit": tc.detected_unit,
                "unit_confidence": tc.unit_confidence,
                "quarantine_count": tc.quarantine_count,
                "quarantine_rate": tc.quarantine_rate,
            }

    upsert(session, TypeDecision, type_decision_rows, index_elements=["column_id", "run_id"])
    upsert(
        session,
        TypeCandidate,
        list(type_candidate_rows.values()),
        index_elements=["column_id", "data_type", "detected_pattern", "run_id"],
    )

    logger.debug(
        "type_resolution_completed",
        table=table.table_name,
        total_rows=total_rows,
        typed_rows=typed_rows,
        quarantined_rows=quarantine_rows,
        columns=len(specs),
    )

    return Result.ok(
        TypeResolutionResult(
            typed_table_id=typed_table_record.table_id,
            typed_table_name=bare,
            quarantine_table_name=bare,
            total_rows=total_rows,
            typed_rows=typed_rows,
            quarantined_rows=quarantine_rows,
            column_results=column_results,
        )
    )
