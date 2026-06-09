"""Temporal-behaviour (stock vs flow) entropy detector (ADR-0009, DAT-445/DAT-459).

Table-scoped, cross-table. For a period-keyed focal table (e.g. ``trial_balance``,
``balance_sheet``) it reconciles each numeric value column against the INDEPENDENT
per-(account, period) movements aggregated from the ledger line-item table
(``journal_lines`` + its entry dates), and pools that structural reading against the
column's DECLARED temporal behaviour (the ontology ``temporal_behavior`` of its
concept, via ``SemanticAnnotation``: ``point_in_time`` → stock, ``additive`` → flow).

The score is the pooled conflict ``C``: high when a column NAMED like a balance
(``debit_balance`` → ``account_balance`` → ``point_in_time``) is actually a per-period
flow in the data — the live ``debit_balance`` case — and quiet when a genuine
carried-forward balance (``balance_sheet.ending_balance``) agrees with its claim.

The reconciliation is what survives the DAT-459 falsification of a time-series
persistence signature: a trending flow still equals its movement and a
mean-reverting stock still carries forward, so both classify correctly. The witness
abstains when neither hypothesis reconciles (a wrong/missing movement anchor) — see
:mod:`dataraum.entropy.measurements.temporal_behavior`.

Finance-shaped lineage: the movement anchor needs an account key, debit/credit, and
an entry-date path. Discovery is by column convention (no FK metadata at detect
time); when the shape is absent the detector emits nothing rather than guessing.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.measurements.temporal_behavior import (
    CLAIM_SPACE,
    measure_temporal_behavior,
)
from dataraum.entropy.models import EntropyObject, WitnessClaim
from dataraum.server.storage import LAKE_CATALOG_ALIAS

logger = get_logger(__name__)

# Column-name conventions used to find the ledger shape (case-insensitive).
_ACCOUNT_NAMES = ("account_id", "account", "acct_id", "acct")
_PERIOD_NAMES = ("period", "fiscal_period", "month")
_DEBIT_NAMES = ("debit",)
_CREDIT_NAMES = ("credit",)
_DATE_NAMES = ("date", "entry_date", "posting_date", "transaction_date")
_NUMERIC_TYPE_HINTS = ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL", "BIGINT")

# Strength of a declared semantic claim (scaled by the annotation's confidence):
# point_in_time → stock, additive → flow. Default confidence when unannotated-but-set.
_DEFAULT_CLAIM_CONFIDENCE = 0.7


def _typed_fqn(table: Any) -> str:
    """Fully-qualified DuckDB name for a typed table."""
    name = table.duckdb_path or table.table_name
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(table.layer)}."{name}"'


def _find_col(columns: list[Any], names: tuple[str, ...]) -> Any | None:
    """First column whose name matches one of ``names`` (case-insensitive)."""
    lowered = {c.column_name.lower(): c for c in columns}
    for n in names:
        if n in lowered:
            return lowered[n]
    return None


def _is_numeric(col: Any) -> bool:
    rt = (col.resolved_type or "").upper()
    return any(h in rt for h in _NUMERIC_TYPE_HINTS)


def _semantic_claim(temporal_behavior: str | None, confidence: float | None) -> dict[str, float] | None:
    """Map an ontology ``temporal_behavior`` to a stock/flow claim distribution.

    ``point_in_time`` leans stock, ``additive`` leans flow, scaled by the
    annotation confidence; anything else (or unset) → no claim (abstain).
    """
    if not temporal_behavior:
        return None
    conf = confidence if confidence is not None else _DEFAULT_CLAIM_CONFIDENCE
    conf = min(1.0, max(0.0, float(conf)))
    if temporal_behavior == "point_in_time":
        return {"stock": 0.5 + 0.5 * conf}
    if temporal_behavior == "additive":
        return {"stock": 0.5 - 0.5 * conf}
    return None


def _resolve_period_path(
    line_table: Any, line_cols: list[Any], tables: list[Any], cols_by_table: dict[str, list[Any]]
) -> tuple[str | None, str, str | None]:
    """Resolve the ``YYYY-MM`` period expression + join + posted-status reference.

    Returns ``(date_expr, join_clause, status_ref)``: ``date_expr`` is a duckdb
    expression yielding the period string (or ``None`` if no date path exists),
    ``join_clause`` joins the entries table when the date lives there, and
    ``status_ref`` is the qualified status column for posted-only filtering (or
    ``None`` if there is no status column). Pure — no detector state (the detector
    is a shared singleton run concurrently across tables).
    """
    own_date = _find_col(line_cols, _DATE_NAMES)
    if own_date is not None:
        status_ref = 'li."status"' if _find_col(line_cols, ("status",)) else None
        return f"strftime(try_cast(li.\"{own_date.column_name}\" AS DATE), '%Y-%m')", "", status_ref

    # No own date — look for an entries table sharing an id column with a date.
    id_col = _find_col(line_cols, ("entry_id", "journal_entry_id", "je_id"))
    if id_col is None:
        return None, "", None
    for t in tables:
        if t.table_id == line_table.table_id:
            continue
        cols = cols_by_table[t.table_id]
        entry_id = _find_col(cols, (id_col.column_name,))
        entry_date = _find_col(cols, _DATE_NAMES)
        if entry_id is not None and entry_date is not None:
            fqn = _typed_fqn(t)
            join = f'JOIN {fqn} je ON li."{id_col.column_name}" = je."{entry_id.column_name}"'
            status_ref = 'je."status"' if _find_col(cols, ("status",)) else None
            return f"strftime(try_cast(je.\"{entry_date.column_name}\" AS DATE), '%Y-%m')", join, status_ref
    return None, "", None


class TemporalBehaviorDetector(EntropyDetector):
    """Reconcile period-keyed columns as stock vs flow and pool against the claim."""

    detector_id = "temporal_behavior"
    layer = Layer.COMPUTATIONAL
    dimension = Dimension.RECONCILIATION
    sub_dimension = SubDimension.TEMPORAL_BEHAVIOR
    scope = "table"
    # No required_analyses: reads DuckDB + metadata directly (base default []).
    description = "Stock vs flow: reconciles period-keyed columns against ledger movements"

    def load_data(self, context: DetectorContext) -> None:
        """Discover the ledger shape, build per-column series + anchors + claims."""
        if context.session is None or context.duckdb_conn is None or not context.table_id:
            return
        from dataraum.entropy.detectors.loaders import load_semantic
        from dataraum.entropy.reliabilities import get_reliability_config
        from dataraum.storage import Column, Table

        session = context.session
        focal = session.get(Table, context.table_id)
        if focal is None:
            return
        focal_cols = list(session.execute(
            select(Column).where(Column.table_id == context.table_id)
        ).scalars().all())
        account_col = _find_col(focal_cols, _ACCOUNT_NAMES)
        period_col = _find_col(focal_cols, _PERIOD_NAMES)
        if account_col is None or period_col is None:
            return  # not a period-keyed, account-scoped table → not applicable

        anchors = self._load_movement_anchors(context, focal.source_id, account_col.column_name)
        if not anchors:
            return  # no independent movement source → abstain (emit nothing)

        value_cols = [
            c
            for c in focal_cols
            if c.column_id not in {account_col.column_id, period_col.column_id}
            and _is_numeric(c)
        ]
        columns_payload: list[dict[str, Any]] = []
        for col in value_cols:
            series = self._load_column_series(
                context, focal, account_col.column_name, period_col.column_name, col.column_name, anchors
            )
            if not series:
                continue
            semantic = load_semantic(session, col.column_id, context.run_id, context.base_runs)
            claim = _semantic_claim(
                (semantic or {}).get("temporal_behavior"), (semantic or {}).get("confidence")
            )
            columns_payload.append(
                {
                    "column": col.column_name,
                    "series_by_account": series,
                    "semantic_claim": claim,
                }
            )

        if columns_payload:
            context.analysis_results["temporal_columns"] = columns_payload
            context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
                self.detector_id
            )

    def _load_movement_anchors(
        self, context: DetectorContext, source_id: str, account_name: str
    ) -> dict[tuple[str, str], dict[str, float]]:
        """Per-(account, period) ``gross_debit``/``gross_credit``/``net`` from the ledger.

        Finds a line-item table (account + debit + credit) and its entry-date path
        (own date column, or a sibling entries table joined on a shared id column),
        and aggregates posted movements by account and ``YYYY-MM`` period.
        """
        from dataraum.storage import Column, Table

        session = context.session
        assert session is not None
        tables = list(session.execute(
            select(Table).where(Table.source_id == source_id, Table.layer == "typed")
        ).scalars().all())
        cols_by_table = {
            t.table_id: list(session.execute(
                select(Column).where(Column.table_id == t.table_id)
            ).scalars().all())
            for t in tables
        }

        # Line-item table: has account + debit + credit.
        line_table: Any = None
        line_cols: list[Any] = []
        for t in tables:
            cols = cols_by_table[t.table_id]
            if (
                _find_col(cols, (account_name,))
                and _find_col(cols, _DEBIT_NAMES)
                and _find_col(cols, _CREDIT_NAMES)
            ):
                line_table, line_cols = t, cols
                break
        if line_table is None:
            return {}

        line_fqn = _typed_fqn(line_table)
        date_expr, join_clause, status_ref = _resolve_period_path(
            line_table, line_cols, tables, cols_by_table
        )
        if date_expr is None:
            return {}

        where = f"WHERE lower(CAST({status_ref} AS VARCHAR)) = 'posted'" if status_ref else ""

        sql = (
            f'SELECT li."{account_name}" AS acct, {date_expr} AS period, '
            f'SUM(CAST(li."debit" AS DOUBLE)) AS gross_debit, '
            f'SUM(CAST(li."credit" AS DOUBLE)) AS gross_credit '
            f"FROM {line_fqn} li {join_clause} {where} "
            f'GROUP BY li."{account_name}", {date_expr}'
        )
        try:
            rows = context.duckdb_conn.execute(sql).fetchall()
        except Exception as exc:  # noqa: BLE001 — abstain on any anchor build failure
            logger.debug("temporal_behavior anchor query failed: %s", exc)
            return {}

        anchors: dict[tuple[str, str], dict[str, float]] = {}
        for acct, period, gd, gc in rows:
            if acct is None or period is None:
                continue
            gross_debit = float(gd or 0.0)
            gross_credit = float(gc or 0.0)
            anchors[(str(acct), str(period))] = {
                "gross_debit": gross_debit,
                "gross_credit": gross_credit,
                "net": gross_debit - gross_credit,
            }
        return anchors

    def _load_column_series(
        self,
        context: DetectorContext,
        focal: Any,
        account_name: str,
        period_name: str,
        value_name: str,
        anchors: dict[tuple[str, str], dict[str, float]],
    ) -> dict[str, dict[str, Any]]:
        """Per-account ``{"values", "anchors"}`` for one column, period-aligned."""
        fqn = _typed_fqn(focal)
        sql = (
            f'SELECT "{account_name}" AS acct, "{period_name}" AS period, '
            f'CAST("{value_name}" AS DOUBLE) AS val '
            f'FROM {fqn} WHERE "{value_name}" IS NOT NULL '
            f'ORDER BY "{account_name}", "{period_name}"'
        )
        try:
            rows = context.duckdb_conn.execute(sql).fetchall()
        except Exception as exc:  # noqa: BLE001
            logger.debug("temporal_behavior series query failed for %s: %s", value_name, exc)
            return {}

        by_acct: dict[str, list[tuple[str, float]]] = {}
        for acct, period, val in rows:
            if acct is None or period is None or val is None:
                continue
            by_acct.setdefault(str(acct), []).append((str(period), float(val)))

        series: dict[str, dict[str, Any]] = {}
        for acct, pairs in by_acct.items():
            pairs.sort(key=lambda pv: pv[0])
            values = [v for _, v in pairs]
            gross_debit: list[float] = []
            gross_credit: list[float] = []
            net: list[float] = []
            for period, _ in pairs:
                a = anchors.get((acct, period), {"gross_debit": 0.0, "gross_credit": 0.0, "net": 0.0})
                gross_debit.append(a["gross_debit"])
                gross_credit.append(a["gross_credit"])
                net.append(a["net"])
            series[acct] = {"values": values, "anchors": [net, gross_debit, gross_credit]}
        return series

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Pool reconciliation vs claim per column; emit one object per column."""
        columns = context.get_analysis("temporal_columns", [])
        if not columns:
            return []
        reliabilities = context.get_analysis("reliabilities", None) or None

        objects: list[EntropyObject] = []
        for payload in columns:
            adj = measure_temporal_behavior(
                context.table_name,
                payload["column"],
                payload["series_by_account"],
                payload["semantic_claim"],
                reliabilities=reliabilities,
            )
            if not adj.witnesses:
                continue  # no witness took a position → nothing to say
            posterior = dict(zip(CLAIM_SPACE, adj.result.posterior, strict=True))
            evidence = [
                {
                    "_table_name": context.table_name,
                    "_column_name": payload["column"],
                    "claim_field": adj.claim_field,
                    "conflict": adj.result.conflict,
                    "ignorance": adj.result.ignorance,
                    "posterior": posterior,
                    "semantic_claim": payload["semantic_claim"],
                    "accounts": len(payload["series_by_account"]),
                }
            ]
            obj = EntropyObject(
                layer=self.layer,
                dimension=self.dimension,
                sub_dimension=self.sub_dimension,
                target=f"column:{context.table_name}.{payload['column']}",
                score=adj.result.conflict,
                evidence=evidence,
                detector_id=self.detector_id,
                witnesses=[
                    WitnessClaim(
                        claim_field=adj.claim_field,
                        witness_id=w.witness_id,
                        distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                        reliability=w.reliability,
                    )
                    for w in adj.witnesses
                ],
            )
            objects.append(obj)
        return objects
