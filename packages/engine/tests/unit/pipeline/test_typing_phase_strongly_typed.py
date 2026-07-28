"""``_promote_strongly_typed``'s physical cross-check (DAT-748).

A strongly-typed source (Parquet, or a DB-recipe backend's DuckDB scanner)
skips pattern-matching inference and trusts ``Column.raw_type`` verbatim — but
that metadata is captured once, at import time, and typing can run much later
(a teach re-run retypes without necessarily re-importing). "Workspaces with
catalog history" is exactly where a stored ``raw_type`` can drift from what the
raw table physically is NOW. These tests pin the fix and the owner ruling on
its design:

- a matching declared type is trusted and promoted verbatim;
- a DISAGREEING but PRESENT live type is ADOPTED (the live DESCRIBE reads the
  exact relation the CTAS then selects from — it is ground truth at that
  instant; the catalog is the only suspect party) — never discarded to VARCHAR;
- a column ABSENT from the live schema entirely has nothing to adopt and is
  downgraded to VARCHAR — which fails loud anyway, since the projection
  references a column that no longer physically exists;
- "the DESCRIBE itself failed" (couldn't check) is NEVER conflated with
  "checked, found a mismatch" — it fails only that one table, loudly, with an
  honest cause;
- the projection is an UNCONDITIONAL explicit per-column SELECT, so detecting
  (or not) a mismatch on one column never changes whether an untracked
  physical column silently rides along on another.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import typing_phase
from dataraum.pipeline.phases.typing_phase import (
    StronglyTypedVerificationFailed,
    TypingPhase,
    _describe_live_types,
)
from dataraum.storage import Column, Source, Table

_RUN = "run-1"


@pytest.fixture
def lake() -> Iterator[duckdb.DuckDBPyConnection]:
    """A minimal ``lake.raw``/``lake.typed`` DuckDB catalog, matching production
    naming (``core.duckdb_naming.schema_for_layer`` / ``LAKE_CATALOG_ALIAS``)."""
    c = duckdb.connect()
    try:
        c.execute("ATTACH ':memory:' AS lake")
        c.execute("CREATE SCHEMA lake.raw")
        c.execute("CREATE SCHEMA lake.typed")
        yield c
    finally:
        c.close()


def _seed_table(session: Session, *, bare: str, columns: list[tuple[str, str]]) -> Table:
    """A raw Table + Columns whose metadata ``raw_type`` is ``columns``' 2nd item.

    Does NOT create the physical DuckDB table — callers create that directly so
    the physical schema can deliberately diverge from this metadata (the DAT-748
    scenario under test).
    """
    source = Source(name=f"src_{uuid4().hex[:8]}", source_type="postgres")
    session.add(source)
    session.flush()
    table = Table(
        source_id=source.source_id,
        table_name=bare,
        layer="raw",
        row_count=2,
        duckdb_path=bare,
    )
    session.add(table)
    session.flush()
    for pos, (name, raw_type) in enumerate(columns):
        session.add(
            Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=name,
                column_position=pos,
                raw_type=raw_type,
            )
        )
    session.flush()
    session.refresh(table)
    return table


def _decision(session: Session, column_id: str) -> TypeDecision:
    return session.execute(
        select(TypeDecision).where(TypeDecision.column_id == column_id, TypeDecision.run_id == _RUN)
    ).scalar_one()


def _typed_columns_by_name(session: Session, bare: str) -> dict[str, Column]:
    """The reconciled TYPED table's Columns, by name — a SEPARATE row/id from
    the raw Column of the same name (``reconcile_typed_columns``)."""
    typed_table = session.execute(
        select(Table).where(Table.table_name == bare, Table.layer == "typed")
    ).scalar_one()
    return {c.column_name: c for c in typed_table.columns}


class TestDescribeLiveTypes:
    def test_reports_live_duckdb_types(self, lake: duckdb.DuckDBPyConnection) -> None:
        lake.execute('CREATE TABLE lake.raw."t1" (a BIGINT, b VARCHAR)')
        live = _describe_live_types(lake, 'lake.raw."t1"')
        assert live == {"a": "BIGINT", "b": "VARCHAR"}

    def test_fails_closed_to_none_on_a_describe_error(
        self, lake: duckdb.DuckDBPyConnection
    ) -> None:
        """``None`` (never ``{}``) signals "couldn't check" — the caller must
        not read a failed DESCRIBE as "checked, disagreed on everything"."""
        assert _describe_live_types(lake, 'lake.raw."does_not_exist"') is None


class TestPromoteStronglyTyped:
    def test_matching_declared_type_is_trusted_and_promoted_verbatim(
        self, session: Session, lake: duckdb.DuckDBPyConnection
    ) -> None:
        """The catalog's raw_type matches the raw table's live type — promoted
        as before: physical passthrough, ``decision_source="automatic"``."""
        lake.execute('CREATE TABLE lake.raw."vendors" (vendor_id BIGINT, name VARCHAR)')
        lake.execute("INSERT INTO lake.raw.\"vendors\" VALUES (1, 'Acme'), (2, 'Globex')")
        table = _seed_table(
            session, bare="vendors", columns=[("vendor_id", "BIGINT"), ("name", "VARCHAR")]
        )
        ctx = PhaseContext(session=session, duckdb_conn=lake, run_id=_RUN)

        typed_table_id, type_decisions, warnings = TypingPhase()._promote_strongly_typed(table, ctx)

        assert warnings == []
        typed_cols = _typed_columns_by_name(session, "vendors")
        vendor_id_col = typed_cols["vendor_id"]
        assert vendor_id_col.resolved_type == "BIGINT"
        decision = _decision(session, vendor_id_col.column_id)
        assert decision.decision_source == "automatic"
        assert decision.decided_type == "BIGINT"
        assert type_decisions[vendor_id_col.column_id] == "BIGINT"

        # Physical passthrough: the typed column is still BIGINT (no CAST).
        live_typed = {
            str(r[0]): str(r[1]) for r in lake.execute('DESCRIBE lake.typed."vendors"').fetchall()
        }
        assert live_typed["vendor_id"] == "BIGINT"
        assert typed_table_id  # a typed Table row was reconciled

    def test_mismatched_but_present_live_type_is_adopted_not_discarded(
        self, session: Session, lake: duckdb.DuckDBPyConnection
    ) -> None:
        """Owner ruling: the live DESCRIBE reads the exact relation the CTAS
        then selects from — it is ground truth at that instant, the catalog
        is the only suspect party. The catalog metadata (stale — e.g. a
        re-extraction under a different numeric width, or a reconciliation
        bug) says ``amount`` is INTEGER, but the raw table's LIVE physical
        column is actually BIGINT. The corrected type is ADOPTED (automatic),
        never discarded to a generic VARCHAR fallback.
        """
        lake.execute('CREATE TABLE lake.raw."ledger" (id BIGINT, amount BIGINT)')
        lake.execute('INSERT INTO lake.raw."ledger" VALUES (1, 100), (2, -50)')
        table = _seed_table(
            session,
            bare="ledger",
            # Metadata is stale about `amount`: claims INTEGER while the
            # physical raw column is BIGINT.
            columns=[("id", "BIGINT"), ("amount", "INTEGER")],
        )
        ctx = PhaseContext(session=session, duckdb_conn=lake, run_id=_RUN)

        _typed_table_id, type_decisions, warnings = TypingPhase()._promote_strongly_typed(
            table, ctx
        )

        assert len(warnings) == 1
        assert "amount" in warnings[0]
        assert "ground truth" in warnings[0] or "corrected" in warnings[0]
        assert "downgraded" not in warnings[0]

        typed_cols = _typed_columns_by_name(session, "ledger")
        id_col, amount_col = typed_cols["id"], typed_cols["amount"]

        # The honest column is untouched.
        assert id_col.resolved_type == "BIGINT"
        id_decision = _decision(session, id_col.column_id)
        assert id_decision.decision_source == "automatic"

        # The mismatched column is ADOPTED at the live type — automatic, not
        # a fallback — and the live type (BIGINT), not VARCHAR.
        assert amount_col.resolved_type == "BIGINT"
        amount_decision = _decision(session, amount_col.column_id)
        assert amount_decision.decision_source == "automatic"
        assert amount_decision.decided_type == "BIGINT"
        assert "stale" in amount_decision.decision_reason
        assert "adopted live type" in amount_decision.decision_reason
        assert type_decisions[amount_col.column_id] == "BIGINT"

        # Physical honesty: the typed table's amount column is ACTUALLY BIGINT
        # (a real CAST happened, to the ADOPTED type) — the raw_type on the RAW
        # Column row is left untouched (typing doesn't own that field).
        live_typed = {
            str(r[0]): str(r[1]) for r in lake.execute('DESCRIBE lake.typed."ledger"').fetchall()
        }
        assert live_typed["amount"] == "BIGINT"
        assert live_typed["id"] == "BIGINT"
        raw_amount = next(c for c in table.columns if c.column_name == "amount")
        assert raw_amount.raw_type == "INTEGER"

    def test_column_absent_from_live_schema_fails_loud(
        self, session: Session, lake: duckdb.DuckDBPyConnection
    ) -> None:
        """A column the catalog references but that no longer physically
        exists in the raw table (dropped/renamed out from under the metadata)
        has no live type to adopt — reserved for the VARCHAR downgrade. But an
        explicit projection referencing a nonexistent column fails immediately
        at execution: that IS the "loud anyway" outcome the owner ruling
        names, not a silently-persisted downgrade decision.
        """
        lake.execute('CREATE TABLE lake.raw."stale" (id BIGINT)')
        lake.execute('INSERT INTO lake.raw."stale" VALUES (1), (2)')
        table = _seed_table(
            session,
            bare="stale",
            # Metadata references "ghost", which was never physically created.
            columns=[("id", "BIGINT"), ("ghost", "BIGINT")],
        )
        ctx = PhaseContext(session=session, duckdb_conn=lake, run_id=_RUN)

        with pytest.raises(duckdb.Error):
            TypingPhase()._promote_strongly_typed(table, ctx)

    def test_unconditional_projection_drops_an_untracked_physical_column_consistently(
        self, session: Session, lake: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-748 nit: the projection must be explicit UNCONDITIONALLY, never
        a blanket ``SELECT *`` only-when-no-mismatch-was-found — otherwise an
        untracked physical column (one the metadata never registered) would
        silently ride along in the no-mismatch case and vanish the moment some
        OTHER column happens to need adopting/downgrading. Proven here on the
        clean, no-mismatch path: an extra physical column absent from the
        Column metadata entirely does not appear in the typed table.
        """
        lake.execute('CREATE TABLE lake.raw."widgets" (widget_id BIGINT, untracked VARCHAR)')
        lake.execute("INSERT INTO lake.raw.\"widgets\" VALUES (1, 'x'), (2, 'y')")
        # Metadata knows ONLY widget_id — "untracked" is a real physical
        # column the metadata never registered (e.g. a schema drift the
        # importer missed).
        table = _seed_table(session, bare="widgets", columns=[("widget_id", "BIGINT")])
        ctx = PhaseContext(session=session, duckdb_conn=lake, run_id=_RUN)

        TypingPhase()._promote_strongly_typed(table, ctx)

        live_typed = {str(r[0]) for r in lake.execute('DESCRIBE lake.typed."widgets"').fetchall()}
        assert live_typed == {"widget_id"}  # "untracked" consistently excluded


class TestCouldntCheckVsDisagreed:
    """DAT-748: a failed DESCRIBE ("couldn't check") is a different failure
    mode from a successful DESCRIBE that disagrees — never the same message,
    never the same destructive action."""

    def test_promote_raises_when_describe_itself_fails(
        self,
        session: Session,
        lake: duckdb.DuckDBPyConnection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        lake.execute('CREATE TABLE lake.raw."vendors" (vendor_id BIGINT)')
        table = _seed_table(session, bare="vendors", columns=[("vendor_id", "BIGINT")])
        ctx = PhaseContext(session=session, duckdb_conn=lake, run_id=_RUN)
        monkeypatch.setattr(typing_phase, "_describe_live_types", lambda *_a, **_kw: None)

        with pytest.raises(StronglyTypedVerificationFailed, match="could not verify"):
            TypingPhase()._promote_strongly_typed(table, ctx)

    def test_run_fails_only_the_unverifiable_table_others_still_type(
        self,
        session: Session,
        lake: duckdb.DuckDBPyConnection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end through ``_run`` (not just the isolated helper): a
        DESCRIBE failure on ONE strongly-typed table must not be treated as a
        mismatch on every column, must not crash the whole phase, and must
        not silently type the unverifiable table anyway — it is skipped, with
        an honest warning naming the real cause, while a healthy sibling
        table still types normally.
        """
        lake.execute('CREATE TABLE lake.raw."vendors" (vendor_id BIGINT)')
        lake.execute('CREATE TABLE lake.raw."products" (product_id BIGINT)')
        lake.execute('INSERT INTO lake.raw."products" VALUES (1), (2)')
        vendors = _seed_table(session, bare="vendors", columns=[("vendor_id", "BIGINT")])
        products = _seed_table(session, bare="products", columns=[("product_id", "BIGINT")])

        real_describe = typing_phase._describe_live_types

        def _flaky_on_vendors(conn: duckdb.DuckDBPyConnection, target_fqn: str) -> dict | None:
            if "vendors" in target_fqn:
                return None
            return real_describe(conn, target_fqn)

        monkeypatch.setattr(typing_phase, "_describe_live_types", _flaky_on_vendors)

        ctx = PhaseContext(
            session=session,
            duckdb_conn=lake,
            table_ids=[vendors.table_id, products.table_id],
            run_id=_RUN,
        )

        result = TypingPhase()._run(ctx)

        assert result.status is PhaseStatus.COMPLETED
        assert any("vendors" in w and "could not verify" in w for w in result.warnings)

        # products typed normally; vendors was never promoted.
        assert lake.execute("SELECT COUNT(*) FROM lake.typed.products").fetchone()[0] == 2
        with pytest.raises(duckdb.Error):
            lake.execute("SELECT COUNT(*) FROM lake.typed.vendors")
