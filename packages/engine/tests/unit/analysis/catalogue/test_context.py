"""The composed-catalogue context builder (DAT-823).

Pins the load-bearing serving properties: run-scoped reads (this run's
TableEntity/Relationship/EnrichedView/SliceDefinition, never a coexisting
run's), generation-head-pinned profiles (fail-closed on a missing head), the
privacy gate on every sample, endpoint samples riding the relationship lines,
honest conformed_dimension serving, and the deterministic shared-axis pairing.
"""

from __future__ import annotations

from typing import Any

import duckdb

from dataraum.analysis.catalogue.context import build_catalogue_inputs
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.llm.config import LLMPrivacy
from dataraum.llm.privacy import DataSampler
from dataraum.storage import Column, Source, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead
from tests.conftest import baseline_run_id

_GEN_RUN = "gen-run-1"


def _sampler(patterns: list[str] | None = None) -> DataSampler:
    return DataSampler(LLMPrivacy(sensitive_patterns=patterns or []))


def _mk_table(session, name: str, columns: list[str], *, duckdb_path: str | None = None) -> Table:
    src = Source(name=f"src_{name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(
        source_id=src.source_id,
        table_name=name,
        layer="typed",
        row_count=100,
        duckdb_path=duckdb_path,
    )
    session.add(table)
    session.flush()
    for pos, col in enumerate(columns):
        session.add(
            Column(
                table_id=table.table_id, column_name=col, column_position=pos, raw_type="VARCHAR"
            )
        )
    session.flush()
    return table


def _promote(session, table: Table, run_id: str = _GEN_RUN) -> None:
    session.add(
        MetadataSnapshotHead(
            target=f"table:{table.table_id}", stage=GENERATION_STAGE, run_id=run_id
        )
    )
    session.flush()


def _col_id(session, table: Table, name: str) -> str:
    return next(
        c.column_id
        for c in session.execute(
            Column.__table__.select().where(Column.table_id == table.table_id)
        ).all()
        if c.column_name == name
    )


def _profile(
    session,
    column_id: str,
    *,
    run_id: str = _GEN_RUN,
    top: list[tuple[Any, int, float]] | None = None,
    numeric: dict[str, Any] | None = None,
) -> None:
    data: dict[str, Any] = {}
    if top is not None:
        data["top_values"] = [{"value": v, "count": c, "percentage": p} for v, c, p in top]
    if numeric is not None:
        data["numeric_stats"] = numeric
    session.add(
        StatisticalProfile(
            column_id=column_id,
            run_id=run_id,
            layer="typed",
            total_count=100,
            null_count=0,
            profile_data=data,
        )
    )
    session.flush()


def _entity(session, table: Table, **kw) -> TableEntity:
    entity = TableEntity(
        run_id=kw.get("run_id", baseline_run_id()),
        table_id=table.table_id,
        detected_entity_type=None,
        grain_columns=kw.get("grain_columns", ["id"]),
        table_role=kw.get("table_role", "fact"),
        time_columns=kw.get("time_columns"),
        identity_columns=kw.get("identity_columns"),
        detection_source="llm",
    )
    session.add(entity)
    session.flush()
    return entity


def _relationship(
    session,
    from_table: Table,
    from_col: str,
    to_table: Table,
    to_col: str,
    **kw,
) -> Relationship:
    rel = Relationship(
        run_id=kw.get("run_id", baseline_run_id()),
        from_table_id=from_table.table_id,
        from_column_id=_col_id(session, from_table, from_col),
        to_table_id=to_table.table_id,
        to_column_id=_col_id(session, to_table, to_col),
        relationship_type=kw.get("relationship_type", "foreign_key"),
        cardinality=kw.get("cardinality", "many-to-one"),
        confidence=kw.get("confidence", 0.9),
        detection_method=kw.get("detection_method", "llm"),
        evidence=kw.get("evidence", {}),
    )
    session.add(rel)
    session.flush()
    return rel


def _build(session, tables: list[Table], **kw) -> dict[str, str]:
    table_ids = [t.table_id for t in tables]
    return build_catalogue_inputs(
        session,
        # An ephemeral conn by default: tables without a duckdb_path serve no
        # chain-conditioned lines (fail-soft), so metadata-only tests run
        # against an empty database.
        kw.get("duckdb_conn") or duckdb.connect(),
        table_ids=kw.get("scope", table_ids),
        session_table_ids=table_ids,
        run_id=kw.get("run_id", baseline_run_id()),
        sampler=kw.get("sampler", _sampler()),
    )


class TestStructuralTables:
    def test_renders_role_grain_time_identity_and_measure_range(self, session) -> None:
        orders = _mk_table(session, "orders", ["id", "vendor", "amount"])
        _promote(session, orders)
        _entity(
            session,
            orders,
            grain_columns=["id"],
            time_columns=[
                {
                    "column": "order_date",
                    "aspect": "order",
                    "role": "event",
                    "is_anchor": True,
                    "note": "Placed.",
                }
            ],
            identity_columns=[{"column": "vendor", "note": "Recurs across rows."}],
        )
        _profile(session, _col_id(session, orders, "vendor"), top=[("ACME", 40, 40.0)])
        _profile(
            session,
            _col_id(session, orders, "amount"),
            numeric={"min_value": -500.0, "max_value": 9000.0, "mean": 100.0},
        )
        session.add(
            SemanticAnnotation(
                column_id=_col_id(session, orders, "amount"),
                run_id=_GEN_RUN,
                semantic_role="measure",
            )
        )
        session.flush()

        out = _build(session, [orders])
        text = out["structural_tables"]
        assert "### orders" in text and "role=fact" in text
        assert "grain: id" in text
        assert "time column order_date" in text and "anchor" in text
        assert "identity column vendor" in text
        assert "'ACME' (40%)" in text  # identity samples (not a relationship endpoint)
        assert "amount: min=-500.0 max=9000.0" in text
        assert "negative values present" in text

    def test_sign_line_renders_from_the_writers_real_profile_shape(self, session) -> None:
        """The measure sign line must survive the WRITER's serialization.

        ``profile_data`` is persisted as ``ColumnProfile.model_dump(mode="json")``
        (analysis/statistics/profiler.py), whose NumericStats keys are
        ``min_value``/``max_value``. The reader once assumed ``min``/``max`` and
        the fixtures mirrored the reader instead of the writer, so the sign line
        silently never rendered in production while tests stayed green
        (DAT-853). This test builds profile_data through the writer's own
        models — if either side's key shape drifts, it fails here."""
        from datetime import UTC, datetime

        from dataraum.analysis.statistics.models import ColumnProfile, NumericStats
        from dataraum.core.models.base import ColumnRef

        ledger = _mk_table(session, "ledger", ["net_amount"])
        _promote(session, ledger)
        column_id = _col_id(session, ledger, "net_amount")
        writer_profile = ColumnProfile(
            column_id=column_id,
            column_ref=ColumnRef(table_name="ledger", column_name="net_amount"),
            profiled_at=datetime.now(UTC),
            total_count=100,
            null_count=0,
            distinct_count=90,
            null_ratio=0.0,
            cardinality_ratio=0.9,
            numeric_stats=NumericStats(min_value=-250.5, max_value=1200.0, mean=10.0, stddev=5.0),
        )
        session.add(
            StatisticalProfile(
                column_id=column_id,
                run_id=_GEN_RUN,
                layer="typed",
                total_count=100,
                null_count=0,
                profile_data=writer_profile.model_dump(mode="json"),
            )
        )
        session.add(
            SemanticAnnotation(column_id=column_id, run_id=_GEN_RUN, semantic_role="measure")
        )
        session.flush()

        out = _build(session, [ledger])
        assert (
            "net_amount: min=-250.5 max=1200.0 — negative values present"
            in out["structural_tables"]
        )

    def test_missing_generation_head_serves_no_samples_or_ranges(self, session) -> None:
        """Fail-closed: no promoted head → no profile reads, never an arbitrary run's."""
        orders = _mk_table(session, "orders", ["id", "vendor"])
        # NO _promote. A profile exists under some run, but nothing pins it.
        _profile(
            session, _col_id(session, orders, "vendor"), run_id="some-run", top=[("X", 1, 1.0)]
        )
        _entity(session, orders, identity_columns=[{"column": "vendor", "note": "n"}])

        out = _build(session, [orders])
        assert "'X'" not in out["structural_tables"]
        assert "No per-column annotations" in out["column_annotations"]

    def test_entity_rows_are_run_scoped(self, session) -> None:
        """Another run's TableEntity never serves this run's structural section."""
        orders = _mk_table(session, "orders", ["id"])
        _entity(session, orders, run_id="other-run", table_role="dimension")

        out = _build(session, [orders])
        assert "no structural reading for this table in this run" in out["structural_tables"]


class TestAnnotations:
    def test_serves_role_entity_term_claim_head_scoped(self, session) -> None:
        orders = _mk_table(session, "orders", ["id", "amount"])
        _promote(session, orders)
        amount_id = _col_id(session, orders, "amount")
        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                run_id=_GEN_RUN,
                semantic_role="measure",
                entity_type="transaction_amount",
                business_name="Transaction Amount",
                temporal_behavior_claim="flow",
                temporal_behavior_claim_confidence=0.9,
            )
        )
        # A coexisting stale run's row for the same column — must NOT serve.
        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                run_id="stale-run",
                semantic_role="dimension",
                entity_type="stale_entity",
            )
        )
        session.flush()

        out = _build(session, [orders])
        text = out["column_annotations"]
        assert "role=measure" in text
        assert "entity=transaction_amount" in text
        assert "term='Transaction Amount'" in text
        assert "claim=flow(0.90)" in text
        assert "stale_entity" not in text


class TestRelationships:
    def test_confirmed_line_carries_evidence_reasoning_and_endpoint_samples(self, session) -> None:
        orders = _mk_table(session, "orders", ["id", "vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "vendor_name"])
        _promote(session, orders)
        _promote(session, vendors)
        _relationship(
            session,
            orders,
            "vendor_id",
            vendors,
            "vendor_id",
            evidence={
                "left_referential_integrity": 100.0,
                "right_referential_integrity": 60.0,
                "left_key_coverage": 100.0,
                "right_key_coverage": 60.0,
                "left_orphan_count": 0,
                "right_orphan_count": 4,
                "introduces_duplicates": False,
                "reasoning": "clean FK to the vendor master",
            },
        )
        _profile(session, _col_id(session, orders, "vendor_id"), top=[("V001", 12, 12.0)])
        _profile(session, _col_id(session, vendors, "vendor_id"), top=[("V001", 1, 1.0)])

        out = _build(session, [orders, vendors])
        text = out["relationship_catalogue"]
        assert "orders.vendor_id -> vendors.vendor_id (foreign_key, many-to-one" in text
        assert "rows resolving: L=100% R=60%" in text
        assert "unresolved rows: L=0 R=4" in text
        assert "fan trap: False" in text
        assert "reasoning: clean FK to the vendor master" in text
        # THE load-bearing serving: endpoint values ride the relationship line.
        assert "from values (vendor_id): 'V001' (12%)" in text
        assert "to values (vendor_id): 'V001' (1%)" in text

    def test_conformed_dimension_kind_served_honestly(self, session) -> None:
        a = _mk_table(session, "ap_ledger", ["period"])
        b = _mk_table(session, "ar_ledger", ["period"])
        _relationship(
            session,
            a,
            "period",
            b,
            "period",
            relationship_type="conformed_dimension",
            cardinality="many-to-many",
        )
        out = _build(session, [a, b])
        assert (
            "conformed_dimension — two facts meeting at a shared axis"
            in out["relationship_catalogue"]
        )

    def test_sensitive_endpoint_redacts_values(self, session) -> None:
        orders = _mk_table(session, "orders", ["id", "customer_email"])
        customers = _mk_table(session, "customers", ["customer_email"])
        _promote(session, orders)
        _promote(session, customers)
        _relationship(session, orders, "customer_email", customers, "customer_email")
        _profile(session, _col_id(session, orders, "customer_email"), top=[("a@b.com", 3, 3.0)])

        out = _build(session, [orders, customers], sampler=_sampler([r".*email.*"]))
        text = out["relationship_catalogue"]
        assert "<REDACTED>" in text
        assert "a@b.com" not in text

    def test_endpoint_identity_samples_not_duplicated_in_table_section(self, session) -> None:
        orders = _mk_table(session, "orders", ["id", "vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id"])
        _promote(session, orders)
        _promote(session, vendors)
        _entity(session, orders, identity_columns=[{"column": "vendor_id", "note": "n"}])
        _relationship(session, orders, "vendor_id", vendors, "vendor_id")
        _profile(session, _col_id(session, orders, "vendor_id"), top=[("V001", 12, 12.0)])

        out = _build(session, [orders, vendors])
        # On the relationship line, not repeated under the identity column.
        assert "from values (vendor_id)" in out["relationship_catalogue"]
        assert "values: 'V001'" not in out["structural_tables"]

    def test_relationships_are_run_scoped(self, session) -> None:
        orders = _mk_table(session, "orders", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id"])
        _relationship(session, orders, "vendor_id", vendors, "vendor_id", run_id="other-run")

        out = _build(session, [orders, vendors])
        assert "No confirmed relationships" in out["relationship_catalogue"]


class TestChainConditionedSamples:
    """Chain-conditioned label evidence on the relationship lines (DAT-853).

    The AP forensics shape: a label column mixing several populations over all
    rows (vendors + customers + fees) is unanimous on the rows that ride the
    join. The conditioned aggregate must serve the discriminating distribution
    — flat samples alone rendered the mixed one and the cycle was mislabeled.
    """

    def _payment_chain(self, session, duck: duckdb.DuckDBPyConnection) -> tuple[Table, Table]:
        """bank_txns.payment_id -> payments.payment_id with a discriminating
        conditioned distribution: flat counterparty is customer-dominated,
        payment-linked counterparty is 100% vendors; one orphan fk row whose
        label must NOT ride the join."""
        duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2'), ('P3'), ('P4')")
        duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, counterparty VARCHAR)")
        duck.execute(
            "INSERT INTO typed_bank_txns VALUES "
            "('P1', 'Vendor A'), ('P2', 'Vendor A'), ('P3', 'Vendor A'), ('P4', 'Vendor B'), "
            "('ORPHAN-1', 'Orphan Corp'), "  # fk set, never resolves
            "(NULL, 'Customer X'), (NULL, 'Customer X'), (NULL, 'Customer X'), "
            "(NULL, 'Customer X'), (NULL, 'Bank Fee'), (NULL, 'Bank Fee')"
        )
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "counterparty"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _promote(session, bank)
        _promote(session, payments)
        _entity(
            session,
            bank,
            identity_columns=[
                {"column": "payment_id", "note": "matches the payments key"},
                {"column": "counterparty", "note": "external party label"},
            ],
        )
        _relationship(session, bank, "payment_id", payments, "payment_id")
        return bank, payments

    def test_conditioned_line_serves_the_discriminating_distribution(self, session) -> None:
        duck = duckdb.connect()
        bank, payments = self._payment_chain(session, duck)
        # Flat endpoint samples stay served next to the conditioned line.
        _profile(session, _col_id(session, bank, "payment_id"), top=[("P1", 1, 9.0)])

        out = _build(session, [bank, payments], duckdb_conn=duck)
        text = out["relationship_catalogue"]
        # The conditioned line: only join-riding rows, frequency-ordered, with
        # percentages over the joined population (4 rows).
        assert "counterparty (payment_id-joined rows): 'Vendor A' (75%), 'Vendor B' (25%)" in text
        # Flat (all-rows) evidence is additional, not replaced.
        assert "from values (payment_id): 'P1' (9%)" in text
        # NULL-fk populations (customers, fees) and the orphan fk (claims a
        # link that never resolves) must not ride the join.
        assert "Customer X" not in text
        assert "Bank Fee" not in text
        assert "Orphan Corp" not in text
        # The fk column itself is a join key — never served as a conditioned
        # label (its IDs are the information-free evidence class).
        assert "payment_id (payment_id-joined rows)" not in text

    def test_sensitive_label_renders_redacted_without_touching_data(self, session) -> None:
        duck = duckdb.connect()
        duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_payments VALUES ('P1')")
        # The label column does NOT exist in duckdb: if the builder tried to
        # aggregate it the query would fail loudly — redaction must short-circuit.
        duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_bank_txns VALUES ('P1')")
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "owner_email"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _entity(session, bank, identity_columns=[{"column": "owner_email", "note": "contact"}])
        _relationship(session, bank, "payment_id", payments, "payment_id")

        out = _build(session, [bank, payments], duckdb_conn=duck, sampler=_sampler([r".*email.*"]))
        assert "owner_email (payment_id-joined rows): <REDACTED>" in out["relationship_catalogue"]

    def test_conformed_dimension_serves_no_conditioned_lines(self, session) -> None:
        """No fk rides a shared-axis meeting — conditioning has no join to honor."""
        duck = duckdb.connect()
        duck.execute("CREATE TABLE typed_ap (period VARCHAR, vendor VARCHAR)")
        duck.execute("INSERT INTO typed_ap VALUES ('2025-01', 'Vendor A')")
        duck.execute("CREATE TABLE typed_ar (period VARCHAR)")
        duck.execute("INSERT INTO typed_ar VALUES ('2025-01')")
        ap = _mk_table(session, "ap_ledger", ["period", "vendor"], duckdb_path="typed_ap")
        ar = _mk_table(session, "ar_ledger", ["period"], duckdb_path="typed_ar")
        _entity(session, ap, identity_columns=[{"column": "vendor", "note": "n"}])
        _relationship(
            session,
            ap,
            "period",
            ar,
            "period",
            relationship_type="conformed_dimension",
            cardinality="many-to-many",
        )

        out = _build(session, [ap, ar], duckdb_conn=duck)
        assert "-joined rows" not in out["relationship_catalogue"]

    def test_missing_typed_table_fails_soft(self, session) -> None:
        """A duckdb_path that resolves to no table logs and serves nothing —
        the prompt build must survive a missing typed table."""
        duck = duckdb.connect()  # empty database, paths dangle
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "counterparty"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _entity(session, bank, identity_columns=[{"column": "counterparty", "note": "n"}])
        _relationship(session, bank, "payment_id", payments, "payment_id")

        out = _build(session, [bank, payments], duckdb_conn=duck)
        assert "bank_txns.payment_id -> payments.payment_id" in out["relationship_catalogue"]
        assert "-joined rows" not in out["relationship_catalogue"]

    def test_conditioned_measure_range_serves_the_conditioned_sign(self, session) -> None:
        """Measure sign/range over the join-riding rows only: globally the
        amounts are mixed-sign (positive unlinked rows, a positive orphan),
        on the joined rows uniformly negative — the flow-sign evidence the
        flat table-level range blurs. A genuinely mixed measure says so
        plainly, in column order. No TableEntity is written: the measure
        selection rides the pinned annotations, not the entity's identity
        columns."""
        duck = duckdb.connect()
        duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2')")
        duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, amount DOUBLE, fee DOUBLE)")
        duck.execute(
            "INSERT INTO typed_bank_txns VALUES "
            "('P1', -135000.0, -5.0), ('P2', -50.25, 10.0), "
            "('ORPHAN-1', 77.0, 1.0), "  # fk set, never resolves — must not ride
            "(NULL, 200.0, 2.0), (NULL, 950.5, 3.0)"  # unlinked rows: positive
        )
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "amount", "fee"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _promote(session, bank)
        _promote(session, payments)
        for name in ("amount", "fee"):
            session.add(
                SemanticAnnotation(
                    column_id=_col_id(session, bank, name),
                    run_id=_GEN_RUN,
                    semantic_role="measure",
                )
            )
        session.flush()
        _relationship(session, bank, "payment_id", payments, "payment_id")

        out = _build(session, [bank, payments], duckdb_conn=duck)
        text = out["relationship_catalogue"]
        assert "amount (payment_id-joined rows): min=-135000.0 max=-50.25 — all negative" in text
        assert "fee (payment_id-joined rows): min=-5.0 max=10.0 — mixed signs" in text
        assert text.index("amount (payment_id-joined") < text.index("fee (payment_id-joined")

    def test_sensitive_measure_range_renders_redacted(self, session) -> None:
        """The label convention holds for measure ranges: ``<REDACTED>``
        without touching the data (the column does NOT exist in duckdb — an
        aggregate would fail loudly, redaction must short-circuit)."""
        duck = duckdb.connect()
        duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_payments VALUES ('P1')")
        duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_bank_txns VALUES ('P1')")
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "salary_amount"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _promote(session, bank)
        _promote(session, payments)
        session.add(
            SemanticAnnotation(
                column_id=_col_id(session, bank, "salary_amount"),
                run_id=_GEN_RUN,
                semantic_role="measure",
            )
        )
        session.flush()
        _relationship(session, bank, "payment_id", payments, "payment_id")

        out = _build(session, [bank, payments], duckdb_conn=duck, sampler=_sampler([r".*salary.*"]))
        assert "salary_amount (payment_id-joined rows): <REDACTED>" in out["relationship_catalogue"]

    def test_nan_tainted_measure_range_serves_nothing(self, session) -> None:
        """A NaN row poisons MIN/MAX (DuckDB sorts NaN greatest) and every
        sign comparison — a poisoned range would read 'all positive'.
        Absence is the honest serving."""
        duck = duckdb.connect()
        duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
        duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2')")
        duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, amount DOUBLE)")
        duck.execute(
            "INSERT INTO typed_bank_txns VALUES ('P1', 2.0), ('P2', CAST('nan' AS DOUBLE))"
        )
        bank = _mk_table(
            session, "bank_txns", ["payment_id", "amount"], duckdb_path="typed_bank_txns"
        )
        payments = _mk_table(session, "payments", ["payment_id"], duckdb_path="typed_payments")
        _promote(session, bank)
        _promote(session, payments)
        session.add(
            SemanticAnnotation(
                column_id=_col_id(session, bank, "amount"),
                run_id=_GEN_RUN,
                semantic_role="measure",
            )
        )
        session.flush()
        _relationship(session, bank, "payment_id", payments, "payment_id")

        out = _build(session, [bank, payments], duckdb_conn=duck)
        assert "amount (payment_id-joined rows)" not in out["relationship_catalogue"]


class TestEnrichedViewsAndAxes:
    def test_views_render_fact_dims_and_join_pairs(self, session) -> None:
        orders = _mk_table(session, "orders", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id"])
        rel = _relationship(session, orders, "vendor_id", vendors, "vendor_id")
        session.add(
            EnrichedView(
                fact_table_id=orders.table_id,
                view_name="orders_enriched",
                run_id=baseline_run_id(),
                dimension_table_ids=[vendors.table_id],
                relationship_ids=[rel.relationship_id],
            )
        )
        session.flush()

        out = _build(session, [orders, vendors])
        text = out["enriched_views"]
        assert "orders_enriched: fact=orders, dimensions=[vendors]" in text
        assert "joins orders.vendor_id -> vendors.vendor_id" in text

    def test_shared_axis_pairing_pairs_different_tables_only(self, session) -> None:
        # Same FK ROLE name across facts → one structural axis (DAT-788): the
        # pairing fires between DIFFERENT tables, never self, and folded slices
        # (no dim identity) never pair.
        ap = _mk_table(session, "ap_ledger", ["vendor_id"])
        ar = _mk_table(session, "ar_ledger", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "region"])

        def _slice(fact: Table, col: str, dim: Table | None, attr: str | None) -> None:
            session.add(
                SliceDefinition(
                    run_id=baseline_run_id(),
                    table_id=fact.table_id,
                    column_id=_col_id(session, fact, col),
                    column_name=f"{col}__{attr}" if attr else col,
                    dimension_table_id=dim.table_id if dim else None,
                    dimension_attribute=attr,
                    fk_role=col if dim else None,
                    slice_priority=5,
                    slice_type="categorical",
                )
            )
            session.flush()

        _slice(ap, "vendor_id", vendors, "region")
        _slice(ar, "vendor_id", vendors, "region")  # same axis + role, different fact
        _slice(ap, "vendor_id", None, None)  # folded — no dimension identity

        out = _build(session, [ap, ar, vendors])
        text = out["shared_axes"]
        assert "ap_ledger slices by vendors.region via vendor_id" in text
        assert "Shared axes" in text
        assert "vendors.region: ap_ledger (via vendor_id) <-> ar_ledger (via vendor_id)" in text

    def test_role_playing_fks_do_not_pair_structurally(self, session) -> None:
        # DAT-788 safe default: differently-named FK roles to one axis (vendor_id
        # vs customer_id → vendors.region) are SEPARATE pre-judge — this served
        # context runs before the conform judge, so it never merges cross-role.
        ap = _mk_table(session, "ap_ledger", ["vendor_id"])
        ar = _mk_table(session, "ar_ledger", ["customer_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "region"])
        for fact, col in ((ap, "vendor_id"), (ar, "customer_id")):
            session.add(
                SliceDefinition(
                    run_id=baseline_run_id(),
                    table_id=fact.table_id,
                    column_id=_col_id(session, fact, col),
                    column_name=f"{col}__region",
                    dimension_table_id=vendors.table_id,
                    dimension_attribute="region",
                    fk_role=col,
                    slice_priority=5,
                    slice_type="categorical",
                )
            )
        session.flush()

        text = _build(session, [ap, ar, vendors])["shared_axes"]
        assert "ap_ledger slices by vendors.region via vendor_id" in text
        assert "ar_ledger slices by vendors.region via customer_id" in text
        assert "Shared axes" not in text  # separate roles, never paired pre-judge

    def test_pairing_order_is_name_keyed_not_uuid_keyed(self, session) -> None:
        """Members sort by resolved table NAME, never by table_id — a uuid sort
        key reshuffles identical catalogues between runs (the DAT-725
        instability class; reproduced live as a flaky render before the fix)."""
        # Created in reverse-alphabetical order so an insertion-order or
        # id-based sort would have a 50/50 chance of exposing itself; the
        # name-keyed sort renders alpha first every time.
        zeta = _mk_table(session, "zeta_ledger", ["vendor_id"])
        alpha = _mk_table(session, "alpha_ledger", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "region"])
        for fact in (zeta, alpha):
            session.add(
                SliceDefinition(
                    run_id=baseline_run_id(),
                    table_id=fact.table_id,
                    column_id=_col_id(session, fact, "vendor_id"),
                    column_name="vendor_id__region",
                    dimension_table_id=vendors.table_id,
                    dimension_attribute="region",
                    fk_role="vendor_id",
                    slice_priority=5,
                    slice_type="categorical",
                )
            )
        session.flush()

        out = _build(session, [zeta, alpha, vendors])
        assert (
            "vendors.region: alpha_ledger (via vendor_id) <-> zeta_ledger (via vendor_id)"
            in out["shared_axes"]
        )

    def test_scoped_retry_still_sees_the_out_of_scope_pair_partner(self, session) -> None:
        """A coverage retry narrowed to one fact must still see the shared-axis
        pairing with the OUT-of-scope partner — a pair needs rows from both
        facts, so the scope filters the rendering, never the load."""
        ap = _mk_table(session, "ap_ledger", ["vendor_id"])
        ar = _mk_table(session, "ar_ledger", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "region"])
        for fact in (ap, ar):
            session.add(
                SliceDefinition(
                    run_id=baseline_run_id(),
                    table_id=fact.table_id,
                    column_id=_col_id(session, fact, "vendor_id"),
                    column_name="vendor_id__region",
                    dimension_table_id=vendors.table_id,
                    dimension_attribute="region",
                    fk_role="vendor_id",
                    slice_priority=5,
                    slice_type="categorical",
                )
            )
        session.flush()

        out = _build(session, [ap, ar, vendors], scope=[ap.table_id])
        text = out["shared_axes"]
        assert "vendors.region: ap_ledger (via vendor_id) <-> ar_ledger (via vendor_id)" in text
        # The rendering is still scope-filtered: the out-of-scope fact's own
        # per-fact axis line is not served.
        assert "ar_ledger slices by" not in text

    def test_single_fact_axis_is_not_paired(self, session) -> None:
        ap = _mk_table(session, "ap_ledger", ["vendor_id"])
        vendors = _mk_table(session, "vendors", ["vendor_id", "region"])
        session.add(
            SliceDefinition(
                run_id=baseline_run_id(),
                table_id=ap.table_id,
                column_id=_col_id(session, ap, "vendor_id"),
                column_name="vendor_id__region",
                dimension_table_id=vendors.table_id,
                dimension_attribute="region",
                fk_role="vendor_id",
                slice_priority=5,
                slice_type="categorical",
            )
        )
        session.flush()

        out = _build(session, [ap, vendors])
        assert "Shared axes" not in out["shared_axes"]
        assert "ap_ledger slices by vendors.region" in out["shared_axes"]
