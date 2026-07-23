"""Tests for the validation schema resolver.

The resolver is an in-run reader (ADR-0008): every run-versioned read is
scoped by the pinned :class:`BaseRunMap` passed in — it never resolves heads
itself. Absent pins read EMPTY (fail-closed, DAT-429).
"""

import pytest

from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)
from dataraum.lifecycle import BaseRunMap
from dataraum.storage import Column, Source, Table

SEM_RUN = "sem-run-1"


@pytest.fixture
def table_with_columns(session):
    """Create a test table with columns and a run-stamped semantic annotation."""
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )

    # Create source and table
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        source_id=source.source_id,
        table_name="transactions",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_transactions",
    )
    session.add(table)
    session.flush()

    # Create columns
    col1 = Column(
        table_id=table.table_id,
        column_name="transaction_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    col2 = Column(
        table_id=table.table_id,
        column_name="amount",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="DECIMAL(18,2)",
    )
    col3 = Column(
        table_id=table.table_id,
        column_name="account_type",
        column_position=2,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([col1, col2, col3])
    session.flush()

    # Add semantic annotation to amount column, stamped with its run
    annotation = SemanticAnnotationDB(
        column_id=col2.column_id,
        run_id=SEM_RUN,
        semantic_role="measure",
        entity_type="amount",
        business_name="Transaction Amount",
    )
    session.add(annotation)
    session.commit()

    return table


@pytest.fixture
def two_tables_with_relationship(session):
    """Two tables + a run-stamped relationship under a seeded session."""
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )

    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    txn_table = Table(
        source_id=source.source_id,
        table_name="transactions",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_transactions",
    )
    acct_table = Table(
        source_id=source.source_id,
        table_name="accounts",
        layer="typed",
        row_count=50,
        duckdb_path="typed_accounts",
    )
    session.add_all([txn_table, acct_table])
    session.flush()

    txn_account_col = Column(
        table_id=txn_table.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    txn_amount_col = Column(
        table_id=txn_table.table_id,
        column_name="amount",
        column_position=1,
        raw_type="DECIMAL",
        resolved_type="DECIMAL(18,2)",
    )
    acct_id_col = Column(
        table_id=acct_table.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    acct_type_col = Column(
        table_id=acct_table.table_id,
        column_name="account_type",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([txn_account_col, txn_amount_col, acct_id_col, acct_type_col])
    session.flush()

    annotation = SemanticAnnotationDB(
        column_id=txn_amount_col.column_id,
        run_id=SEM_RUN,
        semantic_role="measure",
        entity_type="amount",
        business_name="Transaction Amount",
    )
    session.add(annotation)

    relationship = Relationship(
        run_id="run-current",
        from_table_id=txn_table.table_id,
        from_column_id=txn_account_col.column_id,
        to_table_id=acct_table.table_id,
        to_column_id=acct_id_col.column_id,
        relationship_type="foreign_key",
        cardinality="many-to-one",
        confidence=0.95,
        detection_method="llm",
    )
    session.add(relationship)
    session.commit()

    return txn_table, acct_table


def _pins(*tables: Table, relationship_run_id: str | None = "run-current") -> BaseRunMap:
    return BaseRunMap(
        relationship_run_id=relationship_run_id,
        semantic_runs={t.table_id: SEM_RUN for t in tables},
    )


def test_get_multi_table_schema_for_llm(session, two_tables_with_relationship):
    """Multi-table schema formats tables + the PINNED run's relationships."""
    txn_table, acct_table = two_tables_with_relationship

    schema = get_multi_table_schema_for_llm(
        session,
        [txn_table.table_id, acct_table.table_id],
        base_runs=_pins(txn_table, acct_table),
    )

    assert "error" not in schema
    assert "tables" in schema
    assert "relationships" in schema

    # Check tables are included
    assert len(schema["tables"]) == 2
    table_names = [t["table_name"] for t in schema["tables"]]
    assert "transactions" in table_names
    assert "accounts" in table_names

    # Check relationship is included
    assert len(schema["relationships"]) == 1
    rel = schema["relationships"][0]
    assert rel["from_table"] == "typed_transactions"
    assert rel["from_column"] == "account_id"
    assert rel["to_table"] == "typed_accounts"
    assert rel["to_column"] == "account_id"
    assert rel["relationship_type"] == "foreign_key"
    assert rel["confidence"] == 0.95


def test_relationships_scope_to_pinned_run(session, two_tables_with_relationship):
    """Only the pinned run's relationships surface; no pin reads NOTHING.

    A multi-run session coexists two runs' relationship catalogs (DAT-408).
    The resolver must return only the pinned run's rows — and with no pin,
    fail closed (DAT-429): never the cross-run union that would leak stale
    or foreign rows into the LLM schema.
    """
    from dataraum.analysis.relationships.db_models import Relationship

    txn_table, acct_table = two_tables_with_relationship
    table_ids = [txn_table.table_id, acct_table.table_id]

    current = session.query(Relationship).one()
    stale = Relationship(
        run_id="run-stale",
        from_table_id=current.from_table_id,
        from_column_id=current.from_column_id,
        to_table_id=current.to_table_id,
        to_column_id=current.to_column_id,
        relationship_type="foreign_key",
        cardinality="many-to-one",
        confidence=0.10,
        detection_method="llm",
    )
    session.add(stale)
    session.commit()

    pinned = get_multi_table_schema_for_llm(
        session, table_ids, base_runs=_pins(txn_table, acct_table)
    )
    assert len(pinned["relationships"]) == 1
    assert pinned["relationships"][0]["confidence"] == 0.95

    unpinned = get_multi_table_schema_for_llm(
        session, table_ids, base_runs=_pins(txn_table, acct_table, relationship_run_id=None)
    )
    assert unpinned["relationships"] == []


def test_annotations_scope_to_pinned_run(session, table_with_columns):
    """The multi-run annotation regression (DAT-438).

    SemanticAnnotation is run-versioned ((column_id, run_id) UNIQUE) — after
    a second add_source run two rows coexist per column. The resolver must
    return exactly the PINNED run's annotation, not an arbitrary survivor of
    the retired one-to-one ORM navigation.
    """
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )

    table = table_with_columns
    amount_col_id = next(c.column_id for c in table.columns if c.column_name == "amount")
    session.add(
        SemanticAnnotationDB(
            column_id=amount_col_id,
            run_id="sem-run-2",
            semantic_role="dimension",
            entity_type="category",
            business_name="WRONG RUN",
        )
    )
    session.commit()

    schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=_pins(table))
    amount_col = next(c for c in schema["tables"][0]["columns"] if c["column_name"] == "amount")
    assert amount_col["semantic"]["role"] == "measure"
    assert amount_col["semantic"]["business_name"] == "Transaction Amount"

    # Pin the OTHER run: its annotation surfaces instead.
    other = get_multi_table_schema_for_llm(
        session,
        [table.table_id],
        base_runs=BaseRunMap(semantic_runs={table.table_id: "sem-run-2"}),
    )
    amount_col = next(c for c in other["tables"][0]["columns"] if c["column_name"] == "amount")
    assert amount_col["semantic"]["business_name"] == "WRONG RUN"


def test_unpinned_table_has_no_annotations(session, table_with_columns):
    """A table absent from semantic_runs contributes no annotations (fail-closed)."""
    table = table_with_columns

    schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

    amount_col = next(c for c in schema["tables"][0]["columns"] if c["column_name"] == "amount")
    assert "semantic" not in amount_col


def test_get_multi_table_schema_for_llm_single_table(session, table_with_columns):
    """Test fetching multi-table schema with single table (no relationships)."""
    table = table_with_columns

    schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=_pins(table))

    assert "error" not in schema
    assert "tables" in schema
    assert len(schema["tables"]) == 1
    assert schema["tables"][0]["table_name"] == "transactions"
    assert schema["relationships"] == []

    # Check semantic annotations are included
    amount_col = next(c for c in schema["tables"][0]["columns"] if c["column_name"] == "amount")
    assert "semantic" in amount_col
    assert amount_col["semantic"]["role"] == "measure"
    assert amount_col["semantic"]["entity_type"] == "amount"


def test_table_grain_facts_scope_to_pinned_catalogue_run(session, table_with_columns):
    """Table-grain catalog facts (DAT-870) serve at the PINNED catalogue run only.

    ``TableEntity`` (role / grain / time-column semantics) is written by the
    begin_session catalogue run — the ``relationship_run_id`` pin — and
    ``TemporalColumnProfile`` (detected cadence) by the add_source per-table
    run — the ``semantic_runs`` pin. Both fail closed: an absent pin serves
    NO facts, never a cross-run read.
    """
    from datetime import UTC, datetime

    from dataraum.analysis.semantic.db_models import TableEntity
    from dataraum.analysis.temporal.db_models import TemporalColumnProfile

    table = table_with_columns
    date_col = next(c for c in table.columns if c.column_name == "transaction_id")
    session.add(
        TableEntity(
            table_id=table.table_id,
            run_id="run-current",
            table_role="periodic_snapshot",
            grain_columns=["transaction_id", "amount"],
            time_columns=[
                {
                    "column": "transaction_id",
                    "aspect": "period",
                    "role": "event",
                    "is_anchor": True,
                    "note": "The period label defining each snapshot row.",
                }
            ],
        )
    )
    # A STALE catalogue run's entity must never leak into a pinned read.
    session.add(
        TableEntity(
            table_id=table.table_id,
            run_id="run-stale",
            table_role="fact",
            grain_columns=["amount"],
        )
    )
    session.add(
        TemporalColumnProfile(
            profile_id="tp-1",
            column_id=date_col.column_id,
            run_id=SEM_RUN,
            profiled_at=datetime.now(UTC),
            min_timestamp=datetime(2020, 1, 1, tzinfo=UTC),
            max_timestamp=datetime(2021, 1, 1, tzinfo=UTC),
            span_days=366.0,
            detected_granularity="month",
            granularity_confidence=0.99,
        )
    )
    session.commit()

    pinned = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=_pins(table))
    t = pinned["tables"][0]
    assert t["table_role"] == "periodic_snapshot"
    assert t["grain_columns"] == ["transaction_id", "amount"]
    tid_col = next(c for c in t["columns"] if c["column_name"] == "transaction_id")
    assert tid_col["time"] == {
        "role": "event",
        "aspect": "period",
        "note": "The period label defining each snapshot row.",
    }
    assert tid_col["granularity"] == "month"

    # No catalogue pin ⇒ no role/grain/time facts (fail-closed) — the
    # granularity rides the still-pinned semantic run and stays.
    unpinned = get_multi_table_schema_for_llm(
        session, [table.table_id], base_runs=_pins(table, relationship_run_id=None)
    )
    t = unpinned["tables"][0]
    assert "table_role" not in t
    assert "grain_columns" not in t
    assert "time" not in next(c for c in t["columns"] if c["column_name"] == "transaction_id")

    # No semantic pin ⇒ no granularity either.
    bare = get_multi_table_schema_for_llm(
        session,
        [table.table_id],
        base_runs=BaseRunMap(relationship_run_id="run-current", semantic_runs={}),
    )
    t = bare["tables"][0]
    assert t["table_role"] == "periodic_snapshot"
    assert "granularity" not in next(
        c for c in t["columns"] if c["column_name"] == "transaction_id"
    )


def test_get_multi_table_schema_for_llm_empty_list(session):
    """Test fetching multi-table schema with empty list."""
    schema = get_multi_table_schema_for_llm(session, [], base_runs=BaseRunMap())

    assert "error" in schema
    assert "No tables" in schema["error"]


def test_get_multi_table_schema_for_llm_nonexistent_tables(session):
    """Test fetching multi-table schema with nonexistent table IDs."""
    schema = get_multi_table_schema_for_llm(session, ["nonexistent-id"], base_runs=BaseRunMap())

    assert "error" in schema


def test_missing_lake_table_keeps_schema_without_row_count(
    session, duckdb_conn, table_with_columns
):
    """DAT-439 item-4 pin: the row-count swallow holds.

    The table's ``duckdb_path`` does not exist in the lake — COUNT(*) raises,
    the resolver swallows it (row counts are LLM-context garnish) and still
    assembles the schema, just without ``row_count``. The REAL failure
    surfaces downstream at bind: EXPLAIN fails → bind ERROR → the artifact
    stays declared with the reason (pinned by
    test_bind_missing_lake_table_fails_explain in the agent tests).
    """
    table = table_with_columns  # duckdb_path="typed_transactions", not in duckdb_conn

    schema = get_multi_table_schema_for_llm(
        session, [table.table_id], duckdb_conn=duckdb_conn, base_runs=_pins(table)
    )

    assert "error" not in schema
    assert len(schema["tables"]) == 1
    assert "row_count" not in schema["tables"][0]
    # The schema is otherwise complete — columns intact for the LLM prompt.
    assert {c["column_name"] for c in schema["tables"][0]["columns"]} == {
        "transaction_id",
        "amount",
        "account_type",
    }


def test_partially_missing_table_ids_returns_existing(session, table_with_columns):
    """A requested-but-missing table id shrinks the schema loudly (warning),
    never silently: the existing tables still resolve (DAT-439 sweep)."""
    table = table_with_columns

    schema = get_multi_table_schema_for_llm(
        session, [table.table_id, "nonexistent-id"], base_runs=_pins(table)
    )

    assert "error" not in schema
    assert [t["table_id"] for t in schema["tables"]] == [table.table_id]


def test_table_without_duckdb_path_is_excluded(session, table_with_columns):
    """A table with no lake path cannot be validated against — it is excluded
    from the LLM schema (with a warning, DAT-439 sweep); the rest survive."""
    from dataraum.storage import Source, Table

    good = table_with_columns
    source = session.query(Source).first()
    pathless = Table(
        source_id=source.source_id,
        table_name="pathless",
        layer="typed",
        duckdb_path=None,
    )
    session.add(pathless)
    session.commit()

    schema = get_multi_table_schema_for_llm(
        session, [good.table_id, pathless.table_id], base_runs=_pins(good)
    )

    assert "error" not in schema
    assert [t["table_name"] for t in schema["tables"]] == ["transactions"]


class TestFormatMultiTableSchemaForPrompt:
    """Tests for formatting multi-table schema as prompt text."""

    def test_format_multi_table_basic(self):
        """Test formatting a basic multi-table schema."""
        schema = {
            "tables": [
                {
                    "table_name": "orders",
                    "duckdb_path": "typed_orders",
                    "columns": [
                        {"column_name": "order_id", "data_type": "VARCHAR"},
                        {"column_name": "customer_id", "data_type": "VARCHAR"},
                    ],
                },
                {
                    "table_name": "customers",
                    "duckdb_path": "typed_customers",
                    "columns": [
                        {"column_name": "customer_id", "data_type": "VARCHAR"},
                        {"column_name": "name", "data_type": "VARCHAR"},
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<tables>" in result
        assert 'name="orders"' in result
        assert 'name="customers"' in result
        assert 'duckdb_path="typed_orders"' in result
        assert 'name="order_id"' in result
        assert 'name="customer_id"' in result

    def test_format_multi_table_with_relationships(self):
        """Test formatting multi-table schema with relationships."""
        schema = {
            "tables": [
                {
                    "table_name": "orders",
                    "duckdb_path": "typed_orders",
                    "columns": [{"column_name": "customer_id", "data_type": "VARCHAR"}],
                },
                {
                    "table_name": "customers",
                    "duckdb_path": "typed_customers",
                    "columns": [{"column_name": "customer_id", "data_type": "VARCHAR"}],
                },
            ],
            "relationships": [
                {
                    "from_table": "orders",
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "customer_id",
                    "relationship_type": "foreign_key",
                    "cardinality": "many-to-one",
                    "confidence": 0.92,
                },
            ],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<relationships>" in result
        assert 'from_table="orders"' in result
        assert 'from_column="customer_id"' in result
        assert 'to_table="customers"' in result
        assert 'type="foreign_key"' in result
        assert 'cardinality="many-to-one"' in result
        assert 'confidence="92%"' in result

    def test_format_multi_table_with_semantic_annotations(self):
        """Test formatting multi-table schema with semantic annotations."""
        schema = {
            "tables": [
                {
                    "table_name": "accounts",
                    "duckdb_path": "typed_accounts",
                    "columns": [
                        {
                            "column_name": "balance",
                            "data_type": "DECIMAL",
                            "semantic": {
                                "role": "measure",
                                "entity_type": "amount",
                                "business_name": "Account Balance",
                            },
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format with attributes
        assert 'entity="amount"' in result
        assert 'role="measure"' in result
        assert 'business_name="Account Balance"' in result

    def test_format_multi_table_with_error(self):
        """Test formatting an error schema."""
        schema = {"error": "No tables found"}

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<error>No tables found</error>" in result

    def test_format_escapes_quotes_in_prose_attributes(self):
        """LLM-authored prose with embedded quotes must not terminate the attribute."""
        schema = {
            "tables": [
                {
                    "table_name": "t",
                    "duckdb_path": "t",
                    "columns": [
                        {
                            "column_name": "d",
                            "data_type": "DATE",
                            "semantic": {
                                "role": "dimension",
                                "meaning": 'the "as of" moment',
                            },
                            "time": {"note": 'the "as of" label'},
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        assert 'time_note="the &quot;as of&quot; label"' in result
        assert 'meaning="the &quot;as of&quot; moment"' in result
        assert '"as of"' not in result

    def test_format_table_grain_and_time_facts(self):
        """DAT-870: role/grain render as table attrs, time facts as column attrs."""
        schema = {
            "tables": [
                {
                    "table_name": "snapshots",
                    "duckdb_path": "typed_snapshots",
                    "table_role": "periodic_snapshot",
                    "grain_columns": ["entity_id", "period"],
                    "columns": [
                        {
                            "column_name": "period",
                            "data_type": "DATE",
                            "granularity": "month",
                            "time": {
                                "role": "event",
                                "aspect": "period",
                                "note": "The period label defining each snapshot row.",
                            },
                        },
                        {"column_name": "level", "data_type": "DECIMAL"},
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        assert 'role="periodic_snapshot"' in result
        assert 'grain="entity_id, period"' in result
        assert 'granularity="month"' in result
        assert 'time_role="event"' in result
        assert 'time_aspect="period"' in result
        assert 'time_note="The period label defining each snapshot row."' in result
        # A plain column renders without any time attributes.
        level_line = next(line for line in result.splitlines() if 'name="level"' in line)
        assert "time_" not in level_line
        assert "granularity" not in level_line
