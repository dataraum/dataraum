"""Tests for teach MCP tool dispatch.

Each teach type is tested for:
- Correct write path (YAML file or DB model)
- Idempotency (teaching twice replaces, doesn't duplicate)
- DataFix persistence
- Error handling
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import yaml
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.mcp.teach import handle_teach
from dataraum.pipeline.fixes.models import DataFix
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


def _id() -> str:
    return str(uuid4())


# ---------------------------------------------------------------------------
# Fixtures — tables use the naming convention the resolvers expect
# ---------------------------------------------------------------------------


def _setup_typed_tables(
    session: Session,
    tables: dict[str, list[str]] | None = None,
) -> tuple[str, dict[str, tuple[str, list[tuple[str, str]]]]]:
    """Create Source + typed Tables + Columns.

    Args:
        tables: {base_name: [col_names]}. Defaults to {"orders": ["id", "amount", "region"]}.

    Returns:
        (source_id, {base_name: (table_id, [(col_id, col_name)])})
    """
    if tables is None:
        tables = {"orders": ["id", "amount", "region"]}

    source_id = _id()
    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))

    result: dict[str, tuple[str, list[tuple[str, str]]]] = {}
    for base_name, col_names in tables.items():
        table_id = _id()
        # Real DB: table_name has no typed_ prefix, layer distinguishes
        # raw/quarantine/typed. duckdb_path carries the typed_ prefix.
        session.add(
            Table(
                table_id=table_id,
                source_id=source_id,
                table_name=base_name,
                layer="typed",
                duckdb_path=f"typed_{base_name}",
                row_count=100,
            )
        )
        col_ids = []
        for i, name in enumerate(col_names):
            col_id = _id()
            col_ids.append((col_id, name))
            session.add(
                Column(
                    column_id=col_id,
                    table_id=table_id,
                    column_name=name,
                    column_position=i,
                    resolved_type="BIGINT" if name == "amount" else "VARCHAR",
                )
            )
        result[base_name] = (table_id, col_ids)

    session.flush()
    return source_id, result


def _make_config_root(tmp_path: Path) -> Path:
    """Create a config root with empty config files for config teaches."""
    config_root = tmp_path / "config"
    config_root.mkdir()
    # Ontology
    verticals_dir = config_root / "verticals" / "_adhoc"
    verticals_dir.mkdir(parents=True)
    (verticals_dir / "ontology.yaml").write_text("name: _adhoc\nversion: '1.0.0'\nconcepts: []\n")
    # Cycles
    (verticals_dir / "cycles.yaml").write_text("cycle_types: {}\n")
    # Validations dir
    (verticals_dir / "validations").mkdir()
    # Typing
    phases_dir = config_root / "phases"
    phases_dir.mkdir()
    (phases_dir / "typing.yaml").write_text("date_patterns: []\nidentifier_patterns: []\n")
    # Null values
    (config_root / "null_values.yaml").write_text("missing_indicators: []\n")
    return config_root


# ---------------------------------------------------------------------------
# Config teaches — concept, validation, cycle, type_pattern, null_value
# ---------------------------------------------------------------------------


class TestTeachConcept:
    def test_writes_concept_to_ontology(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "concept",
            {
                "name": "revenue",
                "indicators": ["revenue", "sales", "income"],
                "description": "Total revenue",
                "typical_role": "measure",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert result["type"] == "concept"
        assert "teaching_id" in result
        assert "semantic" in result["measurement_hint"]

        ontology = yaml.safe_load(
            (config_root / "verticals" / "_adhoc" / "ontology.yaml").read_text()
        )
        assert len(ontology["concepts"]) == 1
        assert ontology["concepts"][0]["name"] == "revenue"
        assert ontology["concepts"][0]["indicators"] == ["revenue", "sales", "income"]

    def test_idempotent_replaces_existing(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        # Teach once
        handle_teach(
            "concept",
            {"name": "revenue", "indicators": ["rev"]},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )
        # Teach again with updated indicators
        handle_teach(
            "concept",
            {"name": "revenue", "indicators": ["revenue", "total_revenue"]},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        ontology = yaml.safe_load(
            (config_root / "verticals" / "_adhoc" / "ontology.yaml").read_text()
        )
        assert len(ontology["concepts"]) == 1
        assert ontology["concepts"][0]["indicators"] == ["revenue", "total_revenue"]

    def test_preserves_other_concepts(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        # Pre-populate with an existing concept
        ontology_path = config_root / "verticals" / "_adhoc" / "ontology.yaml"
        ontology_path.write_text(
            "name: _adhoc\nversion: '1.0.0'\nconcepts:\n- name: existing\n  indicators: [foo]\n"
        )

        handle_teach(
            "concept",
            {"name": "revenue", "indicators": ["rev"]},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        ontology = yaml.safe_load(ontology_path.read_text())
        names = [c["name"] for c in ontology["concepts"]]
        assert "existing" in names
        assert "revenue" in names
        assert len(ontology["concepts"]) == 2


class TestTeachValidation:
    def test_writes_validation_spec_file(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "validation",
            {
                "validation_id": "custom_check",
                "name": "Custom Check",
                "description": "Checks custom rule",
                "sql_hints": "SELECT COUNT(*) FROM orders",
                "expected_outcome": "Should be > 0",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "validation" in result["measurement_hint"]

        spec_path = config_root / "verticals" / "_adhoc" / "validations" / "custom_check.yaml"
        spec = yaml.safe_load(spec_path.read_text())
        assert spec["name"] == "Custom Check"
        assert spec["version"] == "1.0"
        assert spec["validation_id"] == "custom_check"

    def test_idempotent_overwrites(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        for desc in ("v1", "v2"):
            handle_teach(
                "validation",
                {
                    "validation_id": "my_check",
                    "name": "My Check",
                    "description": desc,
                    "sql_hints": "SELECT 1",
                    "expected_outcome": "pass",
                },
                source_id=source_id,
                session=session,
                vertical="_adhoc",
                config_root=config_root,
                session_id=baseline_session_id(),
            )

        spec = yaml.safe_load(
            (config_root / "verticals" / "_adhoc" / "validations" / "my_check.yaml").read_text()
        )
        assert spec["description"] == "v2"


class TestTeachCycle:
    def test_merges_cycle_into_config(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "cycle",
            {
                "cycle_id": "order_to_cash",
                "description": "Revenue cycle",
                "typical_stages": [
                    {"name": "Ordered", "order": 1, "indicators": ["new"]},
                    {"name": "Paid", "order": 2, "indicators": ["paid"]},
                ],
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "business_cycles" in result["measurement_hint"]

        cycles = yaml.safe_load((config_root / "verticals" / "_adhoc" / "cycles.yaml").read_text())
        assert "order_to_cash" in cycles["cycle_types"]
        assert cycles["cycle_types"]["order_to_cash"]["description"] == "Revenue cycle"

    def test_idempotent_overwrites_cycle(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        for desc in ("v1", "v2"):
            handle_teach(
                "cycle",
                {
                    "cycle_id": "o2c",
                    "description": desc,
                    "typical_stages": [{"name": "Start", "order": 1, "indicators": ["new"]}],
                },
                source_id=source_id,
                session=session,
                vertical="_adhoc",
                config_root=config_root,
                session_id=baseline_session_id(),
            )

        cycles = yaml.safe_load((config_root / "verticals" / "_adhoc" / "cycles.yaml").read_text())
        assert cycles["cycle_types"]["o2c"]["description"] == "v2"


class TestTeachTypePattern:
    def test_writes_pattern_to_typing_config(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "type_pattern",
            {
                "name": "custom_date",
                "pattern": r"^\d{2}/\d{2}/\d{4}$",
                "inferred_type": "DATE",
                "pattern_section": "date_patterns",
                "examples": ["15/01/2024"],
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "typing" in result["measurement_hint"]

        typing_config = yaml.safe_load((config_root / "phases" / "typing.yaml").read_text())
        assert len(typing_config["date_patterns"]) == 1
        assert typing_config["date_patterns"][0]["name"] == "custom_date"

    def test_idempotent_replaces_existing(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        for pattern in [r"^\d{2}/\d{2}$", r"^\d{4}-\d{2}$"]:
            handle_teach(
                "type_pattern",
                {
                    "name": "custom_date",
                    "pattern": pattern,
                    "inferred_type": "DATE",
                    "pattern_section": "date_patterns",
                },
                source_id=source_id,
                session=session,
                vertical="_adhoc",
                config_root=config_root,
                session_id=baseline_session_id(),
            )

        typing_config = yaml.safe_load((config_root / "phases" / "typing.yaml").read_text())
        assert len(typing_config["date_patterns"]) == 1
        assert typing_config["date_patterns"][0]["pattern"] == r"^\d{4}-\d{2}$"


class TestTeachNullValue:
    def test_writes_null_to_missing_indicators(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "null_value",
            {"value": "TBD", "case_sensitive": False, "description": "To be determined"},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "import" in result["measurement_hint"]

        null_config = yaml.safe_load((config_root / "null_values.yaml").read_text())
        assert len(null_config["missing_indicators"]) == 1
        assert null_config["missing_indicators"][0]["value"] == "TBD"

    def test_idempotent_replaces_existing(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        for desc in ("v1", "v2"):
            handle_teach(
                "null_value",
                {"value": "TBD", "description": desc},
                source_id=source_id,
                session=session,
                vertical="_adhoc",
                config_root=config_root,
                session_id=baseline_session_id(),
            )

        null_config = yaml.safe_load((config_root / "null_values.yaml").read_text())
        assert len(null_config["missing_indicators"]) == 1
        assert null_config["missing_indicators"][0]["description"] == "v2"

    def test_preserves_other_entries(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        # Pre-populate
        (config_root / "null_values.yaml").write_text(
            "missing_indicators:\n- value: EXISTING\n  case_sensitive: false\n"
        )

        handle_teach(
            "null_value",
            {"value": "TBD"},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        null_config = yaml.safe_load((config_root / "null_values.yaml").read_text())
        values = [e["value"] for e in null_config["missing_indicators"]]
        assert "EXISTING" in values
        assert "TBD" in values
        assert len(null_config["missing_indicators"]) == 2


# ---------------------------------------------------------------------------
# Metadata teaches — concept_property, relationship, explanation
# ---------------------------------------------------------------------------


class TestTeachConceptProperty:
    def test_patches_semantic_annotation(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(session)
        col_id = tables["orders"][1][1][0]  # amount col_id

        session.add(
            SemanticAnnotation(
                annotation_id=_id(),
                column_id=col_id,
                semantic_role="attribute",
                annotation_source="llm",
            )
        )
        session.flush()

        result = handle_teach(
            "concept_property",
            {"field_updates": {"semantic_role": "measure", "business_concept": "revenue"}},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target="orders.amount",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "measurement_hint" not in result

        ann = session.query(SemanticAnnotation).filter_by(column_id=col_id).one()
        assert ann.semantic_role == "measure"
        assert ann.business_concept == "revenue"
        assert ann.annotation_source == "teach"

    def test_persists_datafix(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(session)
        col_id = tables["orders"][1][1][0]

        session.add(
            SemanticAnnotation(
                annotation_id=_id(),
                column_id=col_id,
                semantic_role="attribute",
                annotation_source="llm",
            )
        )
        session.flush()

        handle_teach(
            "concept_property",
            {"field_updates": {"semantic_role": "measure"}},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target="orders.amount",
            session_id=baseline_session_id(),
        )

        fixes = session.query(DataFix).filter_by(source_id=source_id).all()
        assert len(fixes) == 1
        assert fixes[0].action == "concept_property"
        assert fixes[0].target == "metadata"
        assert fixes[0].status == "applied"

    def test_fails_when_annotation_missing(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)

        result = handle_teach(
            "concept_property",
            {"field_updates": {"semantic_role": "measure"}},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target="orders.amount",
            session_id=baseline_session_id(),
        )

        assert "error" in result
        assert "No SemanticAnnotation found" in result["error"]


class TestTeachRelationship:
    def test_confirms_existing_relationship(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(
            session, {"orders": ["id", "amount"], "customers": ["order_id", "name"]}
        )
        orders_tid, orders_cols = tables["orders"]
        cust_tid, cust_cols = tables["customers"]

        rel_id = _id()
        session.add(
            Relationship(
                relationship_id=rel_id,
                from_table_id=orders_tid,
                from_column_id=orders_cols[0][0],  # id
                to_table_id=cust_tid,
                to_column_id=cust_cols[0][0],  # order_id
                relationship_type="foreign_key",
                confidence=0.7,
                detection_method="candidate",
                is_confirmed=False,
            )
        )
        session.flush()

        result = handle_teach(
            "relationship",
            {
                "from_table": "orders",
                "from_column": "id",
                "to_table": "customers",
                "to_column": "order_id",
                "cardinality": "one-to-many",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"

        rel = session.query(Relationship).filter_by(relationship_id=rel_id).one()
        assert rel.is_confirmed is True
        assert rel.confirmed_by == "teach"
        assert rel.cardinality == "one-to-many"

    def test_creates_new_relationship(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(
            session, {"invoices": ["invoice_id", "vid"], "payments": ["payment_id", "invoice_id"]}
        )

        result = handle_teach(
            "relationship",
            {
                "from_table": "invoices",
                "from_column": "invoice_id",
                "to_table": "payments",
                "to_column": "invoice_id",
                "relationship_type": "foreign_key",
                "cardinality": "one-to-many",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"

        rel = session.query(Relationship).one()
        assert rel.from_column_id == tables["invoices"][1][0][0]  # invoices.invoice_id
        assert rel.to_column_id == tables["payments"][1][1][0]  # payments.invoice_id
        assert rel.relationship_type == "foreign_key"
        assert rel.cardinality == "one-to-many"
        assert rel.is_confirmed is True
        assert rel.confirmed_by == "teach"
        assert rel.detection_method == "manual"

    def test_idempotent_second_teach_updates(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(session, {"orders": ["id"], "items": ["order_id"]})

        params = {
            "from_table": "orders",
            "from_column": "id",
            "to_table": "items",
            "to_column": "order_id",
            "relationship_type": "foreign_key",
            "cardinality": "one-to-many",
        }
        handle_teach(
            "relationship",
            params,
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )
        result = handle_teach(
            "relationship",
            params,
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert session.query(Relationship).count() == 1

    def test_creation_fails_for_nonexistent_column(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)

        result = handle_teach(
            "relationship",
            {
                "from_table": "orders",
                "from_column": "id",
                "to_table": "nonexistent",
                "to_column": "order_id",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )

        assert "error" in result


class TestTeachExplanation:
    def test_creates_marker_datafix(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)

        result = handle_teach(
            "explanation",
            {
                "dimension": "null_semantics",
                "context": "Amount nulls are legitimate — they represent free samples",
                "evidence_sql": "SELECT * FROM orders WHERE amount IS NULL",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target="orders.amount",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        assert "measurement_hint" not in result

        fixes = session.query(DataFix).filter_by(source_id=source_id).all()
        assert len(fixes) == 1
        assert fixes[0].action == "explanation"
        assert fixes[0].target == "metadata"
        assert fixes[0].payload["context"].startswith("Amount nulls")
        assert fixes[0].payload["evidence_sql"] is not None

    def test_table_scoped_explanation(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)

        result = handle_teach(
            "explanation",
            {"dimension": "relationships", "context": "This table is self-referencing"},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target="orders",
            session_id=baseline_session_id(),
        )

        assert result["status"] == "applied"
        fix = session.query(DataFix).filter_by(source_id=source_id).one()
        assert fix.table_name == "orders"
        assert fix.column_name is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestTeachErrors:
    def test_rejects_unknown_type(self, session: Session) -> None:
        result = handle_teach(
            "nonexistent",
            {},
            source_id="fake",
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )
        assert "error" in result
        assert "Unknown teach type" in result["error"]

    def test_rejects_invalid_params(self, session: Session) -> None:
        result = handle_teach(
            "concept",
            {"bad_field": "value"},
            source_id="fake",
            session=session,
            vertical="_adhoc",
            session_id=baseline_session_id(),
        )
        assert "error" in result
        assert "Invalid params" in result["error"]

    def test_concept_property_requires_target(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)
        result = handle_teach(
            "concept_property",
            {"field_updates": {"semantic_role": "measure"}},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            target=None,
            session_id=baseline_session_id(),
        )
        assert "error" in result
        assert "requires a target" in result["error"]

    def test_config_teach_fails_without_config_root(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)
        result = handle_teach(
            "concept",
            {"name": "revenue", "indicators": ["revenue"]},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=None,
            session_id=baseline_session_id(),
        )
        assert "error" in result


# ---------------------------------------------------------------------------
# DataFix persistence for config teaches
# ---------------------------------------------------------------------------


class TestConfigTeachDataFix:
    def test_config_teach_creates_datafix_record(self, session: Session, tmp_path: Path) -> None:
        source_id, _ = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        handle_teach(
            "null_value",
            {"value": "N/A"},
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
            session_id=baseline_session_id(),
        )

        fixes = session.query(DataFix).filter_by(source_id=source_id).all()
        assert len(fixes) == 1
        assert fixes[0].action == "null_value"
        assert fixes[0].target == "config"
        assert fixes[0].status == "applied"
