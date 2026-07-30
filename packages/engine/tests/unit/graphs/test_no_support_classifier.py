"""Evidence-backed no-support classification (DAT-658).

``classify_no_support`` turns the verifier's one measurable fact ("aggregated to
NULL") into a typed cause where the evidence lives: the DuckDB connection the
extract ran on, its own clause parts, its declared grounding basis, and the
served value-set reference. Each evidence combination maps to exactly one class,
and absence is never claimed without a complete served enumeration to back it —
the high-cardinality/search_values gap classifies honestly as revisable.

``FailedSnippetProvenance`` is the persistence chokepoint (provenance is JSON —
no DB CHECK applies), so its pairing validator is pinned here too: a class needs
its evidence, and both qualify only the verifier's rejection.
"""

from __future__ import annotations

import duckdb
import pytest
from pydantic import ValidationError

from dataraum.graphs.agent import classify_no_support
from dataraum.graphs.models import (
    FailedSnippetProvenance,
    NoSupportClass,
    SnippetFailureMode,
)


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """The relation the probes run against: served categories Rent/Salaries only,
    with one row whose measure operand is NULL (the one-sided shape)."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE ledger (category VARCHAR, amount DOUBLE)")
    c.execute("INSERT INTO ledger VALUES ('Rent', 100.0), ('Salaries', NULL)")
    return c


def _parts(where: list[str], relation: str | None = "ledger") -> dict[str, object]:
    return {
        "select": [{"expr": "SUM(amount)", "alias": "value"}],
        "from": [relation] if relation else [],
        "where": where,
    }


def _fall_loud_parts() -> dict[str, object]:
    """The system-composed fall-loud shape, exactly as ``extract_parts_dict("NULL",
    None, [])`` persists it — the ONLY shape the graph-level abstain applies to."""
    return {"select": [{"expr": "NULL", "alias": "value"}], "from": [], "where": []}


def _basis(members: list[tuple[str, str]]) -> dict[str, object]:
    """The persisted MAP shape (``HealthySnippetProvenance``) both paths feed."""
    return {
        "cost_of_goods_sold": {
            "measure_columns": ["amount"],
            "filter_columns": sorted({column for column, _ in members}),
            "filter": "",
            "filter_members": [{"column": c, "value": v} for c, v in members],
        }
    }


_SERVED = {"category": {"Rent", "Salaries"}}


class TestClassifier:
    def test_composition_abstain_short_circuits_everything(self) -> None:
        """The DAT-893 SELECT NULL fixture: the system composed the fall-loud shape,
        so the abstain reason IS the cause — no probe runs (there is no relation to
        probe), and the class is the composition one, never an absence claim."""
        finding = classify_no_support(
            duckdb.connect(":memory:"),
            parts=_fall_loud_parts(),
            column_mappings_basis=None,
            served_values={},
            composition_abstain="SENTINEL_ANCHOR_MISMATCH",
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.COMPOSITION_ABSTAINED
        assert finding.evidence == "SENTINEL_ANCHOR_MISMATCH"

    def test_graph_level_abstain_ignored_for_a_step_with_real_parts(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The abstain reason is GRAPH-level; only the step whose own parts ARE the
        system's fall-loud shape may claim it. A step that authored real SQL in a
        code object carrying an abstain (a hypothetical multi-extract compose)
        classifies by measurement — structural, not reliant on ``execute`` failing
        loud on multi-extract graphs."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'Salaries'"]),
            column_mappings_basis=_basis([("category", "Salaries")]),
            served_values=_SERVED,
            composition_abstain="SENTINEL_OTHER_LEAFS_ABSTAIN",
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.OPERAND_ALL_NULL
        assert "SENTINEL_OTHER_LEAFS_ABSTAIN" not in finding.evidence

    def test_rows_matched_is_operand_all_null(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Rows match the filter but the aggregate was NULL ⇒ the operand is entirely
        NULL over them — DAT-699's second case, now measured instead of enumerated."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'Salaries'"]),
            column_mappings_basis=_basis([("category", "Salaries")]),
            served_values=_SERVED,
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.OPERAND_ALL_NULL
        assert "1 row(s)" in finding.evidence

    def test_zero_rows_with_absent_declared_value_is_concept_absent(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Every declared family value missing from its column's COMPLETE served
        enumeration ⇒ no predicate over the served values can ever select the
        concept. The evidence names the value AND the enumeration, so the next
        authoring can verify the verdict against the Value sets it is served."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'COGS'"]),
            column_mappings_basis=_basis([("category", "COGS")]),
            served_values=_SERVED,
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.CONCEPT_ABSENT
        assert "'COGS'" in finding.evidence
        assert "Rent" in finding.evidence and "Salaries" in finding.evidence

    def test_zero_rows_with_a_served_declared_value_is_predicate_class(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A declared value that IS served means the concept exists — the predicate
        COMPOSITION excluded every row (a system scope, a second conjunct, a period
        bound), which a revision can fix. Never absence."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'Rent'", "amount > 1000"]),
            column_mappings_basis=_basis([("category", "Rent")]),
            served_values=_SERVED,
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.PREDICATE_MATCHED_NO_ROWS
        assert "ARE served" in finding.evidence

    def test_zero_rows_on_an_unscreened_column_stays_revisable(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The known un-screened gap: a high-cardinality (search_values) column has
        no complete served enumeration, so absence cannot be evidenced — classify
        honestly as predicate-zero-rows, never claim what cannot be backed."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'COGS'"]),
            column_mappings_basis=_basis([("category", "COGS")]),
            served_values={},  # no complete enumeration covers 'category'
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.PREDICATE_MATCHED_NO_ROWS
        assert "could not be screened" in finding.evidence

    def test_zero_rows_without_declared_members_stays_revisable(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """No declared family values ⇒ nothing to screen absence WITH — an empty
        declaration is not evidence of absence (the vacuous-truth trap)."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'COGS'"]),
            column_mappings_basis=_basis([]),
            served_values=_SERVED,
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.PREDICATE_MATCHED_NO_ROWS
        assert "no filter values were declared" in finding.evidence

    def test_probe_and_composes_the_where_parts(self, conn: duckdb.DuckDBPyConnection) -> None:
        """The probe applies the SAME AND-composed predicate the extract executed
        (compose_where_predicate is the single source): a row matching only ONE
        conjunct must not count, else a zero-row extract misclassifies as
        operand_all_null."""
        finding = classify_no_support(
            conn,
            parts=_parts(["category = 'Rent'", "amount < 0"]),
            column_mappings_basis=_basis([("category", "Rent")]),
            served_values=_SERVED,
        )
        assert finding is not None
        assert finding.reason_class is NoSupportClass.PREDICATE_MATCHED_NO_ROWS

    def test_fall_loud_shape_is_unclassifiable(self) -> None:
        """The model's own fall-loud (no relation) has no probe to run and no claim
        to make — None keeps the verifier's honest possibility-space message."""
        assert (
            classify_no_support(
                duckdb.connect(":memory:"),
                parts=_fall_loud_parts(),
                column_mappings_basis=None,
                served_values=_SERVED,
            )
            is None
        )

    def test_multi_relation_parts_are_unclassifiable(self, conn: duckdb.DuckDBPyConnection) -> None:
        """No engine path composes a multi-FROM extract today, but the parts schema
        allows one — and the single-relation COUNT probe cannot mirror its scan (a
        WHERE touching only the first relation's columns would count unjoined rows
        and misclassify a zero-support join as operand_all_null). No faithful probe
        → no claim."""
        parts = {
            "select": [{"expr": "SUM(amount)", "alias": "value"}],
            "from": ["ledger", "other_rel"],
            "where": ["category = 'Rent'"],
        }
        assert (
            classify_no_support(
                conn,
                parts=parts,
                column_mappings_basis=_basis([("category", "Rent")]),
                served_values=_SERVED,
            )
            is None
        )

    def test_missing_parts_is_unclassifiable(self) -> None:
        """A pre-parts snippet (DAT-671 predates it) carries no clause parts."""
        assert (
            classify_no_support(
                duckdb.connect(":memory:"),
                parts=None,
                column_mappings_basis=None,
                served_values=_SERVED,
            )
            is None
        )

    def test_failed_probe_degrades_to_none(self, conn: duckdb.DuckDBPyConnection) -> None:
        """A broken probe (dropped view, odd relation) is best-effort context lost,
        never a new failure — and never a guessed class."""
        assert (
            classify_no_support(
                conn,
                parts=_parts([], relation="no_such_relation"),
                column_mappings_basis=None,
                served_values=_SERVED,
            )
            is None
        )


class TestFailedProvenancePairing:
    """The additivity pairing discipline at the pydantic chokepoint."""

    def test_class_with_evidence_on_verifier_rejection_is_valid(self) -> None:
        prov = FailedSnippetProvenance(
            failure_mode=SnippetFailureMode.VERIFIER_REJECTED,
            failure_reason="no support",
            no_support_class=NoSupportClass.CONCEPT_ABSENT,
            no_support_evidence="'COGS' is not among the complete served values",
        )
        dumped = prov.model_dump(mode="json")
        assert dumped["no_support_class"] == "concept_absent"

    def test_unclassified_row_is_valid(self) -> None:
        """Pre-DAT-658 rows and evidence-gathering failures carry no class."""
        FailedSnippetProvenance(
            failure_mode=SnippetFailureMode.EXECUTION_FAILED, failure_reason="boom"
        )

    def test_class_without_evidence_raises(self) -> None:
        with pytest.raises(ValidationError, match="must carry its evidence"):
            FailedSnippetProvenance(
                failure_mode=SnippetFailureMode.VERIFIER_REJECTED,
                failure_reason="no support",
                no_support_class=NoSupportClass.OPERAND_ALL_NULL,
            )

    def test_evidence_without_class_raises(self) -> None:
        with pytest.raises(ValidationError, match="without a class"):
            FailedSnippetProvenance(
                failure_mode=SnippetFailureMode.VERIFIER_REJECTED,
                failure_reason="no support",
                no_support_evidence="floating evidence",
            )

    def test_class_on_a_non_verifier_mode_raises(self) -> None:
        """The DAT-893 invariant generalized: on any other mode the grounding failed
        for its own reason — a cause class there would contradict the mode."""
        with pytest.raises(ValidationError, match="verifier"):
            FailedSnippetProvenance(
                failure_mode=SnippetFailureMode.PROVENANCE_INVALID,
                failure_reason="contract violated",
                no_support_class=NoSupportClass.CONCEPT_ABSENT,
                no_support_evidence="evidence",
            )
