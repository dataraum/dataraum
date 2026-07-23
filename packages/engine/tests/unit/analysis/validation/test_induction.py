"""Agentic validation induction — contract, membership, repair, drop (DAT-735).

Pins the induction seam: the served-graph membership vocabulary, the
provenance-contract-v2 membership validation (reject fabricated references), the
single repair turn, and the drop of any proposal still grounded on a fabricated
entity after repair. The constrained-decoding contract is checked statically here
(the LIVE probe proves it compiles — asserted separately in the report).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dataraum.analysis.semantic.ontology import OntologyConvention
from dataraum.analysis.validation.induction import (
    InducedValidation,
    InducedValidations,
    ValidationInductionAgent,
    _is_clean,
    _render_conventions,
    _to_spec,
    membership_violations,
    served_membership,
)
from dataraum.analysis.validation.models import ValidationSeverity
from dataraum.core.models.base import Result
from dataraum.graphs.context import (
    ColumnContext,
    ConceptContext,
    GraphExecutionContext,
    TableContext,
)
from dataraum.llm.providers.base import ConversationResponse


def _induced(validation_id: str = "v1", **overrides: Any) -> InducedValidation:
    fields: dict[str, Any] = {
        "validation_id": validation_id,
        "name": validation_id,
        "description": "check",
        "category": "data_quality",
        "severity": "warning",
        "check_type": "constraint",
        "tolerance": 0.01,
        "guidance": "ground it",
        "expected_outcome": "",
        "relevant_cycles": [],
        "relevant_conventions": [],
        "referenced_tables": [],
        "referenced_columns": [],
        "referenced_concepts": [],
    }
    fields.update(overrides)
    return InducedValidation(**fields)


def _context() -> GraphExecutionContext:
    return GraphExecutionContext(
        tables=[
            TableContext(
                table_id="t1",
                table_name="journal_entries",
                duckdb_name="src__journal_entries",
                columns=[
                    ColumnContext(
                        column_id="c1", column_name="debit", table_name="journal_entries"
                    ),
                    ColumnContext(
                        column_id="c2", column_name="credit", table_name="journal_entries"
                    ),
                ],
            )
        ],
        concepts=[ConceptContext(name="debit"), ConceptContext(name="credit")],
    )


# --- pure functions ----------------------------------------------------------


def test_served_membership_accepts_bare_and_qualified() -> None:
    m = served_membership(_context(), conventions=["sign_natural_balance"])
    assert "journal_entries" in m.tables
    assert "src__journal_entries" in m.tables  # both forms
    assert "debit" in m.columns  # bare
    assert "journal_entries.debit" in m.columns  # qualified (logical)
    assert "src__journal_entries.credit" in m.columns  # qualified (duckdb)
    assert m.concepts == {"debit", "credit"}
    # A _norm(id) → canonical-id map (the save-side canonicalization home).
    assert m.conventions == {"sign_natural_balance": "sign_natural_balance"}
    # No conventions served (fresh vertical) ⇒ empty vocabulary, every declared
    # dependency is a fabrication.
    assert served_membership(_context()).conventions == {}


def test_membership_violations_flags_fabricated() -> None:
    m = served_membership(_context(), conventions=["sign_natural_balance"])
    output = InducedValidations(
        validations=[
            _induced("ok", referenced_columns=["journal_entries.debit"]),
            _induced("bad", referenced_tables=["ghost_table"], referenced_concepts=["revenue"]),
            _induced("dep", relevant_conventions=["ghost_convention"]),
        ]
    )
    violations = membership_violations(output, m)
    assert any("ghost_table" in v for v in violations)
    assert any("revenue" in v for v in violations)
    # A declared dependency on an unserved convention is a fabrication (DAT-865).
    assert any("ghost_convention" in v for v in violations)
    # The clean validation raises no violation.
    assert not any("'ok'" in v for v in violations)


def test_is_clean() -> None:
    m = served_membership(_context(), conventions=["sign_natural_balance"])
    assert _is_clean(_induced(referenced_columns=["debit"]), m)
    assert not _is_clean(_induced(referenced_columns=["fabricated_col"]), m)
    assert _is_clean(_induced(relevant_conventions=["sign_natural_balance"]), m)
    assert not _is_clean(_induced(relevant_conventions=["ghost_convention"]), m)


def test_to_spec_maps_typed_fields() -> None:
    spec = _to_spec(
        _induced(
            "mycheck",
            tolerance=0.0,
            guidance="g",
            severity="critical",
            relevant_conventions=["sign_natural_balance"],
        )
    )
    assert spec.validation_id == "mycheck"
    assert spec.tolerance == 0.0
    assert spec.guidance == "g"
    assert spec.severity == ValidationSeverity.CRITICAL
    assert spec.source == "generated"
    # The declared convention dependency persists onto the spec (DAT-865) — the
    # binder resolves it back to the convention prose at SQL-generation time.
    assert spec.relevant_conventions == ["sign_natural_balance"]


def test_render_conventions_serves_ids() -> None:
    """The induction conventions render carries each convention's stable id (DAT-865).

    The id header is what makes a convention DECLARABLE (`relevant_conventions` is
    membership-validated against these ids); statement + group lines stay in the
    binder-side format.
    """
    rendered = _render_conventions(
        [
            OntologyConvention(
                id="sign_rule",
                targets=["extraction"],
                statement="Sign every measure by its natural balance.",
                concept_groups={"credit_normal": ["revenue", "equity"]},
            ),
            OntologyConvention(id="netting", targets=[], statement="Net the legs."),
        ]
    )
    assert "[convention: sign_rule]" in rendered
    assert "[convention: netting]" in rendered
    assert "Sign every measure by its natural balance." in rendered
    assert "credit_normal: revenue, equity" in rendered
    assert _render_conventions([]) == ""


def test_contract_is_constrained_decoding_safe() -> None:
    """DAT-807 budget: every field required, no open maps, enums (not unions)."""
    schema = InducedValidation.model_json_schema()
    # All fields required — constrained decoding cannot carry an optional.
    assert set(schema["required"]) == set(schema["properties"])
    # No union-typed (anyOf/oneOf) properties — severity/check_type are `enum`.
    for prop in schema["properties"].values():
        assert "oneOf" not in prop
        assert "anyOf" not in prop
    assert schema["properties"]["severity"]["enum"] == ["info", "warning", "error", "critical"]
    # check_type is the four-value contract — NO `referential` (would break the
    # cockpit's closed CHECK_TYPES enum).
    assert schema["properties"]["check_type"]["enum"] == [
        "balance",
        "comparison",
        "constraint",
        "aggregate",
    ]


def test_check_type_literal_matches_the_single_home_enum() -> None:
    """The induction Literal cannot drift from ValidationCheckType (the DB CHECK's home)."""
    from typing import get_args

    from dataraum.analysis.validation.induction import CheckTypeLiteral
    from dataraum.analysis.validation.models import ValidationCheckType

    assert set(get_args(CheckTypeLiteral)) == {v.value for v in ValidationCheckType}
    assert "referential" not in get_args(CheckTypeLiteral)


# --- the agent: membership + repair + drop -----------------------------------


class _FakeProvider:
    """Returns a queued sequence of structured outputs (one per converse call)."""

    def __init__(self, *outputs: InducedValidations) -> None:
        self._outputs = list(outputs)
        self.calls = 0

    def get_model_for_tier(self, _tier: object) -> str:
        return "test-model"

    def converse(self, _request: object) -> Result[ConversationResponse]:
        out = self._outputs[self.calls]
        self.calls += 1
        return Result.ok(
            ConversationResponse(
                content=out.model_dump_json(),
                stop_reason="end_turn",
                model="test-model",
                input_tokens=1,
                output_tokens=1,
            )
        )


def _agent(provider: _FakeProvider) -> ValidationInductionAgent:
    config = MagicMock()
    # DAT-735: induction reads its OWN feature config key, not `validation`.
    config.features.validation_induction.enabled = True
    config.features.validation_induction.model_tier = "balanced"
    config.features.validation_induction.effort = "low"
    config.limits.max_output_tokens_per_request = 8000
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user", 0.0)
    return ValidationInductionAgent(config=config, provider=provider, prompt_renderer=renderer)


def test_induce_returns_clean_specs() -> None:
    m = served_membership(_context())
    provider = _FakeProvider(
        InducedValidations(validations=[_induced("bal", referenced_columns=["debit", "credit"])])
    )
    result = _agent(provider).induce("<graph>", "conv", m)
    assert result.success
    specs = result.unwrap()
    assert [s.validation_id for s in specs] == ["bal"]
    assert provider.calls == 1  # no repair needed


def test_induce_repairs_then_keeps() -> None:
    m = served_membership(_context())
    fabricated = InducedValidations(validations=[_induced("bal", referenced_tables=["ghost"])])
    repaired = InducedValidations(
        validations=[_induced("bal", referenced_tables=["journal_entries"])]
    )
    provider = _FakeProvider(fabricated, repaired)
    result = _agent(provider).induce("<graph>", "conv", m)
    assert result.success
    assert [s.validation_id for s in result.unwrap()] == ["bal"]
    assert provider.calls == 2  # one induce + one repair


def test_induce_drops_still_fabricated_after_repair() -> None:
    m = served_membership(_context())
    fabricated = InducedValidations(
        validations=[
            _induced("good", referenced_columns=["debit"]),
            _induced("bad", referenced_tables=["ghost"]),
        ]
    )
    # Repair returns the SAME fabrication for 'bad' — it must be dropped, 'good' kept.
    provider = _FakeProvider(fabricated, fabricated)
    result = _agent(provider).induce("<graph>", "conv", m)
    assert result.success
    assert [s.validation_id for s in result.unwrap()] == ["good"]


def test_induce_empty_is_legitimate() -> None:
    provider = _FakeProvider(InducedValidations(validations=[]))
    result = _agent(provider).induce("<graph>", "conv", served_membership(_context()))
    assert result.success
    assert result.unwrap() == []


def test_induce_canonicalizes_declared_convention_variants() -> None:
    """A tolerated case/quote variant persists as the CANONICAL id (DAT-865).

    The membership gate is tolerant (``_norm``) but the bind-time pull matches the
    persisted string exactly against ``Convention.name`` — a variant persisted raw
    would select nothing at bind and silently reproduce the empty-conventions
    defect this lane closes.
    """
    m = served_membership(_context(), conventions=["sign_natural_balance"])
    provider = _FakeProvider(
        InducedValidations(
            validations=[
                _induced(
                    "bal",
                    referenced_columns=["debit"],
                    relevant_conventions=["'Sign_Natural_Balance'"],
                )
            ]
        )
    )
    result = _agent(provider).induce("<graph>", "conv", m)
    assert result.success
    assert provider.calls == 1  # the variant is tolerated, not a fabrication
    (spec,) = result.unwrap()
    assert spec.relevant_conventions == ["sign_natural_balance"]
    # …and the canonical id resolves at the bind-time pull.
    from dataraum.analysis.semantic.ontology import OntologyDefinition, OntologyLoader

    ont = OntologyDefinition.model_construct(
        name="t",
        conventions=[
            OntologyConvention(
                id="sign_natural_balance", targets=["extraction"], statement="the sign rule"
            )
        ],
    )
    rendered = OntologyLoader().format_conventions_for_prompt(
        ont, "validation", qualifier="bal", include_ids=spec.relevant_conventions
    )
    assert "the sign rule" in rendered


# --- served-graph enrichment: metric DAG + additivity (DAT-735 owner ruling) ------


def test_render_metric_dag_serves_declared_metrics(session) -> None:
    """The metric DAG section names each metric, its derives_from concepts + params."""
    from dataraum.analysis.validation.induction import _render_metric_dag
    from dataraum.graphs.metric_graph_db_models import Metric, MetricDerivesFrom, MetricParameter

    session.add(
        Metric(
            vertical="finance",
            graph_id="current_ratio",
            name="Current Ratio",
            output_type="ratio",
            source="seed",
        )
    )
    session.add(
        MetricDerivesFrom(
            vertical="finance",
            graph_id="current_ratio",
            concept_name="current_assets",
        )
    )
    session.add(
        MetricParameter(
            vertical="finance",
            graph_id="current_ratio",
            name="period",
            param_type="string",
            default_value="month",
            source="seed",
        )
    )
    session.flush()

    rendered = _render_metric_dag(session, "finance")
    assert "## Metric DAG" in rendered
    assert "current_ratio" in rendered
    assert "derives_from: current_assets" in rendered
    assert "period=" in rendered
    # A different vertical sees none of it.
    assert _render_metric_dag(session, "marketing") == ""


def test_render_additivity_serves_verdicts_at_head(session) -> None:
    """The additivity section renders the verdicts + reasons at the promoted head."""
    from dataraum.analysis.validation.induction import _render_additivity
    from dataraum.graphs.additivity_db_models import MetricAdditivity

    session.add(
        MetricAdditivity(
            run_id="om-run-1",
            target_kind="metric",
            target_key="current_liabilities",
            categorical_additive=True,
            time_additive=False,
            time_reason="stock",
        )
    )
    session.flush()

    rendered = _render_additivity(session, "om-run-1")
    assert "## Additivity Verdicts" in rendered
    assert "current_liabilities" in rendered
    assert "categorical:additive" in rendered
    assert "time:NON-additive (stock)" in rendered


def test_render_additivity_empty_on_first_run(session) -> None:
    """No promoted operating_model head (first run) ⇒ the section is absent."""
    from dataraum.analysis.validation.induction import _render_additivity

    assert _render_additivity(session, None) == ""
