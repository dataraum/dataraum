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
    _render_existence_universe,
    _render_temporal_form,
    _to_spec,
    membership_violations,
    served_membership,
)
from dataraum.analysis.validation.models import ValidationSeverity
from dataraum.core.models.base import Result
from dataraum.graphs.context_format import format_served_context
from dataraum.graphs.context_models import (
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


def _table(role: str | None) -> TableContext:
    return TableContext(
        table_id=f"t_{role or 'none'}",
        table_name=f"tbl_{role or 'none'}",
        columns=[ColumnContext(column_id="c1", column_name="entity_id", table_name="tbl")],
        table_role=role,
    )


def test_existence_universe_fires_when_no_dimension() -> None:
    """No dimension-role table served ⇒ the existence-check universe fact fires
    (DAT-876), stating existence checks are unbindable — absence falls loud."""
    ctx = GraphExecutionContext(tables=[_table("fact"), _table("periodic_snapshot")])
    rendered = _render_existence_universe(ctx)
    assert "## Existence-check universe" in rendered
    assert "No served table has role=dimension" in rendered
    assert "Do not propose existence checks here." in rendered


def test_existence_universe_silent_when_dimension_present() -> None:
    """A served dimension-role table IS an enumerator ⇒ the fact is silent, so
    legitimate existence checks against it are still proposed (DAT-876)."""
    ctx = GraphExecutionContext(tables=[_table("fact"), _table("dimension")])
    assert _render_existence_universe(ctx) == ""


def test_existence_universe_silent_when_no_tables() -> None:
    """No served tables ⇒ nothing to state; the fact is silent (DAT-876)."""
    assert _render_existence_universe(GraphExecutionContext(tables=[])) == ""


def test_existence_universe_fires_when_role_unclassified() -> None:
    """An unclassified table (no TableEntity ⇒ table_role None) is not an enumerator,
    so the fact fires — the conservative decline, which IS the DAT-876 scenario."""
    ctx = GraphExecutionContext(tables=[_table(None)])
    assert "No served table has role=dimension" in _render_existence_universe(ctx)


def _measure(name: str, materialization: str | None, role: str | None = "measure") -> ColumnContext:
    return ColumnContext(
        column_id=f"c_{name}",
        column_name=name,
        table_name="tbl",
        semantic_role=role,
        materialization=materialization,
    )


def _measures_table(*columns: ColumnContext, duckdb_name: str = "src__tbl") -> TableContext:
    return TableContext(
        table_id="t1", table_name="tbl", duckdb_name=duckdb_name, columns=list(columns)
    )


def test_temporal_form_states_both_verdicts() -> None:
    """The resolved stock/flow verdicts are restated ADJACENT to the task (DAT-874).

    The shared assembler carries them only as a ``Materialization`` cell in a wide
    per-table column table; DAT-870 measured at the binder that facts held that far
    from the prose lose to a confident hint. This is the induction-side counterweight.
    """
    ctx = GraphExecutionContext(
        tables=[_measures_table(_measure("movement", "flow"), _measure("level", "stock"))]
    )
    rendered = _render_temporal_form(ctx)
    assert "## Temporal form of the measures" in rendered
    assert "a per-period movement, additive across periods): movement" in rendered
    assert "a level as of its period, never summed across periods): level" in rendered
    # Named by the SAME display name the catalog section uses, so the two agree.
    assert "- src__tbl:" in rendered


def test_temporal_form_states_undetermined_measures_positively() -> None:
    """A measure with NO verdict is NAMED as undetermined (DAT-874/DAT-876 doctrine).

    ``format_served_context`` renders a missing verdict as an EMPTY CELL, which the
    prompt would have to read absence off. Absence falls loud or it does not land.
    """
    ctx = GraphExecutionContext(
        tables=[_measures_table(_measure("movement", "flow"), _measure("unknown_measure", None))]
    )
    rendered = _render_temporal_form(ctx)
    assert "NO temporal-form verdict — UNDETERMINED" in rendered
    assert "unknown_measure" in rendered
    assert "a stated absence, not 'flow'" in rendered
    assert "grounds no cross-measure comparison at all" in rendered


def test_shared_assembler_leaves_a_missing_verdict_blank() -> None:
    """WHY this renderer exists (DAT-874): the shared catalog states a missing
    verdict as an EMPTY CELL, so the absence is legible only as an omission.

    Pins the premise, not just the remedy. If ``format_served_context`` ever starts
    stating the absence itself, this fails loud — and the signal then is to DELETE
    the undetermined half of ``_render_temporal_form`` rather than serve one fact
    from two homes.
    """
    ctx = GraphExecutionContext(tables=[_measures_table(_measure("unknown_measure", None))])
    catalog = format_served_context(ctx)

    row = next(line for line in catalog.splitlines() if line.startswith("| unknown_measure |"))
    # | Column | Type | Role | Materialization | Notes | — cell 4 is the verdict.
    assert row.split("|")[4].strip() == ""
    assert "UNDETERMINED" not in catalog

    # The induction-owned block names the same column instead of blanking it.
    served = _render_temporal_form(ctx)
    assert "NO temporal-form verdict — UNDETERMINED" in served
    assert "unknown_measure" in served


def test_temporal_form_ignores_unverdicted_non_measures() -> None:
    """The UNDETERMINED list is measure-scoped: a dimension/key column without a
    verdict is not a decision gap, and listing every such column would bury the
    measures that are one."""
    ctx = GraphExecutionContext(
        tables=[_measures_table(_measure("movement", "flow"), _measure("account_key", None, "key"))]
    )
    rendered = _render_temporal_form(ctx)
    assert "account_key" not in rendered
    # The header's standing sentence still names the class; no table LINE claims one.
    assert "NO temporal-form verdict" not in rendered


def test_temporal_form_states_a_verdict_without_a_role_label() -> None:
    """A served verdict is never hidden by a missing semantic_role — the fact was
    measured, so it is stated; only the ABSENCE half needs the measure label."""
    ctx = GraphExecutionContext(tables=[_measures_table(_measure("level", "stock", None))])
    assert "level" in _render_temporal_form(ctx)


def test_temporal_form_silent_when_nothing_to_state() -> None:
    """No verdicts and no unverdicted measures ⇒ no section (empty, not a header)."""
    ctx = GraphExecutionContext(tables=[_measures_table(_measure("account_key", None, "key"))])
    assert _render_temporal_form(ctx) == ""
    assert _render_temporal_form(GraphExecutionContext(tables=[])) == ""


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
    renderer.render_split.return_value = ("system", "user")
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


def test_induce_serves_the_form_facts_and_does_not_veto_a_mixed_form_proposal() -> None:
    """The DAT-874 shape stays PROPOSABLE — the guard is the served fact, not a veto.

    Reconstructs the defect's shape on a neutral graph: a snapshot table whose level
    column is ``stock``, a detail table whose movement columns are ``flow``, and a
    proposed check referencing both sides. Two halves, both deliberate:

    * The proposal survives ``induce()`` untouched. Membership (``_is_clean``) judges
      FABRICATION — an objective property — and nothing else; no seam inspects the
      shape. That is the locked call, not an oversight: a mixed-form reference set is
      not itself wrong (a movement vs the CHANGE in a level, and a cumulated movement
      vs a level, are both coherent and both mixed), so a reference-set rule would
      reject correct checks, and reading the shape out of the guidance or the bound
      SQL would be exactly the deterministic override the design forbids.
    * The temporal-form facts for that same graph NAME both sides' forms. The model
      choosing the shape has the deciding fact adjacent to the task — which is the
      whole remedy, and is what the column-table cell alone did not deliver.
    """
    ctx = GraphExecutionContext(
        tables=[
            TableContext(
                table_id="t_snap",
                table_name="period_snapshot",
                duckdb_name="src__period_snapshot",
                columns=[_measure("closing_level", "stock")],
            ),
            TableContext(
                table_id="t_detail",
                table_name="movement_detail",
                duckdb_name="src__movement_detail",
                columns=[_measure("increase", "flow"), _measure("decrease", "flow")],
            ),
        ]
    )
    facts = _render_temporal_form(ctx)
    assert "closing_level" in facts and "increase" in facts
    assert "a level as of its period, never summed across periods): closing_level" in facts
    assert "a per-period movement, additive across periods): increase, decrease" in facts

    mixed = _induced(
        "level_ties_to_movements",
        check_type="balance",
        tolerance=0.0,
        guidance="net the per-period movements and compare to the period's closing level",
        referenced_tables=["period_snapshot", "movement_detail"],
        referenced_columns=[
            "period_snapshot.closing_level",
            "movement_detail.increase",
            "movement_detail.decrease",
        ],
    )
    provider = _FakeProvider(InducedValidations(validations=[mixed]))
    result = _agent(provider).induce(facts, "conv", served_membership(ctx))

    assert result.success
    specs = result.unwrap()
    assert [s.validation_id for s in specs] == ["level_ties_to_movements"]
    assert specs[0].guidance == mixed.guidance  # persisted verbatim, never rewritten
    assert provider.calls == 1  # clean on membership ⇒ no repair turn


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
    """The additivity section renders the class verdicts at the promoted head."""
    from dataraum.analysis.validation.induction import _render_additivity
    from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity

    session.add_all(
        [
            MetricAxisAdditivity(
                run_id="om-run-1",
                vertical="financial_reporting",
                target_kind="metric",
                target_key="current_liabilities",
                axis_kind="categorical",
                axis_key=AXIS_KEY_ALL,
                status="classified",
                verdict="additive",
            ),
            MetricAxisAdditivity(
                run_id="om-run-1",
                vertical="financial_reporting",
                target_kind="metric",
                target_key="current_liabilities",
                axis_kind="time",
                axis_key=AXIS_KEY_ALL,
                status="classified",
                verdict="semi_additive",
                reason="stock",
            ),
        ]
    )
    session.flush()

    rendered = _render_additivity(session, "om-run-1")
    assert "## Additivity Verdicts" in rendered
    assert "current_liabilities" in rendered
    assert "categorical:additive" in rendered
    assert "time:semi_additive (stock)" in rendered
    # The induction must be told that only `additive` licenses a balance check.
    assert "Only `additive` licenses a sum-of-parts" in rendered


def test_render_additivity_names_an_abstention_as_unjudged(session) -> None:
    """An abstained axis is UNJUDGED, never rendered as if it were non-additive."""
    from dataraum.analysis.validation.induction import _render_additivity
    from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity

    session.add(
        MetricAxisAdditivity(
            run_id="om-run-2",
            vertical="financial_reporting",
            target_kind="measure",
            target_key="unclassified_measure",
            axis_kind="time",
            axis_key=AXIS_KEY_ALL,
            status="abstained",
            abstain_reason="unknown_temporal",
        )
    )
    session.flush()

    rendered = _render_additivity(session, "om-run-2")
    assert "time:UNJUDGED (unknown_temporal)" in rendered


def test_render_additivity_reads_only_the_class_rows(session) -> None:
    """A per-axis refinement row must not duplicate its target in the rendering."""
    from dataraum.analysis.validation.induction import _render_additivity
    from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity

    session.add_all(
        [
            MetricAxisAdditivity(
                run_id="om-run-3",
                vertical="financial_reporting",
                target_kind="measure",
                target_key="revenue",
                axis_kind="time",
                axis_key=AXIS_KEY_ALL,
                status="classified",
                verdict="additive",
            ),
            MetricAxisAdditivity(
                run_id="om-run-3",
                vertical="financial_reporting",
                target_kind="measure",
                target_key="revenue",
                axis_kind="time",
                axis_key="booked_on",
                status="classified",
                verdict="additive",
                bucket_grain="day",
            ),
        ]
    )
    session.flush()

    rendered = _render_additivity(session, "om-run-3")
    assert rendered.count("measure revenue") == 1
    assert "booked_on" not in rendered


def test_render_additivity_empty_on_first_run(session) -> None:
    """No promoted operating_model head (first run) ⇒ the section is absent."""
    from dataraum.analysis.validation.induction import _render_additivity

    assert _render_additivity(session, None) == ""
