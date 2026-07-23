"""Agentic validation induction over the served graph (DAT-735).

The validation SET is *generated* by agentic induction over the whole promoted
operating-model graph — concept edges + part_of closure, references topology,
additivity verdicts, conventions, cycles, measured_in/units, the metric DAG — not
derived deterministically from any single relation. The LLM proposes typed
validation specs (``check_type`` + ``tolerance`` + advisory ``guidance``); the
proposals are membership-validated against the served context (the
provenance-contract-v2 pattern — reject fabricated tables/columns/concepts) with a
single repair turn (the graph agent's ``repair_tool_contract`` precedent), and the
clean set is persisted as ``source='generated'`` rows.

The output contract is strict and constrained-decoding-safe (DAT-807): every field
is REQUIRED with a documented sentinel, no open maps, no union-typed properties
(the ``check_type``/``severity`` enums compile to ``enum``, not ``anyOf``). Only a
live call proves a schema compiles — the induction call is real-LLM.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field
from sqlalchemy import select

from dataraum.analysis.semantic.convention_store import load_workspace_conventions
from dataraum.analysis.semantic.db_models import WorkspaceSettings
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.graphs.context import (
    GraphExecutionContext,
    build_execution_context,
    format_served_context,
)
from dataraum.llm.contract_repair import repair_tool_contract
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import ConversationRequest, Message
from dataraum.llm.structured_output import parse_structured_output
from dataraum.storage.snapshot_head import catalog_head_target, head_run_id

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.analysis.semantic.ontology import OntologyConvention

logger = get_logger(__name__)

INDUCTION_TEMPLATE_NAME = "validation_induction"

# Generic check SHAPES (not domain terms) — the induced check_type vocabulary. Mirrors
# :class:`ValidationCheckType` (the cockpit CHECK_TYPES contract, four values) as an
# inline Literal for constrained decoding; the drift guard test pins Literal == enum.
# A referential-integrity check is `constraint`-shaped (zero violating rows), so there
# is no fifth `referential` value — it would break the cockpit's closed enum.
CheckTypeLiteral = Literal["balance", "comparison", "constraint", "aggregate"]
SeverityLiteral = Literal["info", "warning", "error", "critical"]


class InducedValidation(BaseModel):
    """One induced validation — a TYPED check definition grounded in the served graph.

    Every field is REQUIRED (DAT-807): constrained decoding cannot carry an optional,
    so "none" is a documented sentinel (``""`` / ``[]``). The three ``referenced_*``
    lists are the provenance contract — the tables/columns/concepts this check reasons
    over, membership-validated against the served graph so a fabricated reference is
    caught (and repaired, then dropped) before the row is ever persisted.
    """

    validation_id: str = Field(
        description="lowercase_snake_case identifier, unique within the vertical."
    )
    name: str = Field(description="Human-readable name.")
    description: str = Field(description="What the check verifies and why it matters.")
    category: str = Field(
        description="Free-text grouping label (e.g. data_quality, referential_integrity)."
    )
    severity: SeverityLiteral = Field(description="How bad a failure is.")
    check_type: CheckTypeLiteral = Field(description="The generic shape of the check.")
    tolerance: float = Field(
        description=(
            "The ADR-0017 pass threshold: a row passes when deviation <= tolerance. "
            "Use 0.0 for an exact identity (e.g. a balance that must net to zero)."
        )
    )
    guidance: str = Field(
        description=(
            "Advisory prose for the SQL-binding agent — reference the real table and "
            "column names from the served graph. NOT the check's definition (that is "
            'check_type + tolerance). "" when none.'
        )
    )
    expected_outcome: str = Field(description='What a passing result looks like; "" when none.')
    relevant_cycles: list[str] = Field(
        description="Cycle types this validation applies to; [] means universal."
    )
    relevant_conventions: list[str] = Field(
        description=(
            "Ids of the served domain conventions this check's logic relies on (e.g. "
            "a sign or netting rule) — the SQL author binding the check receives "
            "EXACTLY these conventions. [] when the check relies on none."
        )
    )
    referenced_tables: list[str] = Field(
        description="Tables from the served graph this check reads; [] when none."
    )
    referenced_columns: list[str] = Field(
        description="Columns (table.column) from the served graph this check reads; [] when none."
    )
    referenced_concepts: list[str] = Field(
        description="Concept names from the served graph this check reasons over; [] when none."
    )


class InducedValidations(BaseModel):
    """The induce_validations structured output — the proposed validation set."""

    validations: list[InducedValidation] = Field(
        description="The proposed validations (may be empty on a thin graph)."
    )


@dataclass
class Membership:
    """The served graph's vocabulary — the reference sets membership is judged against."""

    tables: set[str] = field(default_factory=set)
    columns: set[str] = field(default_factory=set)
    concepts: set[str] = field(default_factory=set)
    # Served convention ids (DAT-865): `relevant_conventions` is membership-validated
    # like the referenced_* trio — a declared dependency on an unserved convention is
    # a fabrication (repaired once, then dropped). A MAP, not a set: ``_norm(id) →
    # canonical id``. The validator is tolerant (``_norm``) but the bind-time pull
    # (``format_conventions_for_prompt include_ids``) matches EXACTLY, so ``induce()``
    # rewrites every kept declaration to the canonical id before persist — a
    # case/quote variant must never pass the gate and then silently select nothing
    # at bind (that empty-conventions bind is the DAT-865 defect itself).
    conventions: dict[str, str] = field(default_factory=dict)


def _norm(token: str) -> str:
    """Normalize a reference token for membership comparison (case/quote-insensitive)."""
    return token.strip().strip('"').strip("'").lower()


def served_membership(
    context: GraphExecutionContext, conventions: Iterable[str] = ()
) -> Membership:
    """Build the reference vocabulary from the served graph.

    Accepts a column by its bare name AND its ``table.column`` qualifier (in either the
    logical table_name or the duckdb_name form): membership catches FABRICATED entities,
    it does not enforce a reference style. ``conventions`` are the served convention
    ids (DAT-865) — the vocabulary ``relevant_conventions`` is judged against, kept
    as a ``_norm(id) → canonical id`` map so a tolerated variant canonicalizes at save.
    """
    membership = Membership(conventions={_norm(c): c for c in conventions})
    for table in context.tables:
        table_forms = [n for n in (table.table_name, table.duckdb_name) if n]
        for form in table_forms:
            membership.tables.add(_norm(form))
        for col in table.columns:
            membership.columns.add(_norm(col.column_name))
            for form in table_forms:
                membership.columns.add(_norm(f"{form}.{col.column_name}"))
    for concept in context.concepts:
        membership.concepts.add(_norm(concept.name))
    return membership


def _is_clean(validation: InducedValidation, membership: Membership) -> bool:
    """True when every referenced table/column/concept is in the served vocabulary."""
    return (
        all(_norm(t) in membership.tables for t in validation.referenced_tables)
        and all(_norm(c) in membership.columns for c in validation.referenced_columns)
        and all(_norm(c) in membership.concepts for c in validation.referenced_concepts)
        and all(_norm(c) in membership.conventions for c in validation.relevant_conventions)
    )


def membership_violations(output: InducedValidations, membership: Membership) -> list[str]:
    """Human-readable violation lines for every fabricated reference (repair feed).

    The provenance-contract-v2 shape: one line per fabricated table/column/concept,
    naming the offending validation — fed verbatim into ``repair_tool_contract``.
    """
    violations: list[str] = []
    for validation in output.validations:
        vid = validation.validation_id
        for table in validation.referenced_tables:
            if _norm(table) not in membership.tables:
                violations.append(f"validation '{vid}' references table '{table}' not in the graph")
        for column in validation.referenced_columns:
            if _norm(column) not in membership.columns:
                violations.append(
                    f"validation '{vid}' references column '{column}' not in the graph"
                )
        for concept in validation.referenced_concepts:
            if _norm(concept) not in membership.concepts:
                violations.append(
                    f"validation '{vid}' references concept '{concept}' not in the graph"
                )
        for convention in validation.relevant_conventions:
            if _norm(convention) not in membership.conventions:
                # The repair turn sees ONLY the violation lines (no vocabulary
                # re-serve), and the served convention set is a handful — inline it
                # so a near-miss can be FIXED rather than amputated (dropping the
                # reference loses the declared judgment the check relies on).
                served = ", ".join(sorted(membership.conventions.values())) or "none"
                violations.append(
                    f"validation '{vid}' relies on convention '{convention}' not in "
                    f"the served domain conventions (served ids: {served})"
                )
    return violations


def _to_spec(validation: InducedValidation) -> ValidationSpec:
    """A generated :class:`ValidationSpec` from an induced proposal."""
    return ValidationSpec(
        validation_id=validation.validation_id,
        name=validation.name,
        description=validation.description,
        category=validation.category,
        severity=ValidationSeverity(validation.severity),
        check_type=validation.check_type,
        tolerance=validation.tolerance,
        guidance=validation.guidance or None,
        expected_outcome=validation.expected_outcome or None,
        relevant_cycles=validation.relevant_cycles,
        relevant_conventions=validation.relevant_conventions,
        source="generated",
    )


def _render_additivity(session: Session, om_head_run_id: str | None) -> str:
    """The additivity verdicts at the promoted operating_model head (DAT-716/735).

    Whether a breakdown by an axis class reconciles to the unsliced total. A
    NON-additive target must not be summed across that axis, so the balance-check
    class the induction proposes needs this explicitly: a Σ-of-parts check over a
    non-additive target is a false positive. Run-versioned — read at the promoted
    head (``om_head_run_id``); empty on a first run (no operating_model promoted yet).
    """
    if om_head_run_id is None:
        return ""
    from dataraum.graphs.additivity_db_models import MetricAdditivity

    rows = (
        session.execute(
            select(MetricAdditivity)
            .where(MetricAdditivity.run_id == om_head_run_id)
            .order_by(MetricAdditivity.target_kind, MetricAdditivity.target_key)
        )
        .scalars()
        .all()
    )
    if not rows:
        return ""
    lines = ["", "## Additivity Verdicts"]
    for r in rows:
        cat = (
            "categorical:additive"
            if r.categorical_additive
            else f"categorical:NON-additive ({r.categorical_reason or 'n/a'})"
        )
        tim = (
            "time:additive" if r.time_additive else f"time:NON-additive ({r.time_reason or 'n/a'})"
        )
        lines.append(f"- {r.target_kind} {r.target_key}: {cat}; {tim}")
    return "\n".join(lines)


def _render_metric_dag(session: Session, vertical: str) -> str:
    """The declared metric DAG for a vertical (DAT-732/735).

    Each metric, the concepts it derives_from (its ``part_of``-style inputs), and its
    declared parameters. Vertical-scoped and always-current (declaration-versioned),
    so it is served regardless of the operating_model head. Gives the induction the
    rollup structure (metric = Σ over derived concepts) the raw-column schema hides —
    the substrate for the new balance/reconciliation checks the shipped YAML lacked.
    """
    from dataraum.graphs.metric_graph_db_models import (
        Metric,
        MetricDerivesFrom,
        MetricParameter,
    )

    metrics = (
        session.execute(
            select(Metric)
            .where(Metric.vertical == vertical, Metric.superseded_at.is_(None))
            .order_by(Metric.graph_id)
        )
        .scalars()
        .all()
    )
    if not metrics:
        return ""
    derives: dict[str, list[str]] = {}
    for edge in session.execute(
        select(MetricDerivesFrom).where(
            MetricDerivesFrom.vertical == vertical, MetricDerivesFrom.superseded_at.is_(None)
        )
    ).scalars():
        derives.setdefault(edge.graph_id, []).append(edge.concept_name)
    params: dict[str, list[MetricParameter]] = {}
    for param in session.execute(
        select(MetricParameter).where(
            MetricParameter.vertical == vertical, MetricParameter.superseded_at.is_(None)
        )
    ).scalars():
        params.setdefault(param.graph_id, []).append(param)

    lines = ["", "## Metric DAG"]
    for metric in metrics:
        unit = f", unit={metric.unit}" if metric.unit else ""
        lines.append(f"### {metric.graph_id} ({metric.name}) — {metric.output_type or '?'}{unit}")
        froms = derives.get(metric.graph_id)
        if froms:
            lines.append(f"derives_from: {', '.join(sorted(froms))}")
        declared = params.get(metric.graph_id)
        if declared:
            rendered = ", ".join(f"{p.name}={p.default_value!r} ({p.param_type})" for p in declared)
            lines.append(f"parameters: {rendered}")
    return "\n".join(lines)


def _render_existence_universe(context: GraphExecutionContext) -> str:
    """The existence-check universe fact (DAT-876) — a projection of table_role.

    An existence check ("this id must exist") binds only against a table that
    authoritatively enumerates the entity: a DIMENSION-role table. When the served
    graph has tables but none is dimension-role, no such enumerator exists — state
    it as a POSITIVE fact so the induction DECLINES the class rather than inferring
    existability from a fact/snapshot table (an id absent from an activity table may
    simply have no activity there, not be nonexistent). Absence falls loud, never
    inferred from the omission of a DIMENSION label. Empty when a dimension-role
    table is served (existence checks bind normally) or no tables are served.

    Served in ``build_served_context`` (induction-owned), NOT ``format_served_context``:
    that assembler is SHARED with the metric grounding agent, which proposes no
    existence checks — this validation-scoped fact would only be noise there.
    """
    tables = context.tables
    if not tables or any(t.table_role == "dimension" for t in tables):
        return ""
    return (
        "\n## Existence-check universe\n"
        "No served table has role=dimension — none authoritatively enumerates an "
        "entity's whole population. An existence check (a parent id or referenced "
        'key "must exist") is UNBINDABLE against this graph: a fact or snapshot '
        "table records activity, so an id absent from it may simply have no activity "
        "there, not be nonexistent. Do not propose existence checks here."
    )


def _render_conventions(conventions: list[OntologyConvention]) -> str:
    """Render the workspace conventions WITH their stable ids (DAT-865).

    The induction DECLARES the conventions a check's logic relies on
    (``relevant_conventions``), so each block is headed by the convention's id —
    the referenceable name membership is judged against. The statement + group
    lines render exactly like the binder-side ``format_conventions_for_prompt``
    blocks; only the id header is added, and only on this serving (extraction and
    the binder keep their id-less rendering).
    """
    blocks: list[str] = []
    for conv in conventions:
        lines = [f"[convention: {conv.id}]", conv.statement.strip()]
        for group, members in conv.concept_groups.items():
            lines.append(f"{group}: {', '.join(members)}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def build_served_context(
    session: Session,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    *,
    vertical: str,
    om_run_id: str | None,
    catalogue_run_id: str | None,
    workspace_id: str | None,
) -> tuple[str, str, Membership]:
    """Serve the promoted graph: (rendered context, conventions, membership vocab).

    Reuses the shared graph assembler (``build_execution_context`` +
    ``format_served_context`` — the SAME served graph the metric grounding agent
    reads: concepts + part_of, references, cycles, per-column materialization = the
    additivity signal, reconciles_with), then APPENDS induction-specific sections
    the served graph leaves implicit: the additivity verdicts and the metric DAG
    the balance-check class needs (DAT-735), and the existence-check universe fact
    (DAT-876) — a positive statement when no served table is dimension-role, so the
    existence class is declined rather than bound against an activity table.

    The conventions slot is induction's OWN (DAT-865): ALL active conventions,
    rendered WITH their stable ids — not ``context.conventions`` (the
    extraction-routed, id-less set). The induction is the vertical's judgment pass:
    it sees the whole declared judgment and DECLARES per check which conventions
    the check relies on (``relevant_conventions``, membership-validated against
    these ids); the ``targets`` routing keeps narrowing what a bound SQL prompt
    sees, it no longer gates what the induction may know.

    This-run cycles/additivity are not yet promoted when induction runs (it precedes
    them in the spine), so those reflect the PRIOR operating_model head — **empty on a
    first run**, when no operating_model has ever promoted. The metric DAG is
    declaration-versioned (always current). The induction degrades gracefully.
    """
    context = build_execution_context(
        session,
        table_ids,
        duckdb_conn,
        vertical=vertical,
        om_run_id=om_run_id,
        catalogue_run_id=catalogue_run_id,
        workspace_id=workspace_id,
    )
    served = format_served_context(context)

    # The promoted operating_model head for the run-versioned additivity read (DAT-848
    # scoping mirrors the concept reads: active_vertical wins, the run's vertical is
    # the unbound fallback). om_head is None on a first run ⇒ additivity section empty.
    om_head = head_run_id(session, catalog_head_target(), "operating_model")
    effective_vertical = (
        session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none() or vertical
    )
    served += (
        _render_additivity(session, om_head)
        + _render_metric_dag(session, effective_vertical)
        + _render_existence_universe(context)
    )
    ws_conventions = load_workspace_conventions(session, vertical)
    return (
        served,
        _render_conventions(ws_conventions),
        served_membership(context, conventions=[c.id for c in ws_conventions]),
    )


class ValidationInductionAgent(LLMFeature):
    """LLM-powered validation induction over the served graph (DAT-735)."""

    def induce(
        self, served_graph: str, conventions: str, membership: Membership
    ) -> Result[list[ValidationSpec]]:
        """Propose typed validations, membership-validate (+1 repair), drop fabricated.

        Returns the CLEAN generated specs (a validation still referencing a fabricated
        table/column/concept after the single repair turn is DROPPED, never persisted).
        An empty result is legitimate (a thin graph). A provider error propagates for
        the durable boundary to retry; a parse failure is a hard failure (no rescue,
        mirroring the validation-SQL path).
        """
        feature_config = self.config.features.validation_induction
        if not feature_config or not feature_config.enabled:
            return Result.fail("Validation induction feature is disabled in config")

        context = {"served_graph": served_graph, "conventions": conventions or "None"}
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                INDUCTION_TEMPLATE_NAME, context
            )
        except Exception as e:  # noqa: BLE001 - render failure is a hard, non-retryable stop
            return Result.fail(f"Failed to render validation induction prompt: {e}")

        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            output_schema=InducedValidations.model_json_schema(),
            label=INDUCTION_TEMPLATE_NAME,
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )
        # converse raises a typed ProviderError on transient failure — it rides to the
        # durable boundary for retry, so we don't re-wrap it. A returned Result succeeds.
        response = self.provider.converse(request).unwrap()

        parsed = parse_structured_output(
            response, InducedValidations, label=INDUCTION_TEMPLATE_NAME
        )
        if not parsed.success:
            return Result.fail(parsed.error or "validation_induction failed")
        output = parsed.unwrap()

        # Membership contract (provenance-contract-v2): repair fabricated references
        # ONCE, then DROP any validation still grounded on a fabricated entity.
        violations = membership_violations(output, membership)
        if violations:
            logger.warning("validation_induction_membership_violations", count=len(violations))
            repaired = repair_tool_contract(
                self.provider,
                output.model_dump(mode="json"),
                violations,
                InducedValidations,
                model=model,
                label=INDUCTION_TEMPLATE_NAME,
                max_tokens=self.config.limits.max_output_tokens_per_request,
            )
            if repaired.success:
                output = repaired.unwrap()

        clean = [v for v in output.validations if _is_clean(v, membership)]
        # Canonicalize the declared convention ids before persist (DAT-865): the
        # membership gate tolerates case/quote variants (``_norm``), but the
        # bind-time pull matches the persisted string EXACTLY against
        # ``Convention.name`` — a tolerated variant that persisted raw would select
        # nothing at bind, silently reproducing the empty-conventions defect.
        for v in clean:
            v.relevant_conventions = [
                membership.conventions[_norm(c)] for c in v.relevant_conventions
            ]
        dropped = len(output.validations) - len(clean)
        if dropped:
            logger.warning("validation_induction_dropped_fabricated", dropped=dropped)
        logger.info(
            "validation_induction_complete", proposed=len(output.validations), kept=len(clean)
        )
        return Result.ok([_to_spec(v) for v in clean])


__all__ = [
    "InducedValidation",
    "InducedValidations",
    "Membership",
    "ValidationInductionAgent",
    "build_served_context",
    "membership_violations",
    "served_membership",
]
