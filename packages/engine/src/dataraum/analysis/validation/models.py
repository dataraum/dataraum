"""Pydantic models for generic validation checks.

Contains data structures for validation specs and results.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(UTC)


class ValidationSeverity(StrEnum):
    """Severity levels for validation failures."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationStatus(StrEnum):
    """Status of a validation check."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class ValidationCheckType(StrEnum):
    """The generic check SHAPE — a cross-package VOCABULARY contract (DAT-735).

    CLOSED to these four values, mirrored EXACTLY by the cockpit's
    ``validation-spec.ts`` ``CHECK_TYPES`` zod enum — a new value is engine
    evolution, NEVER a teach (the cockpit rejects an unlisted value at spec parse). The
    single home the typed ``validations`` CHECK and the induction contract's Literal
    both derive from. ``check_type`` is a LABEL the ADR-0017 evaluator never branches on
    — it names the shape, not the logic (``deviation <= tolerance`` is uniform).

    (A ``referential`` value was cut here: referential-integrity checks are
    ``constraint``-shaped by the enum's own "zero violating rows" definition — the
    historical ``orphan_transactions`` seed check's shape, before DAT-725 band 3
    retired finance's shipped validation YAMLs — and the fifth value would break
    the cockpit's closed enum.)
    """

    BALANCE = "balance"
    COMPARISON = "comparison"
    CONSTRAINT = "constraint"
    AGGREGATE = "aggregate"


class ExpectedFormulaDeclaration(BaseModel):
    """The DAT-447 expected-formula teach's typed declaration payload (DAT-880).

    Present on a :class:`ValidationSpec` exactly when ``check_type ==
    "expected_formula"`` (enforced by :meth:`ValidationSpec._check_expected_formula_
    pairing`) — the fifth, out-of-vocabulary check_type value a column-identity
    declaration rides (see the field comment on ``ValidationSpec.check_type``).
    ``table``/``column`` identify WHICH column the declaration targets —
    :func:`dataraum.entropy.detectors.loaders.load_declared_formula` matches them
    case-insensitively against the measured column — and ``formula`` is the human's
    claimed identity in the discovery's binary-arithmetic language (e.g. ``"subtotal
    + tax"``), which the SQL-binding agent renders into its grounding guidance
    (``analysis.validation.agent``) and the derived_value measurement pools as the
    ``human_declaration`` witness. Replaces the untyped ``parameters: {table,
    column, formula}`` bag the DAT-880 retype retired.
    """

    table: str
    column: str
    formula: str


class ValidationSpec(BaseModel):
    """Specification for a validation check — a TYPED check definition (DAT-735).

    The check LOGIC is typed: ``check_type`` + ``tolerance`` (the ADR-0017 verdict
    param, ``deviation <= tolerance``). ``guidance`` is advisory prose for the
    SQL-binding agent — the former free-text ``sql_hints``, which is NO LONGER the
    check's definition. The LLM interprets the description + guidance to identify
    relevant columns; no pre-resolution needed.

    Read from the typed ``validations`` home (:class:`~dataraum.analysis.validation.
    db_models.Validation`) ``⊕`` the ``validation`` teach overlay — a
    :func:`~dataraum.core.overlay.apply_overlay`-merged raw dict re-parsed through
    this model at load time (``analysis.validation.config.load_all_validation_specs``),
    so every field here (including ``expected_formula``, DAT-880) is the ROW'S wire
    shape, typed — never a free-form bag a loader re-interprets downstream. ONE
    wire shape is still legacy, and LIVE: see ``_fold_legacy_check_fields`` below.

    ``extra="forbid"`` (DAT-880): the ``mode="before"`` fold below consumes the
    one legacy wire shape's keys (``parameters``/``sql_hints``) before field
    validation runs, so any OTHER unrecognized key reaching this point is a
    genuine unknown field, not a variant the model has to be lenient about — a
    malformed teach/induced row now fails loudly at construction instead of
    silently dropping data (the DAT-880 review finding: a silently-ignored key
    on a live wire shape is a worse failure mode than the shim it replaced).
    """

    model_config = ConfigDict(extra="forbid")

    validation_id: str
    name: str
    description: str
    category: str  # 'financial', 'data_quality', 'business_rule'
    severity: ValidationSeverity = ValidationSeverity.ERROR

    # Typed check definition (DAT-735). A union, not the bare ValidationCheckType
    # enum (DAT-880): the DAT-447 `expected_formula` teach overlay rides this field
    # with a fifth, documented value OUTSIDE the four-value contract — a column-
    # identity declaration, not an evaluator branch (see `expected_formula` below).
    # The typed `validations` home still CHECK-enforces the closed four (balance |
    # comparison | constraint | aggregate — the cockpit CHECK_TYPES contract);
    # `expected_formula` rows never reach that table, only the overlay ⊕ layer. This
    # union is a LABEL the ADR-0017 evaluator never branches on.
    check_type: ValidationCheckType | Literal["expected_formula"]
    tolerance: float | None = None  # ADR-0017 pass threshold; None ⇒ DEFAULT_TOLERANCE

    # Advisory SQL-binding hint prose (the former sql_hints) + what a pass looks like.
    guidance: str | None = None
    expected_outcome: str | None = None

    # The DAT-447 column-identity declaration (DAT-880): present iff
    # check_type == "expected_formula" (enforced below, loud on mismatch — there is
    # no partial-declaration state). See ExpectedFormulaDeclaration.
    expected_formula: ExpectedFormulaDeclaration | None = None

    # Metadata
    tags: list[str] = Field(default_factory=list)
    relevant_cycles: list[str] = Field(
        default_factory=list
    )  # cycle types this applies to; empty = universal
    # Convention ids (= `Convention.name`, the prompt-facing id) this check's LOGIC
    # relies on — the typed
    # validation→convention dependency (DAT-865). The SQL binder receives exactly
    # these (∪ the convention-side `targets` routing), so a judgment the check
    # depends on (e.g. a sign rule) arrives declared, never re-guessed at bind
    # time. Declared by induction (membership-validated) or the seed YAML; empty =
    # only targets-routed conventions reach the binder.
    relevant_conventions: list[str] = Field(default_factory=list)
    version: str = "1.0"
    source: str = "config"

    @model_validator(mode="before")
    @classmethod
    def _fold_legacy_check_fields(cls, data: Any) -> Any:
        """Fold the LIVE ``parameters`` + ``sql_hints`` wire shape onto the typed fields.

        LIVE, not a shim for a retired design (DAT-880 review correction: the
        ticket's premise — "the expected_formula teach overlay is the one
        remaining producer" — miscounted the SECOND, unrelated producer). The
        cockpit's frame INDUCTION path still emits exactly this shape for the
        four CANONICAL check types: ``validation-induction.ts``'s
        ``InducedValidation`` schema is constrained-decoding shaped as
        ``parameters`` (an array) + ``sql_hints`` (a string) — its own header
        comment documents why it was never migrated alongside DAT-735's
        ``ValidationSpecSchema`` — and ``toProposedValidation`` folds the array
        into a ``{name: value}`` map and spreads ``sql_hints`` verbatim into the
        overlay payload ``frame.ts``'s induce path writes straight to
        ``config_overlay`` WITHOUT ever running it through
        ``ValidationSpecSchema.parse`` (only the separate user-edited
        ``opts.edited`` declare path does). Deleting this fold silently strips
        ``tolerance``/``guidance`` from every frame-induced validation — probe-
        verified by two independent reviewers (tolerance falls to
        ``DEFAULT_TOLERANCE``, a 10x-looser gate; guidance empties the
        ``sql_hints`` prompt slot).

        This fold retires WITH the cockpit-migration follow-on that retypes
        ``InducedValidation`` to the typed ``tolerance``/``guidance`` shape
        (DAT-880 follow-on — a semantically-graded prompt change needing a live
        constrained-decoding compile probe, lead-gated, out of this lane's
        budget) — NOT before. It has no ``expected_formula`` branch and needs
        none: the DAT-447 declaration rides the typed ``expected_formula``
        submodel instead (above), and a row naming that check_type never
        carries ``parameters``/``sql_hints`` in the first place — the two wire
        shapes are disjoint by construction.

        Normalization (unchanged from the pre-DAT-880 shape): ``parameters.
        tolerance`` → ``tolerance``; ``sql_hints`` → ``guidance``, with any
        NON-tolerance ``parameters`` folded into ``guidance``. Explicit typed
        fields always win over the legacy inference. Runs before ``extra=
        "forbid"`` is enforced, so the legacy keys are consumed here, never
        seen as unrecognized fields.
        """
        if not isinstance(data, dict):
            return data
        data = dict(data)
        params = data.pop("parameters", None)
        sql_hints = data.pop("sql_hints", None)
        if data.get("tolerance") is None and isinstance(params, dict) and "tolerance" in params:
            data["tolerance"] = params["tolerance"]
        if data.get("guidance") is None:
            parts: list[str] = []
            if sql_hints:
                parts.append(str(sql_hints))
            extra = (
                {k: v for k, v in params.items() if k != "tolerance"}
                if isinstance(params, dict)
                else {}
            )
            if extra:
                parts.append("Parameters: " + json.dumps(extra))
            data["guidance"] = "\n\n".join(parts) if parts else None
        return data

    @model_validator(mode="after")
    def _check_expected_formula_pairing(self) -> ValidationSpec:
        """Enforce the ``check_type``/``expected_formula`` pairing (DAT-880).

        ``expected_formula`` is not an optional add-on to any check — it IS the
        ``check_type == "expected_formula"`` row's declaration, always. A row
        naming that check_type with no declaration (or vice versa) is malformed
        data, not a valid partial state — fail loudly at construction. Absence
        falls loud PER ROW: the one caller that parses arbitrary overlay-merged
        data (``analysis.validation.config.load_all_validation_specs``) catches
        this ``ValidationError`` and skips just that row (logged), mirroring
        ``ensure_validations_seeded``'s per-doc isolation — one malformed teach
        must not take the whole vocabulary load, or the phase it feeds, down.
        """
        is_formula_check = self.check_type == "expected_formula"
        has_declaration = self.expected_formula is not None
        if is_formula_check != has_declaration:
            raise ValueError(
                "check_type='expected_formula' requires expected_formula to be set, "
                "and vice versa — got "
                f"check_type={self.check_type!r}, expected_formula={self.expected_formula!r}"
            )
        return self


class ValidationSQLOutput(BaseModel):
    """The ``validation_sql`` structured output.

    Every field is REQUIRED (DAT-807). ``sql`` and ``skip_reason`` are the
    either/or pair — exactly one is populated and the other is "" — modelled as
    two required strings rather than a union, because a union spends one of the
    request's 16 union slots to express what a documented sentinel expresses for
    free. ``can_validate`` remains the discriminator.
    """

    sql: str = Field(
        description=(
            'The DuckDB SQL query to execute; "" when the validation cannot be '
            "performed (can_validate false)."
        )
    )
    # No free-text `explanation` field: it flowed into GeneratedSQL and was read by
    # nothing (DAT-603 consumer audit) — an unread sentence per call is pure
    # serial-decode latency. The judgeable context lives in columns_used +
    # skip_reason; the spec itself already says what is being validated.
    columns_used: list[str] = Field(
        description="Columns used in the query, in 'table.column' format; [] when none.",
    )
    can_validate: bool = Field(
        description="Whether the validation can be performed with the available schema."
    )
    skip_reason: str = Field(
        description=(
            "If can_validate is false, explain why (e.g., 'Missing required "
            'columns: ...\'); "" when can_validate is true.'
        ),
    )


class GeneratedSQL(BaseModel):
    """LLM-generated SQL for a validation check."""

    validation_id: str
    sql_query: str
    columns_used: list[str] = Field(default_factory=list)  # Columns identified by LLM

    # Generation metadata
    generated_at: datetime = Field(default_factory=_utc_now)
    model_used: str | None = None

    # Validation info
    is_valid: bool = True
    validation_error: str | None = None


class ValidationResult(BaseModel):
    """Result of executing a validation check."""

    validation_id: str
    spec_name: str
    status: ValidationStatus
    severity: ValidationSeverity

    # Execution details
    table_ids: list[str] = Field(default_factory=list)
    table_name: str
    executed_at: datetime = Field(default_factory=_utc_now)

    # Results
    passed: bool = False
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)

    # SQL execution
    sql_used: str | None = None
    columns_used: list[str] = Field(default_factory=list)  # Columns LLM identified
    result_rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0


class ValidationRunResult(BaseModel):
    """Result of running all validations across tables."""

    run_id: str
    table_ids: list[str] = Field(default_factory=list)
    table_name: str
    started_at: datetime = Field(default_factory=_utc_now)
    completed_at: datetime | None = None

    # Results
    results: list[ValidationResult] = Field(default_factory=list)
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    skipped_checks: int = 0
    error_checks: int = 0

    # Summary
    overall_status: ValidationStatus = ValidationStatus.PASSED
    has_critical_failures: bool = False

    @classmethod
    def from_results(
        cls,
        *,
        run_id: str,
        table_ids: list[str],
        table_name: str,
        started_at: datetime,
        results: list[ValidationResult],
    ) -> ValidationRunResult:
        """Summarize a run's individual results into the aggregate.

        ``run_id`` is the workflow-minted run (DAT-408), never minted here.
        """
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        skipped = sum(1 for r in results if r.status == ValidationStatus.SKIPPED)
        errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)
        return cls(
            run_id=run_id,
            table_ids=table_ids,
            table_name=table_name,
            started_at=started_at,
            completed_at=_utc_now(),
            results=results,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            skipped_checks=skipped,
            error_checks=errors,
            # ``overall_status`` collapses to FAILED on either a judged data
            # failure OR an inconclusive/errored check (``errors`` now
            # includes inconclusive evaluations, DAT-439) — it is a coarse
            # "not all-clean" flag, NOT a pure data-failure signal. A
            # cockpit/readiness consumer that needs to distinguish "data is
            # wrong" from "couldn't judge" must read the per-check
            # ``failed_checks`` vs ``error_checks`` axes, not this rollup
            # (a DEGRADED/INCONCLUSIVE overall state is DAT-440+).
            overall_status=(
                ValidationStatus.FAILED if (failed or errors) else ValidationStatus.PASSED
            ),
            has_critical_failures=any(
                r.status == ValidationStatus.FAILED and r.severity == ValidationSeverity.CRITICAL
                for r in results
            ),
        )


__all__ = [
    "ExpectedFormulaDeclaration",
    "ValidationCheckType",
    "ValidationSeverity",
    "ValidationStatus",
    "ValidationSpec",
    "ValidationSQLOutput",
    "GeneratedSQL",
    "ValidationResult",
    "ValidationRunResult",
]
