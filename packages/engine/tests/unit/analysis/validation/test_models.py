"""ValidationSpec — the typed check definition, ONE wire shape (DAT-735;
retyped DAT-880).

Pins three things: the ``check_type`` union (``ValidationCheckType |
Literal["expected_formula"]``), the ``check_type``/``expected_formula`` pairing
invariant (the DAT-447 declaration's typed shape), and — since DAT-880's
close-out — that the typed fields are the ONLY accepted wire shape.

WHAT REPLACED WHAT, because the history is the reason these tests are worded
this way. A ``mode="before"`` fold used to map a ``parameters``/``sql_hints``
payload onto ``tolerance``/``guidance``. DAT-880 tried to delete it as dead; a
review caught that the cockpit's frame INDUCTION path was a second, live
producer of that shape, writing straight to ``config_overlay`` without ever
validating against the typed ``ValidationSpecSchema`` — so deleting the fold
would have silently stripped tolerance + guidance from every frame-induced
check. The close-out migrated that producer instead
(``validation-induction.ts``'s ``InducedValidation`` and the cached
``getFrameValidationsInstructions()`` block, verified against the live
constrained-decoding compiler), and THEN deleted the fold.

So the silent-strip failure mode is not merely absent, it is unreachable:
``extra="forbid"`` means a payload still carrying the legacy keys raises at
construction. ``TestNativeTypedWireShape`` below pins exactly that, and it is
what a future re-deletion of the migration would trip on.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataraum.analysis.validation.models import ExpectedFormulaDeclaration, ValidationSpec


def _spec(**overrides) -> ValidationSpec:
    base = {
        "validation_id": "v",
        "name": "V",
        "description": "d",
        "category": "c",
        "check_type": "balance",
    }
    base.update(overrides)
    return ValidationSpec(**base)


def test_typed_fields_construct_directly() -> None:
    """The typed shape passes through unchanged — the one shape every producer
    writes (the DB home, the teach overlay, frame induction).

    ``tolerance=0.0`` specifically: the cockpit's ``-1`` sentinel decodes to an
    ABSENT property, so a 0 arriving here is a real claim (exact agreement, or
    zero violating rows) and must not collapse to the evaluator's default."""
    spec = _spec(tolerance=0.0, guidance="g")
    assert spec.tolerance == 0.0
    assert spec.guidance == "g"


def test_no_check_fields_leave_tolerance_and_guidance_none() -> None:
    spec = _spec()
    assert spec.tolerance is None
    assert spec.guidance is None
    assert spec.expected_formula is None


class TestNativeTypedWireShape:
    """The ONE accepted wire shape — and the structural guarantee behind it."""

    def test_frame_induced_payload_shape(self) -> None:
        """Pins the EXACT payload ``toProposedValidation``
        (validation-induction.ts) produces from a frame induction turn, taken
        from the live compile probe that gated the migration: a typed
        ``tolerance`` and free-text ``guidance`` carrying the classification
        vocabulary the retired ``parameters`` list used to hold. 0.001 is kept
        from the pre-migration pin deliberately — it is the value that
        demonstrated a 10x-looser gate than intended back when this field could
        go missing, so it is the one that proves it no longer can."""
        frame_induced_payload = {
            "validation_id": "trial_balance",
            "name": "Trial Balance",
            "description": "Assets + expenses equal liabilities + equity + revenue",
            "category": "financial",
            "check_type": "balance",
            "tolerance": 0.001,
            "guidance": "Classify by account_type in ('asset','assets') before summing.",
        }
        spec = ValidationSpec.model_validate(frame_induced_payload)
        assert spec.tolerance == 0.001
        assert "account_type in ('asset','assets')" in (spec.guidance or "")

    def test_legacy_parameters_key_fails_loud(self) -> None:
        """The structural guarantee: with no fold left, ``extra="forbid"`` makes
        a residual legacy payload raise at construction instead of silently
        dropping the tolerance it carried. Re-introducing the pre-DAT-880
        induction schema fails HERE, loudly, rather than in production as a
        check quietly graded at DEFAULT_TOLERANCE."""
        with pytest.raises(ValidationError):
            _spec(parameters={"tolerance": 0.05})

    def test_legacy_sql_hints_key_fails_loud(self) -> None:
        """Same guarantee for the guidance half — a dropped ``sql_hints`` used to
        empty the SQL-binding prompt slot with no signal at all."""
        with pytest.raises(ValidationError):
            _spec(sql_hints="sum the debits")


@pytest.mark.parametrize(
    "value", ["balance", "comparison", "constraint", "aggregate", "expected_formula"]
)
def test_check_type_accepts_every_legal_value(value: str) -> None:
    kwargs = {"check_type": value}
    if value == "expected_formula":
        kwargs["expected_formula"] = {"table": "orders", "column": "total", "formula": "a + b"}
    spec = _spec(**kwargs)
    assert spec.check_type == value


def test_check_type_rejects_an_unknown_value() -> None:
    """The union is closed to the four canonical values + the one documented
    expected_formula sentinel — anything else is malformed data, not a new type."""
    with pytest.raises(ValidationError):
        _spec(check_type="bogus")


def test_expected_formula_declaration_parses_typed() -> None:
    spec = _spec(
        check_type="expected_formula",
        expected_formula={"table": "orders", "column": "total", "formula": "subtotal + tax"},
    )
    assert spec.expected_formula == ExpectedFormulaDeclaration(
        table="orders", column="total", formula="subtotal + tax"
    )


def test_expected_formula_check_type_without_declaration_is_rejected() -> None:
    """check_type='expected_formula' with no declaration is malformed — there is no
    partial-declaration state; the SQL binder must never see an empty claim."""
    with pytest.raises(ValidationError):
        _spec(check_type="expected_formula")


def test_expected_formula_declaration_without_matching_check_type_is_rejected() -> None:
    """A stray declaration on an ordinary check is equally malformed."""
    with pytest.raises(ValidationError):
        _spec(
            check_type="balance",
            expected_formula={"table": "orders", "column": "total", "formula": "a + b"},
        )


def test_extra_forbid_rejects_a_genuinely_unknown_field() -> None:
    """DAT-880: every unrecognized key is a real unknown field — fails loud, never
    silently dropped. The legacy-key cases above are one instance of this rule."""
    with pytest.raises(ValidationError):
        _spec(this_key_has_never_existed="x")
