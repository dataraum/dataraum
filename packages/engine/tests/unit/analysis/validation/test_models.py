"""ValidationSpec — the typed check definition + the LIVE legacy fold (DAT-735;
retyped DAT-880).

Pins three things: the ``check_type`` union (``ValidationCheckType |
Literal["expected_formula"]``), the ``check_type``/``expected_formula`` pairing
invariant (the DAT-447 declaration's typed shape), and the ``mode="before"``
fold that maps the ``parameters``/``sql_hints`` wire shape onto the typed
``tolerance``/``guidance`` fields.

DAT-880 REVIEW CORRECTION: the ticket's premise was that this fold's only
remaining producer (the DAT-447 expected_formula overlay) had no live writer, so
the whole fold could be deleted. That was correct about expected_formula — and
WRONG about the fold overall: the cockpit's frame INDUCTION path
(``validation-induction.ts``'s ``InducedValidation`` schema, filled by
``getFrameValidationsInstructions()``'s cached LLM instructions) still emits
this exact wire shape for the four CANONICAL check types, and its induce path
writes straight to ``config_overlay`` without ever validating against the typed
``ValidationSpecSchema``. Deleting the fold would have silently stripped
tolerance + guidance from every frame-induced validation. The fold stays until
that cockpit-side schema is migrated (a follow-on, lead-gated on a live
constrained-decoding compile probe) — see ``_fold_legacy_check_fields``'s
docstring on the model for the full producer chain.
"""

from __future__ import annotations

import json

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
    """The typed home's shape (tolerance/guidance) passes through unchanged —
    the DB-home read never carries legacy keys, so the fold is a no-op."""
    spec = _spec(tolerance=0.0, guidance="g")
    assert spec.tolerance == 0.0
    assert spec.guidance == "g"


def test_no_check_fields_leave_tolerance_and_guidance_none() -> None:
    spec = _spec()
    assert spec.tolerance is None
    assert spec.guidance is None
    assert spec.expected_formula is None


class TestLegacyFold:
    """The LIVE fold — frame induction's producer contract, not a retired shim."""

    def test_legacy_parameters_tolerance_maps_to_typed_tolerance(self) -> None:
        spec = _spec(parameters={"tolerance": 0.05})
        assert spec.tolerance == 0.05

    def test_legacy_sql_hints_maps_to_guidance(self) -> None:
        spec = _spec(sql_hints="sum the debits")
        assert spec.guidance == "sum the debits"

    def test_non_tolerance_params_fold_into_guidance(self) -> None:
        """Non-tolerance parameters (LLM classification hints, e.g. asset_types)
        survive into guidance — the binding agent gets them as a JSON blob."""
        spec = _spec(
            sql_hints="classify accounts", parameters={"tolerance": 0.01, "asset_types": ["a"]}
        )
        assert spec.tolerance == 0.01
        assert "classify accounts" in (spec.guidance or "")
        assert "asset_types" in (spec.guidance or "")
        folded = spec.guidance.split("Parameters: ", 1)[1]  # type: ignore[union-attr]
        assert json.loads(folded) == {"asset_types": ["a"]}

    def test_explicit_typed_fields_win_over_legacy(self) -> None:
        """An explicit tolerance/guidance always wins; the legacy fields are dropped."""
        spec = _spec(
            tolerance=0.2,
            guidance="explicit prose",
            parameters={"tolerance": 0.9, "asset_types": ["x"]},
            sql_hints="legacy prose",
        )
        assert spec.tolerance == 0.2
        assert spec.guidance == "explicit prose"

    def test_legacy_keys_never_survive_as_attributes(self) -> None:
        """The fold CONSUMES parameters/sql_hints before field validation — they
        are never exposed on the typed model, fold or no fold."""
        spec = _spec(parameters={"tolerance": 0.5}, sql_hints="x")
        assert not hasattr(spec, "parameters")
        assert not hasattr(spec, "sql_hints")

    def test_frame_induced_payload_shape(self) -> None:
        """Pins the EXACT wire shape ``toProposedValidation``
        (validation-induction.ts) produces from a frame induction turn — the
        probe both reviewers ran independently, reproduced here byte-for-byte:
        a numeric ``tolerance`` parameter (0.001 — the value that demonstrated a
        10x-looser gate than the intended threshold once the fold was deleted)
        plus a classification-hint parameter, and free-text ``sql_hints``. The
        fold must recover the induced tolerance EXACTLY and compose guidance
        from both the hint prose and the leftover parameter."""
        frame_induced_payload = {
            "validation_id": "trial_balance",
            "name": "Trial Balance",
            "description": "Assets + expenses equal liabilities + equity + revenue",
            "category": "financial",
            "check_type": "balance",
            "sql_hints": "Classify accounts by asset_types before summing.",
            "parameters": {"tolerance": 0.001, "asset_types": ["asset", "assets"]},
        }
        spec = ValidationSpec.model_validate(frame_induced_payload)
        assert spec.tolerance == 0.001
        assert "Classify accounts by asset_types before summing." in (spec.guidance or "")
        assert "asset_types" in (spec.guidance or "")


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
    """DAT-880: once the fold consumes the one live legacy shape's keys, any OTHER
    unrecognized key is a real unknown field — fails loud, never silently dropped."""
    with pytest.raises(ValidationError):
        _spec(this_key_has_never_existed="x")
