"""ValidationSpec — the typed check definition (DAT-735; retyped DAT-880).

Pins the ``check_type`` union (``ValidationCheckType | Literal["expected_formula"]``)
and the ``check_type``/``expected_formula`` pairing invariant that replaced the
``mode="before"`` legacy normalizer (DAT-880 deleted it entirely — no
``parameters``/``sql_hints`` wire shape exists anymore; a row still carrying those
keys is silently ignored as an unrecognized extra field, never folded).
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
    """The typed home's shape (tolerance/guidance) passes through unchanged."""
    spec = _spec(tolerance=0.0, guidance="g")
    assert spec.tolerance == 0.0
    assert spec.guidance == "g"


def test_no_check_fields_leave_tolerance_and_guidance_none() -> None:
    spec = _spec()
    assert spec.tolerance is None
    assert spec.guidance is None
    assert spec.expected_formula is None


def test_legacy_parameters_and_sql_hints_are_silently_ignored() -> None:
    """No wire shape reads these anymore — DAT-880 deleted the fold with no shim.

    Unrecognized keys are dropped as ordinary extra fields (pydantic's default),
    not folded into tolerance/guidance and not exposed as attributes.
    """
    spec = _spec(parameters={"tolerance": 0.5}, sql_hints="legacy prose")
    assert spec.tolerance is None
    assert spec.guidance is None
    assert not hasattr(spec, "parameters")
    assert not hasattr(spec, "sql_hints")


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
