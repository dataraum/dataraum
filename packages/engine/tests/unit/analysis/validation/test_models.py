"""ValidationSpec — the typed check definition + legacy normalizer (DAT-735).

Pins the ``mode="before"`` normalizer that maps the legacy YAML + cockpit teach-overlay
wire shape (``parameters``/``sql_hints``, a live cross-package contract) onto the typed
``tolerance``/``guidance`` fields, and the "explicit typed fields always win" rule.
"""

from __future__ import annotations

import json

from dataraum.analysis.validation.models import ValidationSpec


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


def test_legacy_parameters_tolerance_maps_to_typed_tolerance() -> None:
    spec = _spec(parameters={"tolerance": 0.05})
    assert spec.tolerance == 0.05


def test_legacy_sql_hints_maps_to_guidance() -> None:
    spec = _spec(sql_hints="sum the debits")
    assert spec.guidance == "sum the debits"


def test_non_tolerance_params_fold_into_guidance() -> None:
    """Non-tolerance parameters (LLM hints) survive into guidance — the binding agent
    used to get them as a JSON blob; the fold keeps it equally informed."""
    spec = _spec(
        sql_hints="classify accounts", parameters={"tolerance": 0.01, "asset_types": ["a"]}
    )
    assert spec.tolerance == 0.01
    assert "classify accounts" in (spec.guidance or "")
    assert "asset_types" in (spec.guidance or "")
    # The folded block is valid JSON of the non-tolerance params.
    folded = spec.guidance.split("Parameters: ", 1)[1]  # type: ignore[union-attr]
    assert json.loads(folded) == {"asset_types": ["a"]}


def test_explicit_typed_fields_win_over_legacy() -> None:
    """An explicit tolerance/guidance always wins; the legacy fields are dropped."""
    spec = _spec(
        tolerance=0.2,
        guidance="explicit prose",
        parameters={"tolerance": 0.9, "asset_types": ["x"]},
        sql_hints="legacy prose",
    )
    assert spec.tolerance == 0.2
    # Explicit guidance is kept verbatim — the legacy sql_hints/params are NOT folded in.
    assert spec.guidance == "explicit prose"


def test_new_shape_is_a_noop() -> None:
    """A row already in the typed shape passes through unchanged (DB-home read)."""
    spec = _spec(tolerance=0.0, guidance="g")
    assert spec.tolerance == 0.0
    assert spec.guidance == "g"


def test_no_check_fields_leave_both_none() -> None:
    spec = _spec()
    assert spec.tolerance is None
    assert spec.guidance is None
    # The retired fields are not exposed on the typed model.
    assert not hasattr(spec, "parameters")
    assert not hasattr(spec, "sql_hints")
