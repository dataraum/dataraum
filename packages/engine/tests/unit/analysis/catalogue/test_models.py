"""The catalogue_semantics output contract (DAT-823).

Strict schema discipline (feedback-llm-schema-no-optionals): every field
required, absence via the "" sentinel — the same contract the per-table tier
carried before the rebalance moved authoring here — plus the persisted-status
vocabulary pin (``MEANING_STATUSES`` ↔ the ``determination`` Literal).
"""

from __future__ import annotations

from typing import get_args

import pytest
from pydantic import ValidationError

from dataraum.analysis.catalogue.models import (
    MEANING_STATUSES,
    CatalogueSemanticsOutput,
    ColumnConceptOutput,
    MeaningStatus,
)


def test_no_optional_fields_anywhere() -> None:
    """Optional is usually a modelling mistake — cut it or require it.

    Under constrained decoding every optional spends one of the request's 24
    optional-parameter slots; the not-applicable case is a documented empty
    value ("" / 0.0), never an omitted key (the DAT-672 class).
    """
    schema = CatalogueSemanticsOutput.model_json_schema()
    for owner, node in [("CatalogueSemanticsOutput", schema), *schema["$defs"].items()]:
        props = node.get("properties")
        if not props:
            continue
        optional = set(props) - set(node.get("required", []))
        assert not optional, f"{owner} has optional fields: {sorted(optional)}"


def test_omitting_a_required_field_raises() -> None:
    """Omission is a validation error — the signal the repair/parse path keys on."""
    with pytest.raises(ValidationError):
        CatalogueSemanticsOutput.model_validate({"table_readings": []})
    with pytest.raises(ValidationError):
        ColumnConceptOutput.model_validate(
            {
                "table_name": "t",
                "column_name": "c",
                "meaning": "m",
                # determination omitted
                "unit_source_column": "",
                "derived_formula_hypothesis": "",
                "derived_formula_confidence": 0.0,
            }
        )


def test_meaning_statuses_pins_the_determination_literal() -> None:
    """The DB CHECK vocabulary and the output Literal can never drift.

    ``MEANING_STATUSES`` is the single home the ``ck_column_concepts_
    meaning_status`` CHECK derives from (semantic/db_models.py); the LLM-facing
    ``determination`` field commits to the same values through ``MeaningStatus``.
    """
    assert set(MEANING_STATUSES) == set(get_args(MeaningStatus))
    assert MEANING_STATUSES == tuple(sorted(MEANING_STATUSES))  # deterministic DDL


def test_determination_rejects_unknown_values() -> None:
    with pytest.raises(ValidationError):
        ColumnConceptOutput(
            table_name="t",
            column_name="c",
            meaning="m",
            determination="maybe",  # type: ignore[arg-type]
            unit_source_column="",
            derived_formula_hypothesis="",
            derived_formula_confidence=0.0,
        )
