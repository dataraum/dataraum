"""The semantic_per_table context surfaces the value-carried unit (DAT-647).

_format_persisted_annotations must render `value_unit=<unit>` when the typing
phase detected one, so the table agent can resolve unit_source_column instead of
treating the measure's unit as unknown.
"""

from dataraum.analysis.semantic.agent import SemanticAgent


def test_value_unit_rendered_when_present():
    annotations = [
        {
            "table_name": "orders",
            "column_name": "total_eur",
            "semantic_role": "measure",
            "confidence": 0.9,
            "detected_unit": "EUR",
        },
    ]

    formatted = SemanticAgent._format_persisted_annotations(annotations)

    assert "value_unit=EUR" in formatted


def test_value_unit_omitted_when_absent():
    annotations = [
        {
            "table_name": "journal_lines",
            "column_name": "debit",
            "semantic_role": "measure",
            "confidence": 0.9,
            "detected_unit": None,
        },
    ]

    formatted = SemanticAgent._format_persisted_annotations(annotations)

    assert "value_unit" not in formatted
    assert "debit" in formatted
