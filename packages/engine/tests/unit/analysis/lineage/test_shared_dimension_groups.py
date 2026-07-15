"""Shared-dimension grouping for aggregation lineage (DAT-756 + DAT-800).

Pins the pure grouping function: referenced identities pair on
(dimension_table_id, dimension_attribute); judge-conformed FOLDED bus-matrix
cells form identities keyed by the conform pass's concept label, served by the
facts' own folded SliceDefinitions; a conformed fold whose key column was
never sliced abstains for that fact rather than guessing.
"""

from __future__ import annotations

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
from dataraum.analysis.lineage.processor import _shared_dimension_groups
from dataraum.analysis.slicing.db_models import SliceDefinition

RUN = "run-1"


def _slice(
    table_id: str,
    column_name: str,
    *,
    dim: str | None = None,
    attr: str | None = None,
) -> SliceDefinition:
    return SliceDefinition(
        run_id=RUN,
        table_id=table_id,
        column_id=f"{table_id}:{column_name}",
        column_name=column_name,
        dimension_table_id=dim,
        dimension_attribute=attr,
        slice_priority=1,
        slice_type="categorical",
        detection_source="llm",
    )


def _cell(fact: str, key: str, label: str) -> BusMatrixEntry:
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="folded",
        concept_label=label,
        roles=[key],
        attributes=[],
        confirmation_source="judge",
        signature=f"bus:folded:{fact}:{key}",
    )


def test_referenced_identity_pairs_across_facts() -> None:
    defs = [
        _slice("f1", "account_fk", dim="d1", attr="type"),
        _slice("f2", "acct", dim="d1", attr="type"),
        _slice("f2", "region", dim="d2", attr=None),  # only one fact — singleton
    ]
    groups, folded_labels = _shared_dimension_groups(defs, [])
    assert set(groups[("d1", "type")]) == {"f1", "f2"}
    assert set(groups[("d2", "")]) == {"f2"}
    assert folded_labels == {}


def test_conformed_folded_cells_form_an_identity() -> None:
    defs = [
        _slice("gl", "account_id"),  # folded slices: no dimension_table_id
        _slice("tb", "account_id"),
    ]
    cells = [_cell("gl", "account_id", "account"), _cell("tb", "account_id", "account")]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    identity = ("folded:account", "")
    assert set(groups[identity]) == {"gl", "tb"}
    assert folded_labels[identity] == "account"
    # the lens objects ARE the facts' own folded slices
    assert groups[identity]["gl"][0].column_name == "account_id"


def test_unsliced_fold_key_abstains_for_that_fact() -> None:
    defs = [_slice("gl", "account_id")]  # tb's fold key was never sliced
    cells = [_cell("gl", "account_id", "account"), _cell("tb", "account_id", "account")]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups[("folded:account", "")]) == {"gl"}  # singleton — caller filters


def test_referenced_slice_never_serves_a_folded_identity() -> None:
    # A same-named REFERENCED slice must not be borrowed as a folded lens.
    defs = [_slice("gl", "account_id", dim="d1"), _slice("tb", "account_id")]
    cells = [_cell("gl", "account_id", "account"), _cell("tb", "account_id", "account")]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups.get(("folded:account", ""), {})) == {"tb"}
    assert set(groups[("d1", "")]) == {"gl"}


def test_distinct_labels_stay_distinct_axes() -> None:
    defs = [_slice("gl", "account_id"), _slice("bt", "payment_id")]
    cells = [_cell("gl", "account_id", "account"), _cell("bt", "payment_id", "vendor payment")]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert ("folded:account", "") in groups and ("folded:vendor payment", "") in groups
    assert len(folded_labels) == 2
