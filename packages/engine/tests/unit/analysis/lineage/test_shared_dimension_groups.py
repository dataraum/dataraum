"""Shared-dimension grouping for aggregation lineage (DAT-756 + DAT-800).

Pins the pure grouping function: referenced identities pair on
(dimension_table_id, dimension_attribute); judge-conformed FOLDED bus-matrix
cells form identities keyed by ``conformed_group`` — the conform-connected
component's signature, NEVER the concept label: a label collision across
distinct groups must not merge them (it would discard a DISTINCT verdict) and
label drift inside one group must not split it. A conformed fold whose key
column was never sliced abstains for that fact rather than guessing.
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


def _cell(fact: str, key: str, label: str, group: str | None) -> BusMatrixEntry:
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="folded",
        concept_label=label,
        roles=[key],
        attributes=[],
        confirmation_source="judge",
        conformed_group=group,
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
    g = "conform:gl:account_id|tb:account_id"
    cells = [_cell("gl", "account_id", "account", g), _cell("tb", "account_id", "account", g)]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    identity = (f"folded:{g}", "")
    assert set(groups[identity]) == {"gl", "tb"}
    assert folded_labels[identity] == "account"
    # the lens objects ARE the facts' own folded slices
    assert groups[identity]["gl"][0].column_name == "account_id"


def test_unsliced_fold_key_abstains_for_that_fact() -> None:
    defs = [_slice("gl", "account_id")]  # tb's fold key was never sliced
    g = "conform:gl:account_id|tb:account_id"
    cells = [_cell("gl", "account_id", "account", g), _cell("tb", "account_id", "account", g)]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g}", "")]) == {"gl"}  # singleton — caller filters


def test_referenced_slice_never_serves_a_folded_identity() -> None:
    # A same-named REFERENCED slice must not be borrowed as a folded lens.
    defs = [_slice("gl", "account_id", dim="d1"), _slice("tb", "account_id")]
    g = "conform:gl:account_id|tb:account_id"
    cells = [_cell("gl", "account_id", "account", g), _cell("tb", "account_id", "account", g)]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups.get((f"folded:{g}", ""), {})) == {"tb"}
    assert set(groups[("d1", "")]) == {"gl"}


def test_label_collision_across_groups_never_merges() -> None:
    # The judge said DISTINCT (two separate components) but both got the same
    # generic label — grouping by label would silently merge them.
    defs = [_slice("gl", "status"), _slice("bt", "state")]
    g1 = "conform:gl:status|iv:status"
    g2 = "conform:bt:state|pm:state"
    cells = [_cell("gl", "status", "status", g1), _cell("bt", "state", "status", g2)]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g1}", "")]) == {"gl"}
    assert set(groups[(f"folded:{g2}", "")]) == {"bt"}
    assert len(folded_labels) == 2  # two identities, both displaying "status"
    assert set(folded_labels.values()) == {"status"}


def test_one_group_is_one_axis_regardless_of_labels() -> None:
    # Cells of ONE conform component always form one axis; the conform pass
    # canonicalizes the label, and the grouping never re-splits on it.
    defs = [_slice("gl", "account_id"), _slice("tb", "acct_no")]
    g = "conform:gl:account_id|tb:acct_no"
    cells = [_cell("gl", "account_id", "account", g), _cell("tb", "acct_no", "account", g)]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g}", "")]) == {"gl", "tb"}
    assert folded_labels[(f"folded:{g}", "")] == "account"


def test_unconformed_cell_contributes_nothing() -> None:
    # conformed_group is None → no cross-fact identity was asserted.
    defs = [_slice("gl", "account_id")]
    cells = [_cell("gl", "account_id", "account_id", None)]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert groups == {}
    assert folded_labels == {}
