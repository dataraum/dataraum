"""Shared-dimension grouping for aggregation lineage (DAT-756 + DAT-800 + DAT-788).

Pins the pure grouping function. The identity key is
``(dimension_table_id, dimension_attribute, role_identity)``:

- REFERENCED identities pair on the dim table + attribute AND the DAT-788 role
  identity — the ``conformed_group`` the bus-matrix referenced cell carries.
  Role-playing FKs to one dim (bill-to vs ship-to) are SEPARATE identities unless
  the conform judge merged their roles; same-named FK roles share an identity
  structurally (a slice with no cell falls back to that structural signature).
- Judge-conformed FOLDED bus-matrix cells form identities keyed by
  ``conformed_group`` — the conform-connected component's signature, NEVER the
  concept label.
"""

from __future__ import annotations

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
from dataraum.analysis.lineage.processor import _shared_dimension_groups
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.storage.base import load_all_models

# Instantiating a mapped class configures the mapper registry, which needs EVERY
# model imported (Column → TemporalColumnProfile, …). This file only imports two
# models directly, so register them all — a no-op when a sibling already did.
load_all_models()

RUN = "run-1"


def _slice(
    table_id: str,
    column_name: str,
    *,
    dim: str | None = None,
    attr: str | None = None,
    fk_role: str | None = None,
) -> SliceDefinition:
    return SliceDefinition(
        run_id=RUN,
        table_id=table_id,
        column_id=f"{table_id}:{column_name}",
        column_name=column_name,
        dimension_table_id=dim,
        dimension_attribute=attr,
        fk_role=fk_role,
        slice_priority=1,
        slice_type="categorical",
        detection_source="llm",
    )


def _ref_cell(
    fact: str,
    dim: str,
    roles: list[str],
    group: str,
    *,
    needs_confirmation: bool = False,
) -> BusMatrixEntry:
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="referenced",
        concept_label=dim,
        dimension_table_id=dim,
        roles=roles,
        attributes=[],
        confirmation_source="unconfirmed",
        conformed_group=group,
        needs_confirmation=needs_confirmation,
        signature=f"bus:referenced:{fact}:{dim}:" + "|".join(roles),
    )


def _folded_cell(fact: str, key: str, label: str, group: str | None) -> BusMatrixEntry:
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


# --- Referenced role identity (DAT-788) --------------------------------------


def test_same_named_fk_roles_pair_structurally() -> None:
    # The DAT-756 common case: two facts reach one dim via the SAME FK role name.
    # They share the structural fallback identity even with no referenced cell.
    defs = [
        _slice("f1", "account_id", dim="d1", attr="type", fk_role="account_id"),
        _slice("f2", "account_id", dim="d1", attr="type", fk_role="account_id"),
    ]
    groups, _ = _shared_dimension_groups(defs, [])
    assert set(groups[("d1", "type", "ref:d1:account_id")]) == {"f1", "f2"}


def test_conform_merges_differently_named_roles() -> None:
    # The judge conformed two differently-named FK roles into ONE group; both
    # facts' cells carry the same conformed_group → ONE identity.
    g = "ref:d1:billto|invoice"
    defs = [
        _slice("f1", "billto", dim="d1", attr="type", fk_role="billto"),
        _slice("f2", "invoice", dim="d1", attr="type", fk_role="invoice"),
    ]
    cells = [_ref_cell("f1", "d1", ["billto"], g), _ref_cell("f2", "d1", ["invoice"], g)]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups[("d1", "type", g)]) == {"f1", "f2"}
    assert len([k for k in groups if k[0] == "d1"]) == 1  # exactly one referenced identity


def test_bill_to_ship_to_unjudged_are_two_identities() -> None:
    # (a) Two FKs from one fact to one dimension, unjudged → two identities.
    defs = [
        _slice("f1", "billto", dim="d1", attr="type", fk_role="billto"),
        _slice("f1", "shipto", dim="d1", attr="type", fk_role="shipto"),
    ]
    cells = [
        _ref_cell("f1", "d1", ["billto"], "ref:d1:billto"),
        _ref_cell("f1", "d1", ["shipto"], "ref:d1:shipto"),
    ]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert ("d1", "type", "ref:d1:billto") in groups
    assert ("d1", "type", "ref:d1:shipto") in groups
    assert len([k for k in groups if k[0] == "d1"]) == 2


def test_bill_to_ship_to_conformed_are_one_identity() -> None:
    # (b) The judge conformed the two roles → one identity carrying the merged group.
    g = "ref:d1:billto|shipto"
    defs = [
        _slice("f1", "billto", dim="d1", attr="type", fk_role="billto"),
        _slice("f1", "shipto", dim="d1", attr="type", fk_role="shipto"),
    ]
    cells = [_ref_cell("f1", "d1", ["billto", "shipto"], g)]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups[("d1", "type", g)]) == {"f1"}
    assert len([k for k in groups if k[0] == "d1"]) == 1


def test_bill_to_ship_to_role_verdict_stays_two_identities() -> None:
    # (c) The judge said ROLE → separate axes, each keyed by its own role.
    defs = [
        _slice("f1", "billto", dim="d1", attr="type", fk_role="billto"),
        _slice("f1", "shipto", dim="d1", attr="type", fk_role="shipto"),
    ]
    cells = [
        _ref_cell("f1", "d1", ["billto"], "ref:d1:billto"),
        _ref_cell("f1", "d1", ["shipto"], "ref:d1:shipto"),
    ]
    groups, _ = _shared_dimension_groups(defs, cells)
    referenced = sorted(k[2] for k in groups if k[0] == "d1")
    assert referenced == ["ref:d1:billto", "ref:d1:shipto"]


def test_missing_referenced_cell_falls_back_to_structural_role() -> None:
    # A referenced slice with no bus-matrix cell (should not happen in production)
    # still gets a deterministic, content-derived structural identity — never a
    # crash, never a merge with a different role.
    defs = [
        _slice("f1", "billto", dim="d1", attr=None, fk_role="billto"),
        _slice("f2", "billto", dim="d1", attr=None, fk_role="billto"),
    ]
    groups, _ = _shared_dimension_groups(defs, [])
    assert set(groups[("d1", "", "ref:d1:billto")]) == {"f1", "f2"}


def test_referenced_singleton_fact_is_kept_for_the_filter() -> None:
    # Only one fact at an identity — a singleton the caller's >=2 filter drops.
    defs = [_slice("f2", "region", dim="d2", attr=None, fk_role="region")]
    groups, _ = _shared_dimension_groups(defs, [])
    assert set(groups[("d2", "", "ref:d2:region")]) == {"f2"}


# --- Folded identity (DAT-800) -----------------------------------------------


def test_conformed_folded_cells_form_an_identity() -> None:
    defs = [
        _slice("gl", "account_id"),  # folded slices: no dimension_table_id
        _slice("tb", "account_id"),
    ]
    g = "conform:gl:account_id|tb:account_id"
    cells = [
        _folded_cell("gl", "account_id", "account", g),
        _folded_cell("tb", "account_id", "account", g),
    ]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    identity = (f"folded:{g}", "", "")
    assert set(groups[identity]) == {"gl", "tb"}
    assert folded_labels[identity] == "account"
    # the lens objects ARE the facts' own folded slices
    assert groups[identity]["gl"][0].column_name == "account_id"


def test_unsliced_fold_key_abstains_for_that_fact() -> None:
    defs = [_slice("gl", "account_id")]  # tb's fold key was never sliced
    g = "conform:gl:account_id|tb:account_id"
    cells = [
        _folded_cell("gl", "account_id", "account", g),
        _folded_cell("tb", "account_id", "account", g),
    ]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g}", "", "")]) == {"gl"}  # singleton — caller filters


def test_referenced_slice_never_serves_a_folded_identity() -> None:
    # A same-named REFERENCED slice must not be borrowed as a folded lens.
    defs = [_slice("gl", "account_id", dim="d1", fk_role="account_id"), _slice("tb", "account_id")]
    g = "conform:gl:account_id|tb:account_id"
    cells = [
        _folded_cell("gl", "account_id", "account", g),
        _folded_cell("tb", "account_id", "account", g),
    ]
    groups, _ = _shared_dimension_groups(defs, cells)
    assert set(groups.get((f"folded:{g}", "", ""), {})) == {"tb"}
    assert set(groups[("d1", "", "ref:d1:account_id")]) == {"gl"}


def test_label_collision_across_groups_never_merges() -> None:
    # The judge said DISTINCT (two separate components) but both got the same
    # generic label — grouping by label would silently merge them.
    defs = [_slice("gl", "status"), _slice("bt", "state")]
    g1 = "conform:gl:status|iv:status"
    g2 = "conform:bt:state|pm:state"
    cells = [
        _folded_cell("gl", "status", "status", g1),
        _folded_cell("bt", "state", "status", g2),
    ]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g1}", "", "")]) == {"gl"}
    assert set(groups[(f"folded:{g2}", "", "")]) == {"bt"}
    assert len(folded_labels) == 2  # two identities, both displaying "status"
    assert set(folded_labels.values()) == {"status"}


def test_one_group_is_one_axis_regardless_of_labels() -> None:
    # Cells of ONE conform component always form one axis; the conform pass
    # canonicalizes the label, and the grouping never re-splits on it.
    defs = [_slice("gl", "account_id"), _slice("tb", "acct_no")]
    g = "conform:gl:account_id|tb:acct_no"
    cells = [
        _folded_cell("gl", "account_id", "account", g),
        _folded_cell("tb", "acct_no", "account", g),
    ]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert set(groups[(f"folded:{g}", "", "")]) == {"gl", "tb"}
    assert folded_labels[(f"folded:{g}", "", "")] == "account"


def test_unconformed_cell_contributes_nothing() -> None:
    # conformed_group is None → no cross-fact identity was asserted.
    defs = [_slice("gl", "account_id")]
    cells = [_folded_cell("gl", "account_id", "account_id", None)]
    groups, folded_labels = _shared_dimension_groups(defs, cells)
    assert groups == {}
    assert folded_labels == {}
