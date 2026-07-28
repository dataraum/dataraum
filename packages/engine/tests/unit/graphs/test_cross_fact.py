"""The conformed drill-across gate (DAT-809).

Pins the pure part: which conformed groups may become a JOIN, and how each fact
spells the axis. The bar is deliberately higher than lineage's grouping — a
structurally-conformed-but-unconfirmed pairing is a typed refusal, never a merge.

FIXTURE PROVENANCE: the finance corpus is FK-normalized and carries no dim tables,
so these cells are the evidence base. Their shape is derived from the ONE writer,
``analysis/hierarchies/bus_matrix.py::derive_bus_matrix`` — referenced cells always
carry ``conformed_group`` (``_ref_group_signature`` -> ``ref:{dim}:{roles}``) and a
non-null ``dimension_table_id``; folded cells carry ``dimension_table_id=None``,
exactly one entry in ``roles``, and a ``conformed_group`` ONLY when the conform judge
returned a verdict (``conform:{fact}:{key}|...``). Not an idealized shape.
"""

from __future__ import annotations

import pytest

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.graphs.cross_fact import (
    CrossFactAbstain,
    CrossFactAxis,
    CrossFactDecision,
    CrossFactStatus,
    confirmed_axis_columns,
)
from dataraum.storage.base import load_all_models

# Instantiating a mapped class configures the mapper registry, which needs every
# model imported — a no-op when a sibling already did it.
load_all_models()

RUN = "catalog-run-1"
GL = "t_gl"
AP = "t_ap"
DIM = "t_accounts"


def _ref_cell(
    fact: str,
    roles: list[str],
    group: str,
    *,
    source: str = "judge",
    needs_confirmation: bool = False,
    dim: str = DIM,
) -> BusMatrixEntry:
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="referenced",
        concept_label="accounts",
        dimension_table_id=dim,
        roles=roles,
        attributes=[],
        confirmation_source=source,
        conformed_group=group,
        needs_confirmation=needs_confirmation,
        signature=f"bus:referenced:{fact}:{dim}:" + "|".join(roles),
    )


def _folded_cell(
    fact: str,
    key: str,
    group: str | None,
    *,
    source: str = "judge",
    needs_confirmation: bool = False,
) -> BusMatrixEntry:
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="folded",
        concept_label="region",
        dimension_table_id=None,
        roles=[key],
        attributes=[],
        confirmation_source=source,
        conformed_group=group,
        needs_confirmation=needs_confirmation,
        signature=f"bus:folded:{fact}:{key}",
    )


def _key_slice(table: str, column: str, *, role: str, dim: str = DIM) -> SliceDefinition:
    """The FK KEY slice — `dimension_attribute` is NULL (it is the key, not an attribute)."""
    return SliceDefinition(
        run_id=RUN,
        table_id=table,
        column_id=f"{table}:{column}",
        column_name=column,
        dimension_table_id=dim,
        dimension_attribute=None,
        fk_role=role,
        slice_type="categorical",
        detection_source="llm",
    )


def _folded_slice(table: str, column: str) -> SliceDefinition:
    return SliceDefinition(
        run_id=RUN,
        table_id=table,
        column_id=f"{table}:{column}",
        column_name=column,
        dimension_table_id=None,
        dimension_attribute=None,
        fk_role=None,
        slice_type="categorical",
        detection_source="llm",
    )


class TestConfirmedAxisColumns:
    def test_differently_spelled_fk_roles_resolve_to_each_fact_own_column(self) -> None:
        """The crossing a NAME intersection cannot see.

        Both facts reference the same dim in one conformed role; the judge merged
        the differently-named roles, so `conformed_group` is shared while the local
        columns are not.
        """
        group = f"ref:{DIM}:account_id"
        cells = [
            _ref_cell(GL, ["account_id"], group),
            _ref_cell(AP, ["acct"], group),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {
            group: ("accounts", {GL: "account_id", AP: "acct"})
        }

    def test_an_unconfirmed_cell_is_not_a_join(self) -> None:
        """Same-named FK roles conform STRUCTURALLY — nobody confirmed the relationship.

        This is the silent-join risk the whole gate exists for: the group key is
        present and identical on both sides, so a conformed_group-only filter would
        happily merge two facts' numbers on an unconfirmed FK.
        """
        group = f"ref:{DIM}:account_id"
        cells = [
            _ref_cell(GL, ["account_id"], group, source="unconfirmed"),
            _ref_cell(AP, ["account_id"], group, source="unconfirmed"),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "account_id", role="account_id"),
        ]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    def test_a_cell_awaiting_review_is_not_a_join(self) -> None:
        group = f"ref:{DIM}:account_id"
        cells = [
            _ref_cell(GL, ["account_id"], group),
            _ref_cell(AP, ["acct"], group, needs_confirmation=True),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    @pytest.mark.parametrize("source", ["judge", "keeper", "user"])
    def test_every_confirmed_source_backs_a_join(self, source: str) -> None:
        group = f"ref:{DIM}:account_id"
        cells = [
            _ref_cell(GL, ["account_id"], group, source=source),
            _ref_cell(AP, ["acct"], group, source=source),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        assert group in confirmed_axis_columns(cells, slices, {GL, AP})

    def test_a_group_covering_only_one_fact_is_dropped(self) -> None:
        """A merge covering some carriers answers a narrower question than was asked."""
        group = f"ref:{DIM}:account_id"
        cells = [_ref_cell(GL, ["account_id"], group)]
        slices = [_key_slice(GL, "account_id", role="account_id")]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    def test_a_folded_cell_with_no_conformed_group_is_dropped(self) -> None:
        """The production default: a fold conforms only on a judge `conform` verdict."""
        cells = [_folded_cell(GL, "region", None), _folded_cell(AP, "region", None)]
        slices = [_folded_slice(GL, "region"), _folded_slice(AP, "region")]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    def test_a_conformed_fold_resolves_to_its_fold_key(self) -> None:
        group = f"conform:{AP}:region|{GL}:region_name"
        cells = [_folded_cell(GL, "region_name", group), _folded_cell(AP, "region", group)]
        slices = [_folded_slice(GL, "region_name"), _folded_slice(AP, "region")]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {
            group: ("region", {GL: "region_name", AP: "region"})
        }

    def test_a_conformed_axis_never_sliced_abstains_rather_than_guessing(self) -> None:
        group = f"ref:{DIM}:account_id"
        cells = [_ref_cell(GL, ["account_id"], group), _ref_cell(AP, ["acct"], group)]
        slices = [_key_slice(GL, "account_id", role="account_id")]  # AP's key uncurated
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    def test_a_dim_side_attribute_is_not_mistaken_for_the_key(self) -> None:
        """The breakdown groups by what the FACT carries — an id, not the dim row's name."""
        group = f"ref:{DIM}:account_id"
        cells = [_ref_cell(GL, ["account_id"], group), _ref_cell(AP, ["acct"], group)]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            SliceDefinition(
                run_id=RUN,
                table_id=AP,
                column_id=f"{AP}:acct__name",
                column_name="acct__name",
                dimension_table_id=DIM,
                dimension_attribute="name",
                fk_role="acct",
                slice_type="categorical",
                detection_source="llm",
            ),
        ]
        assert confirmed_axis_columns(cells, slices, {GL, AP}) == {}

    def test_label_drift_does_not_split_a_group(self) -> None:
        """`concept_label` is display-only — the identity is the group signature."""
        group = f"ref:{DIM}:account_id"
        a = _ref_cell(GL, ["account_id"], group)
        b = _ref_cell(AP, ["acct"], group)
        b.concept_label = "ledger accounts"
        resolved = confirmed_axis_columns(
            [a, b],
            [
                _key_slice(GL, "account_id", role="account_id"),
                _key_slice(AP, "acct", role="acct"),
            ],
            {GL, AP},
        )
        assert list(resolved) == [group]
        assert resolved[group][1] == {GL: "account_id", AP: "acct"}


class TestCrossFactAxis:
    def test_step_grain_projects_each_carrier_under_the_shared_identity(self) -> None:
        axis = CrossFactAxis(
            identity=f"ref:{DIM}:account_id",
            label="accounts",
            columns={GL: "account_id", AP: "acct"},
            steps={"revenue": GL, "payables": AP},
        )
        assert axis.step_grain() == {
            "revenue": (("account_id", f"ref:{DIM}:account_id"),),
            "payables": (("acct", f"ref:{DIM}:account_id"),),
        }
        assert axis.facts == (AP, GL)


class TestCrossFactDecision:
    def test_an_abstention_must_name_a_typed_reason_and_a_message(self) -> None:
        with pytest.raises(ValueError, match="requires a typed reason"):
            CrossFactDecision(status=CrossFactStatus.ABSTAINED, reason="no")
        with pytest.raises(ValueError, match="must name why"):
            CrossFactDecision(
                status=CrossFactStatus.ABSTAINED,
                abstain_reason=CrossFactAbstain.SINGLE_FACT,
            )

    def test_an_abstention_claims_nothing_about_an_axis(self) -> None:
        axis = CrossFactAxis(identity="g", label="l", columns={}, steps={})
        with pytest.raises(ValueError, match="must not carry an axis"):
            CrossFactDecision(
                status=CrossFactStatus.ABSTAINED,
                axis=axis,
                reason="x",
                abstain_reason=CrossFactAbstain.SINGLE_FACT,
            )

    def test_a_resolved_decision_requires_an_axis(self) -> None:
        with pytest.raises(ValueError, match="requires an axis"):
            CrossFactDecision(status=CrossFactStatus.RESOLVED)
