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
    survey_conformed_axes,
)
from dataraum.storage.base import load_all_models

# Instantiating a mapped class configures the mapper registry, which needs every
# model imported — a no-op when a sibling already did it.
load_all_models()

RUN = "catalog-run-1"
GL = "t_gl"
AP = "t_ap"
DIM = "t_accounts"

# `_ref_group_signature(dim, roles)` renders `ref:{dim}:` + "|".join(sorted(roles)),
# so a judge CONFORM joining differently-named roles yields the UNION of both role
# names — not either one alone. Same-named roles conform structurally to the single
# form. Getting this wrong is the fixture defect class the harness rule warns about.
CONFORMED = f"ref:{DIM}:acct|account_id"
STRUCTURAL = f"ref:{DIM}:account_id"


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
    source: str | None = None,
    attributes: list[str] | None = None,
    needs_confirmation: bool = False,
) -> BusMatrixEntry:
    """A folded cell in the shape `_folded_cells` writes.

    `source` defaults to what the writer would actually emit: it sets 'judge' only
    when the component conformed, which is exactly when `conformed_group` is set —
    so judge-with-no-group is an UNWRITABLE combination and must not be seeded.
    The signature covers ALL member columns (fold key and attributes), not the key
    alone, because that is half the (signature, run_id) upsert key.
    """
    attrs = attributes or []
    members = sorted([key, *attrs])
    return BusMatrixEntry(
        run_id=RUN,
        fact_table_id=fact,
        attachment="folded",
        concept_label="region",
        dimension_table_id=None,
        roles=[key],
        attributes=attrs,
        confirmation_source=source or ("judge" if group else "unconfirmed"),
        conformed_group=group,
        needs_confirmation=needs_confirmation,
        signature=f"bus:folded:{fact}:" + "|".join(members),
    )


def _key_slice(
    table: str,
    column: str,
    *,
    role: str,
    dim: str = DIM,
    interest: str | None = "primary",
    relevance: float | None = 0.8,
) -> SliceDefinition:
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
        slice_interest=interest,
        slice_relevance=relevance,
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


class TestSurveyConformedAxes:
    def test_differently_spelled_fk_roles_resolve_to_each_fact_own_column(self) -> None:
        """The crossing a NAME intersection cannot see.

        Both facts reference the same dim in one conformed role; the judge merged
        the differently-named roles, so `conformed_group` is shared while the local
        columns are not.
        """
        cells = [
            _ref_cell(GL, ["account_id"], CONFORMED),
            _ref_cell(AP, ["acct"], CONFORMED),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        survey = survey_conformed_axes(cells, slices, {GL, AP})
        assert list(survey.qualified) == [CONFORMED]
        assert survey.qualified[CONFORMED].columns == {GL: "account_id", AP: "acct"}
        assert survey.qualified[CONFORMED].label == "accounts"
        assert survey.unsliced == {}

    def test_an_unconfirmed_cell_is_not_a_join(self) -> None:
        """Same-named FK roles conform STRUCTURALLY — nobody confirmed the relationship.

        This is the silent-join risk the whole gate exists for: the group key is
        present and identical on both sides, so a conformed_group-only filter would
        happily merge two facts' numbers on an unconfirmed FK.
        """
        cells = [
            _ref_cell(GL, ["account_id"], STRUCTURAL, source="unconfirmed"),
            _ref_cell(AP, ["account_id"], STRUCTURAL, source="unconfirmed"),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "account_id", role="account_id"),
        ]
        survey = survey_conformed_axes(cells, slices, {GL, AP})
        assert survey.qualified == {}
        # An unconfirmed cell is never even asked for a column, so it must NOT be
        # reported as an unsliced-axis problem — that would name the wrong fix.
        assert survey.unsliced == {}

    def test_a_cell_awaiting_review_is_not_a_join(self) -> None:
        cells = [
            _ref_cell(GL, ["account_id"], CONFORMED),
            _ref_cell(AP, ["acct"], CONFORMED, needs_confirmation=True),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        assert survey_conformed_axes(cells, slices, {GL, AP}).qualified == {}

    @pytest.mark.parametrize("source", ["judge", "keeper", "user"])
    def test_every_confirmed_source_backs_a_join(self, source: str) -> None:
        cells = [
            _ref_cell(GL, ["account_id"], CONFORMED, source=source),
            _ref_cell(AP, ["acct"], CONFORMED, source=source),
        ]
        slices = [
            _key_slice(GL, "account_id", role="account_id"),
            _key_slice(AP, "acct", role="acct"),
        ]
        assert CONFORMED in survey_conformed_axes(cells, slices, {GL, AP}).qualified

    def test_a_group_covering_only_one_fact_is_dropped(self) -> None:
        """A merge covering some carriers answers a narrower question than was asked."""
        cells = [_ref_cell(GL, ["account_id"], CONFORMED)]
        slices = [_key_slice(GL, "account_id", role="account_id")]
        assert survey_conformed_axes(cells, slices, {GL, AP}).qualified == {}

    def test_a_folded_cell_with_no_conformed_group_is_dropped(self) -> None:
        """The production default: a fold conforms only on a judge `conform` verdict."""
        cells = [_folded_cell(GL, "region", None), _folded_cell(AP, "region", None)]
        slices = [_folded_slice(GL, "region"), _folded_slice(AP, "region")]
        assert survey_conformed_axes(cells, slices, {GL, AP}).qualified == {}

    def test_a_conformed_fold_resolves_to_its_fold_key(self) -> None:
        group = f"conform:{AP}:region|{GL}:region_name"
        cells = [_folded_cell(GL, "region_name", group), _folded_cell(AP, "region", group)]
        slices = [_folded_slice(GL, "region_name"), _folded_slice(AP, "region")]
        survey = survey_conformed_axes(cells, slices, {GL, AP})
        assert survey.qualified[group].columns == {GL: "region_name", AP: "region"}

    def test_a_conformed_axis_never_sliced_is_reported_AS_unsliced(self) -> None:
        """The confirmed-but-uncurated case, which has its own fix.

        Reporting this as "no confirmed conformed dimension" is factually wrong —
        the pairing IS confirmed — and sends someone to confirm something already
        confirmed instead of curating the column.
        """
        cells = [_ref_cell(GL, ["account_id"], CONFORMED), _ref_cell(AP, ["acct"], CONFORMED)]
        slices = [_key_slice(GL, "account_id", role="account_id")]  # AP's key uncurated
        survey = survey_conformed_axes(cells, slices, {GL, AP})
        assert survey.qualified == {}
        assert survey.unsliced == {CONFORMED: (AP, "acct")}

    def test_a_dim_side_attribute_is_not_mistaken_for_the_key(self) -> None:
        """The breakdown groups by what the FACT carries — an id, not the dim row's name."""
        cells = [_ref_cell(GL, ["account_id"], CONFORMED), _ref_cell(AP, ["acct"], CONFORMED)]
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
        assert survey_conformed_axes(cells, slices, {GL, AP}).qualified == {}

    def test_label_drift_does_not_split_a_group(self) -> None:
        """`concept_label` is display-only — the identity is the group signature."""
        a = _ref_cell(GL, ["account_id"], CONFORMED)
        b = _ref_cell(AP, ["acct"], CONFORMED)
        b.concept_label = "ledger accounts"
        survey = survey_conformed_axes(
            [a, b],
            [
                _key_slice(GL, "account_id", role="account_id"),
                _key_slice(AP, "acct", role="acct"),
            ],
            {GL, AP},
        )
        assert list(survey.qualified) == [CONFORMED]
        assert survey.qualified[CONFORMED].columns == {GL: "account_id", AP: "acct"}

    def test_rank_follows_the_catalog_curation_not_the_identity(self) -> None:
        """The workspace decides which axis is interesting — not a uuid sort.

        The `primary` axis must outrank the `supporting` one even though its group
        identity sorts LATER, which is exactly what a bare min(identity) got wrong.
        """
        primary = f"ref:{DIM}:zzz_seg"
        supporting = f"ref:{DIM}:aaa_acct"
        cells = [
            _ref_cell(GL, ["zzz_seg"], primary),
            _ref_cell(AP, ["zzz_seg"], primary),
            _ref_cell(GL, ["aaa_acct"], supporting),
            _ref_cell(AP, ["aaa_acct"], supporting),
        ]
        slices = [
            _key_slice(GL, "zzz_seg", role="zzz_seg", interest="primary", relevance=0.9),
            _key_slice(AP, "zzz_seg", role="zzz_seg", interest="primary", relevance=0.9),
            _key_slice(GL, "aaa_acct", role="aaa_acct", interest="supporting", relevance=0.4),
            _key_slice(AP, "aaa_acct", role="aaa_acct", interest="supporting", relevance=0.4),
        ]
        survey = survey_conformed_axes(cells, slices, {GL, AP})
        assert survey.qualified[primary].rank < survey.qualified[supporting].rank


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
