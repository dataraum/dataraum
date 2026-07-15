"""The veto lane's integration pass (DAT-762 Phase C) — scripted judge.

Pins the seam contract: a veto SURFACES (needs_confirmation=True), never
deletes; uphold changes nothing; a failed judge call skips the lane
observably and the rows are byte-identical; role-check and manual rows are
never routed.
"""

from __future__ import annotations

import copy
from unittest.mock import MagicMock

import polars as pl

from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge, VetoVerdict
from dataraum.analysis.hierarchies.processor import (
    _judge_veto_pass,
    _route_row,
    column_evidence,
)


def _frame() -> pl.DataFrame:
    # entry_key: high-cardinality id; desc_entry: prose — the DAT-761 residue
    # shape (id<->text proxy bijection). region: a legit low-card level.
    return pl.DataFrame(
        {
            "entry_key": [f"JE-{i:06d}" for i in range(200)],
            "desc_entry": [
                f"a long posting description for entry number {i} etc" for i in range(200)
            ],
            "region": ["North" if i % 2 else "South" for i in range(200)],
        }
    )


_D_SQL = {"entry_key": 200, "desc_entry": 200, "region": 2}


def _alias_row(
    *,
    sig: str = "alias:t1:desc_entry|entry_key",
    role_verdict: str | None = None,
    detection_source: str = "g3",
    needs_confirmation: bool = False,
) -> dict[str, object]:
    return {
        "kind": "alias",
        "signature": sig,
        "detection_source": detection_source,
        "role_verdict": role_verdict,
        "needs_confirmation": needs_confirmation,
        "members": [
            {"column_name": "entry_key", "column_id": "c1", "distinct_count": 200, "level": 0},
            {"column_name": "desc_entry", "column_id": "c2", "distinct_count": 200, "level": 1},
        ],
    }


def _judge_returning(verdict: str) -> MagicMock:
    from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge

    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.veto.return_value = MagicMock(
        success=True,
        unwrap=lambda: [
            VetoVerdict(
                structure_ref="alias:t1:desc_entry|entry_key",
                verdict=verdict,  # type: ignore[arg-type]
                reason="scripted",
            )
        ],
    )
    return judge


def test_veto_surfaces_never_deletes() -> None:
    rows = [_alias_row()]
    stats = _judge_veto_pass(
        rows,
        view_name="v",
        frame=_frame(),
        n_rows=200,
        d_sql=_D_SQL,
        judge=_judge_returning("veto"),
    )
    assert len(rows) == 1
    assert rows[0]["needs_confirmation"] is True
    assert (stats.status, stats.routed, stats.vetoed, stats.views_judged) == ("ran", 1, 1, 1)


def test_uphold_changes_nothing() -> None:
    rows = [_alias_row()]
    before = copy.deepcopy(rows)
    stats = _judge_veto_pass(
        rows,
        view_name="v",
        frame=_frame(),
        n_rows=200,
        d_sql=_D_SQL,
        judge=_judge_returning("uphold"),
    )
    assert rows == before
    assert (stats.routed, stats.vetoed) == (1, 0)


def test_judge_failure_skips_lane_observably() -> None:
    from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge

    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.veto.return_value = MagicMock(success=False, error="api down")
    rows = [_alias_row()]
    before = copy.deepcopy(rows)
    stats = _judge_veto_pass(
        rows, view_name="v", frame=_frame(), n_rows=200, d_sql=_D_SQL, judge=judge
    )
    assert rows == before
    assert (stats.status, stats.views_failed) == ("failed", 1)


def test_permanent_error_skips_transient_propagates() -> None:
    import pytest

    from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge
    from dataraum.llm.providers.base import PermanentProviderError, TransientProviderError

    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.veto.side_effect = PermanentProviderError("bad key")
    rows = [_alias_row()]
    stats = _judge_veto_pass(
        rows, view_name="v", frame=_frame(), n_rows=200, d_sql=_D_SQL, judge=judge
    )
    assert stats.status == "failed" and rows[0]["needs_confirmation"] is False

    # Transient errors ride to the Temporal boundary: the phase retry re-runs
    # the seed-deterministic stats identically and re-asks the judge.
    judge.veto.side_effect = TransientProviderError("429")
    with pytest.raises(TransientProviderError):
        _judge_veto_pass(rows, view_name="v", frame=_frame(), n_rows=200, d_sql=_D_SQL, judge=judge)


def test_role_and_manual_rows_never_routed() -> None:
    ev = {c: column_evidence(_frame(), c, n_rows=200, d_sql=_D_SQL) for c in _frame().columns}
    assert _route_row(_alias_row(role_verdict="value_systematic"), ev) is None
    assert _route_row(_alias_row(detection_source="manual"), ev) is None
    # A row the stats already surfaced is not an ASSERTION — routing it would
    # spend a judgment on a no-op (the flag is already set).
    assert _route_row(_alias_row(needs_confirmation=True), ev) is None
    # ...while the same structure from the stack IS routed (the id<->prose residue).
    assert _route_row(_alias_row(), ev) == "proxy-bijection"


def test_routing_reads_level_not_array_position() -> None:
    """``level`` is the sole carrier of order (DAT-779) — a valid-but-shuffled
    members array must route identically, never swap determinant and dependent."""
    from dataraum.analysis.hierarchies import routing

    ev = {
        # idlike, NOT near-key (60 of 200 rows) — routable, unlike entry_key.
        "order_code": routing.ColumnEvidence(
            n_rows=200,
            n_distinct=60,
            dtype="String",
            sample_values=[f"ORD-{i:04d}" for i in range(60)],
        ),
        "status": routing.ColumnEvidence(
            n_rows=200, n_distinct=3, dtype="String", sample_values=["open", "closed", "held"]
        ),
    }
    shuffled = {
        "kind": "drilldown",
        "signature": "drill:t1:order_code|status",
        "detection_source": "g3",
        "role_verdict": None,
        "needs_confirmation": False,
        # Array order fine-then-coarse; levels say coarse(0) -> fine(1).
        "members": [
            {"column_name": "order_code", "column_id": "c1", "distinct_count": 60, "level": 1},
            {"column_name": "status", "column_id": "c3", "distinct_count": 3, "level": 0},
        ],
    }
    # Determinant = the FINER member (order_code, idlike) over a tiny enum:
    # quasi-identifier. Read by array position the determinant would be the
    # name-shaped status column -> None (the silent det/dep swap).
    assert _route_row(shuffled, ev) == "quasi-identifier"


def test_unanswered_structures_are_counted() -> None:
    """A routed structure with no verdict stands (absence of judgment is not a
    judgment) but is OBSERVABLE in the lane stats, never uphold-by-omission."""
    rows = [_alias_row(), _alias_row(sig="alias:t1:another|entry_key")]
    rows[1]["members"] = [
        {"column_name": "entry_key", "column_id": "c1", "distinct_count": 200, "level": 0},
        {"column_name": "desc_entry", "column_id": "c2", "distinct_count": 200, "level": 1},
    ]
    stats = _judge_veto_pass(
        rows,
        view_name="v",
        frame=_frame(),
        n_rows=200,
        d_sql=_D_SQL,
        judge=_judge_returning("veto"),  # answers only the default sig
    )
    assert stats.routed == 2
    assert stats.vetoed == 1
    assert stats.unanswered == 1
    assert rows[1]["needs_confirmation"] is False  # unanswered row unchanged


def test_duplicate_verdicts_never_double_count() -> None:
    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.veto.return_value = MagicMock(
        success=True,
        unwrap=lambda: [
            VetoVerdict(
                structure_ref="alias:t1:desc_entry|entry_key", verdict="veto", reason="dup"
            ),
            VetoVerdict(
                structure_ref="alias:t1:desc_entry|entry_key", verdict="veto", reason="dup"
            ),
        ],
    )
    rows = [_alias_row()]
    stats = _judge_veto_pass(
        rows, view_name="v", frame=_frame(), n_rows=200, d_sql=_D_SQL, judge=judge
    )
    assert stats.vetoed == 1
    assert stats.unanswered == 0


def test_column_evidence_is_deterministic() -> None:
    a = column_evidence(_frame(), "entry_key", n_rows=200, d_sql=_D_SQL)
    b = column_evidence(_frame(), "entry_key", n_rows=200, d_sql=_D_SQL)
    assert a == b
    assert a.n_distinct == 200


def _account_frame() -> pl.DataFrame:
    # The clean-flat false-veto shape: an entity's own drill chain
    # (type -> parent -> id) plus the id's 1:1 label alias (account_name).
    types = ["Asset", "Liability", "Revenue", "Expense"]
    return pl.DataFrame(
        {
            "account_id": [f"{1000 + i}" for i in range(60) for _ in range(5)],
            "account_name": [f"Account {1000 + i} Name" for i in range(60) for _ in range(5)],
            "parent_account_id": [f"{100 + i % 13}" for i in range(60) for _ in range(5)],
            "account_type": [types[i % 4] for i in range(60) for _ in range(5)],
        }
    )


_ACCT_D_SQL = {"account_id": 60, "account_name": 60, "parent_account_id": 13, "account_type": 4}


def _chain_row() -> dict[str, object]:
    return {
        "kind": "drilldown",
        "signature": "drilldown:t1:account_id|account_type|parent_account_id",
        "detection_source": "g3",
        "role_verdict": None,
        "needs_confirmation": False,
        "members": [
            {"column_name": "account_type", "column_id": "c1", "distinct_count": 4, "level": 0},
            {
                "column_name": "parent_account_id",
                "column_id": "c2",
                "distinct_count": 13,
                "level": 1,
            },
            {"column_name": "account_id", "column_id": "c3", "distinct_count": 60, "level": 2},
        ],
    }


def _label_alias_row() -> dict[str, object]:
    return {
        "kind": "alias",
        "signature": "alias:t1:account_id|account_name",
        "detection_source": "g3",
        "role_verdict": None,
        "needs_confirmation": False,
        "members": [
            {"column_name": "account_id", "column_id": "c3", "distinct_count": 60, "level": 0},
            {"column_name": "account_name", "column_id": "c4", "distinct_count": 60, "level": 1},
        ],
    }


def test_entity_anchored_chain_is_never_routed() -> None:
    """The clean-flat false-veto pin: the account chain skips the judge entirely."""
    rows = [_chain_row(), _label_alias_row()]
    judge = MagicMock(spec=DimensionIdentityJudge)
    stats = _judge_veto_pass(
        rows, view_name="v", frame=_account_frame(), n_rows=300, d_sql=_ACCT_D_SQL, judge=judge
    )
    assert stats.routed == 0
    judge.veto.assert_not_called()
    assert rows[0]["needs_confirmation"] is False


def test_same_chain_without_label_alias_still_routes() -> None:
    """No anchor, no protection: a bare id-over-enum chain reaches the judge."""
    rows = [_chain_row()]
    stats = _judge_veto_pass(
        rows,
        view_name="v",
        frame=_account_frame(),
        n_rows=300,
        d_sql=_ACCT_D_SQL,
        judge=_judge_returning("uphold"),
    )
    assert stats.routed == 1
