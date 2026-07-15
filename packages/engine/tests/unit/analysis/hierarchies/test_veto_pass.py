"""The veto lane's integration pass (DAT-762 Phase C) — scripted judge.

Pins the seam contract: a veto SURFACES (needs_confirmation=True), never
deletes; uphold changes nothing; judge off/failed means the lane is skipped
and the rows are byte-identical; role-check and manual rows are never routed.
"""

from __future__ import annotations

import copy
from unittest.mock import MagicMock

import polars as pl

from dataraum.analysis.hierarchies.judge import VetoVerdict
from dataraum.analysis.hierarchies.processor import (
    _column_evidence,
    _judge_veto_pass,
    _route_row,
)


def _frame() -> pl.DataFrame:
    # entry_key: high-cardinality id; desc_entry: prose — the DAT-761 residue
    # shape (id<->text proxy bijection). region: a legit low-card level.
    return pl.DataFrame(
        {
            "entry_key": [f"JE-{i:06d}" for i in range(200)],
            "desc_entry": [f"a long posting description for entry number {i} etc" for i in range(200)],
            "region": ["North" if i % 2 else "South" for i in range(200)],
        }
    )


_D_SQL = {"entry_key": 200, "desc_entry": 200, "region": 2}


def _alias_row(*, sig: str = "alias:t1:desc_entry|entry_key",
               role_verdict: str | None = None,
               detection_source: str = "g3") -> dict[str, object]:
    return {
        "kind": "alias",
        "signature": sig,
        "detection_source": detection_source,
        "role_verdict": role_verdict,
        "needs_confirmation": False,
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
        unwrap=lambda: [VetoVerdict(
            structure_ref="alias:t1:desc_entry|entry_key",
            verdict=verdict,  # type: ignore[arg-type]
            reason="scripted",
        )],
    )
    return judge


def test_veto_surfaces_never_deletes() -> None:
    rows = [_alias_row()]
    _judge_veto_pass(rows, view_name="v", frame=_frame(), n_rows=200,
                     d_sql=_D_SQL, judge=_judge_returning("veto"))
    assert len(rows) == 1
    assert rows[0]["needs_confirmation"] is True


def test_uphold_changes_nothing() -> None:
    rows = [_alias_row()]
    before = copy.deepcopy(rows)
    _judge_veto_pass(rows, view_name="v", frame=_frame(), n_rows=200,
                     d_sql=_D_SQL, judge=_judge_returning("uphold"))
    assert rows == before


def test_lane_off_is_byte_identical() -> None:
    rows = [_alias_row()]
    before = copy.deepcopy(rows)
    _judge_veto_pass(rows, view_name="v", frame=_frame(), n_rows=200,
                     d_sql=_D_SQL, judge=None)
    assert rows == before


def test_judge_failure_skips_lane() -> None:
    from dataraum.analysis.hierarchies.judge import DimensionIdentityJudge

    judge = MagicMock(spec=DimensionIdentityJudge)
    judge.veto.return_value = MagicMock(success=False, error="api down")
    rows = [_alias_row()]
    before = copy.deepcopy(rows)
    _judge_veto_pass(rows, view_name="v", frame=_frame(), n_rows=200,
                     d_sql=_D_SQL, judge=judge)
    assert rows == before


def test_role_and_manual_rows_never_routed() -> None:
    ev = {c: _column_evidence(_frame(), c, n_rows=200, d_sql=_D_SQL)
          for c in _frame().columns}
    assert _route_row(_alias_row(role_verdict="value_systematic"), ev) is None
    assert _route_row(_alias_row(detection_source="manual"), ev) is None
    # ...while the same structure from the stack IS routed (the id<->prose residue).
    assert _route_row(_alias_row(), ev) == "proxy-bijection"


def test_column_evidence_is_deterministic() -> None:
    a = _column_evidence(_frame(), "entry_key", n_rows=200, d_sql=_D_SQL)
    b = _column_evidence(_frame(), "entry_key", n_rows=200, d_sql=_D_SQL)
    assert a == b
    assert a.n_distinct == 200
