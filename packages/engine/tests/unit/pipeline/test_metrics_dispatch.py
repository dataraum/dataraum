"""Unit tests for metrics_phase dispatch helpers (DAT-456).

Focused on the three module-level helpers — `_execute_metrics_serial`,
`_execute_metrics_parallel`, `_execute_isolated` — and the concurrency
contract (semaphore cap, per-call resource isolation).

The full phase `_run()` path (declare → compose → execute) is exercised by
`test_metrics_phase.py` and the integration suite; here we lock the dispatch
contract in isolation. The parallel/isolated helpers' positional id argument is
the snippet base's `schema_mapping_id` (workspace-stable, source-free).
"""

# Tests pass MagicMock / stub objects where the helpers expect concrete
# GraphAgent / ConnectionManager / TransformationGraph instances. That's
# the whole point of mocking — silence the arg-type complaints at module
# level rather than per-call.
# mypy: disable-error-code="arg-type, list-item, comparison-overlap"

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from dataraum.core.models.base import Result
from dataraum.pipeline.phases import metrics_phase as gep

_WORKSPACE_ID = "test"
_VERTICAL = "finance"


@dataclass
class _StubGraph:
    """Minimal graph stand-in. Only metadata.inspiration_snippet_id is read."""

    graph_id: str
    metadata: Any = None


class _StubMetadata:
    def __init__(self, inspiration_snippet_id: str | None = None) -> None:
        self.inspiration_snippet_id = inspiration_snippet_id


def _graph(gid: str, inspiration: str | None = None) -> _StubGraph:
    return _StubGraph(graph_id=gid, metadata=_StubMetadata(inspiration))


# ---------------------------------------------------------------------------
# Serial fallback
# ---------------------------------------------------------------------------


class TestExecuteMetricsSerial:
    def test_dispatches_each_graph_in_order(self) -> None:
        agent = MagicMock()
        agent.execute.side_effect = [Result.ok(f"r{i}") for i in range(3)]
        session = MagicMock()
        exec_ctx = MagicMock()
        prep = [
            ("g0", _graph("g0"), None, None),
            ("g1", _graph("g1"), "SELECT 1", "insp-1"),
            ("g2", _graph("g2"), None, None),
        ]

        out = gep._execute_metrics_serial(
            prep, session, exec_ctx, agent, workspace_id=_WORKSPACE_ID
        )

        assert [graph_id for graph_id, _, _ in out] == ["g0", "g1", "g2"]
        assert [r.value for _, r, _ in out] == ["r0", "r1", "r2"]
        assert [iid for _, _, iid in out] == [None, "insp-1", None]
        assert agent.execute.call_count == 3
        # All calls share the same session + exec_ctx (the fallback contract)
        for call in agent.execute.call_args_list:
            assert call.args[0] is session
            assert call.args[2] is exec_ctx

    def test_propagates_failure_per_graph(self) -> None:
        agent = MagicMock()
        agent.execute.side_effect = [
            Result.ok("ok"),
            Result.fail("nope"),
        ]
        out = gep._execute_metrics_serial(
            [("g0", _graph("g0"), None, None), ("g1", _graph("g1"), None, None)],
            MagicMock(),
            MagicMock(),
            agent,
            workspace_id=_WORKSPACE_ID,
        )
        assert out[0][1].success is True
        assert out[1][1].success is False
        assert out[1][1].error == "nope"


# ---------------------------------------------------------------------------
# Parallel dispatch
# ---------------------------------------------------------------------------


class _ConcurrencyTrackingAgent:
    """Mock GraphAgent that records the peak in-flight count and per-call sleep."""

    def __init__(self, sleep_seconds: float = 0.05) -> None:
        self._sleep = sleep_seconds
        self._lock = threading.Lock()
        self.in_flight = 0
        self.peak_in_flight = 0
        self.calls: list[str] = []

    def execute(
        self,
        session: Any,
        graph: _StubGraph,
        context: Any,
        inspiration_sql: str | None = None,
        *,
        workspace_id: str = "",
    ) -> Result[str]:
        with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        # Hold the slot for sleep_seconds so concurrent calls overlap
        time.sleep(self._sleep)
        with self._lock:
            self.in_flight -= 1
        self.calls.append(graph.graph_id)
        return Result.ok(graph.graph_id)


def _stub_manager() -> MagicMock:
    """ConnectionManager mock with session_scope + duckdb_cursor context managers."""
    manager = MagicMock()
    session = MagicMock()
    cursor = MagicMock()

    @contextmanager
    def session_scope() -> Any:
        yield session

    @contextmanager
    def duckdb_cursor() -> Any:
        yield cursor

    manager.session_scope = session_scope
    manager.duckdb_cursor = duckdb_cursor
    return manager


class TestExecuteMetricsParallel:
    def test_dispatches_all_graphs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Stub ExecutionContext.with_rich_context so we don't touch DB
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        agent = _ConcurrencyTrackingAgent(sleep_seconds=0.01)
        manager = _stub_manager()
        prep = [(f"g{i}", _graph(f"g{i}"), None, None) for i in range(7)]

        out = gep._execute_metrics_parallel(
            prep,
            manager,
            agent,
            "src-1",
            ["t1"],
            _VERTICAL,
            om_run_id="run-test",
        )

        assert sorted(gid for gid, _, _ in out) == [f"g{i}" for i in range(7)]
        assert all(r.success for _, r, _ in out)
        assert sorted(agent.calls) == sorted(g.graph_id for _, g, _, _ in prep)

    def test_concurrency_capped_at_max(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        # Drop cap to a small number so the test runs fast and the assertion is sharp
        monkeypatch.setattr(gep, "_MAX_CONCURRENT_METRICS", 2)
        # Sleep long enough that contention is observable
        agent = _ConcurrencyTrackingAgent(sleep_seconds=0.05)
        manager = _stub_manager()
        prep = [(f"g{i}", _graph(f"g{i}"), None, None) for i in range(8)]

        gep._execute_metrics_parallel(
            prep,
            manager,
            agent,
            "src",
            ["t"],
            _VERTICAL,
            om_run_id="run-test",
        )

        # With cap=2 and 8 metrics, peak must not exceed 2
        assert agent.peak_in_flight <= 2, f"Peak in-flight {agent.peak_in_flight} exceeded cap of 2"
        # But should actually reach the cap (otherwise the test isn't proving anything)
        assert agent.peak_in_flight == 2

    def test_preserves_inspiration_id_passthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        agent = _ConcurrencyTrackingAgent(sleep_seconds=0.0)
        manager = _stub_manager()
        prep = [
            ("g0", _graph("g0"), None, None),
            ("g1", _graph("g1"), "SELECT 1", "insp-1"),
            ("g2", _graph("g2"), None, "insp-2"),
        ]

        out = gep._execute_metrics_parallel(
            prep,
            manager,
            agent,
            "src",
            ["t"],
            _VERTICAL,
            om_run_id="run-test",
        )

        by_id = {gid: (r, iid) for gid, r, iid in out}
        assert by_id["g0"][1] is None
        assert by_id["g1"][1] == "insp-1"
        assert by_id["g2"][1] == "insp-2"

    def test_empty_prep_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        out = gep._execute_metrics_parallel(
            [],
            _stub_manager(),
            MagicMock(),
            "src",
            [],
            _VERTICAL,
            om_run_id="run-test",
        )
        assert out == []

    def test_exception_in_one_worker_does_not_abort_siblings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unexpected exception in one _execute_isolated must be captured as
        Result.fail for that graph, with the other workers still completing.

        Without the try/except guard around `to_thread`, asyncio.gather would
        propagate the first exception and discard sibling results.
        """
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )

        # Agent that raises on graph_id="bad", succeeds otherwise
        class _FlakyAgent:
            def execute(
                self,
                session: Any,
                graph: _StubGraph,
                context: Any,
                inspiration_sql: str | None = None,
                *,
                workspace_id: str = "",
            ) -> Result[str]:
                if graph.graph_id == "bad":
                    raise RuntimeError("simulated infra failure")
                return Result.ok(graph.graph_id)

        manager = _stub_manager()
        prep = [
            ("g0", _graph("g0"), None, None),
            ("bad", _graph("bad"), None, "insp-bad"),
            ("g2", _graph("g2"), None, None),
        ]

        out = gep._execute_metrics_parallel(
            prep,
            manager,
            _FlakyAgent(),
            "src",
            ["t"],
            _VERTICAL,
            om_run_id="run-test",
        )

        by_id = {gid: (r, iid) for gid, r, iid in out}
        # All three results are present
        assert set(by_id.keys()) == {"g0", "bad", "g2"}
        # Siblings succeeded
        assert by_id["g0"][0].success is True
        assert by_id["g2"][0].success is True
        # Failed worker returned a structured Result.fail (not a raised exception)
        assert by_id["bad"][0].success is False
        assert "simulated infra failure" in (by_id["bad"][0].error or "")
        # inspiration_id passthrough preserved even on failure
        assert by_id["bad"][1] == "insp-bad"


# ---------------------------------------------------------------------------
# Isolated dispatch (per-call session + cursor)
# ---------------------------------------------------------------------------


class TestExecuteIsolated:
    def test_opens_fresh_session_and_cursor_per_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock(_session=kw.get("session"))),
        )

        manager = MagicMock()
        opened_sessions: list[MagicMock] = []
        opened_cursors: list[MagicMock] = []

        @contextmanager
        def session_scope() -> Any:
            s = MagicMock(name=f"session-{len(opened_sessions)}")
            opened_sessions.append(s)
            yield s

        @contextmanager
        def duckdb_cursor() -> Any:
            c = MagicMock(name=f"cursor-{len(opened_cursors)}")
            opened_cursors.append(c)
            yield c

        manager.session_scope = session_scope
        manager.duckdb_cursor = duckdb_cursor

        agent = MagicMock()
        agent.execute.return_value = Result.ok("done")

        # Each call should open a fresh pair
        gep._execute_isolated(
            _graph("g0"), None, manager, agent, "src", ["t"], _VERTICAL, "run-test"
        )
        gep._execute_isolated(
            _graph("g1"), None, manager, agent, "src", ["t"], _VERTICAL, "run-test"
        )

        assert len(opened_sessions) == 2
        assert len(opened_cursors) == 2
        assert opened_sessions[0] is not opened_sessions[1]
        assert opened_cursors[0] is not opened_cursors[1]


# ---------------------------------------------------------------------------
# Async correctness sanity
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Node warming pre-pass (DAT-629)
# ---------------------------------------------------------------------------


from dataraum.graphs.models import (  # noqa: E402
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)


def _real_extract(step_id: str, standard_field: str) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=standard_field, statement="income_statement"),
        aggregation="sum",
    )


def _real_formula(step_id: str, expression: str, depends_on: list[str]) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.FORMULA,
        expression=expression,
        depends_on=depends_on,
        output_step=True,
    )


def _real_graph(graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


class _RecordingWarmAgent:
    """Records the output-step identity of each warmed mini-graph, in call order."""

    def __init__(self) -> None:
        self.warmed: list[str] = []

    def execute(
        self,
        session: Any,
        graph: TransformationGraph,
        context: Any,
        inspiration_sql: str | None = None,
        *,
        workspace_id: str = "",
    ) -> Result[str]:
        out = graph.get_output_step()
        assert out is not None  # every mini-graph has exactly one output
        if out.step_type == StepType.EXTRACT and out.source:
            ident = f"extract:{out.source.standard_field}"
        else:
            ident = f"formula:{out.expression}"
        self.warmed.append(ident)
        return Result.ok(ident)


class _StubCtx:
    """Minimal PhaseContext stand-in for the warm pre-pass (serial path)."""

    def __init__(self) -> None:
        self.manager = None  # forces serial warming
        self.session = MagicMock()
        self.duckdb_conn = MagicMock()


class TestWarmSharedNodes:
    def test_shared_extract_warmed_once_extracts_before_formulas(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        gross = _real_graph(
            "gross_margin",
            {
                "rev": _real_extract("rev", "revenue"),
                "cogs": _real_extract("cogs", "cost_of_goods_sold"),
                "gp": _real_formula("gp", "revenue - cogs", ["rev", "cogs"]),
            },
        )
        net = _real_graph(
            "net_income",
            {
                "rev2": _real_extract("rev2", "revenue"),
                "cogs2": _real_extract("cogs2", "cost_of_goods_sold"),
                "opex": _real_extract("opex", "operating_expense"),
                "ni": _real_formula("ni", "revenue - cogs - opex", ["rev2", "cogs2", "opex"]),
            },
        )
        agent = _RecordingWarmAgent()

        gep._warm_shared_nodes(
            {"gross_margin": gross, "net_income": net},
            _StubCtx(),  # type: ignore[arg-type]
            agent,  # type: ignore[arg-type]
            _WORKSPACE_ID,
            ["t1"],
            _VERTICAL,
            om_run_id="run-test",
        )

        # cost_of_goods_sold + revenue each warmed exactly once (deduped).
        assert agent.warmed.count("extract:cost_of_goods_sold") == 1
        assert agent.warmed.count("extract:revenue") == 1
        assert agent.warmed.count("extract:operating_expense") == 1
        # The two distinct formula expressions warmed once each.
        formulas = [w for w in agent.warmed if w.startswith("formula:")]
        assert len(formulas) == 2
        # Every extract is warmed before any formula (dependency order).
        first_formula = next(i for i, w in enumerate(agent.warmed) if w.startswith("formula:"))
        assert all(w.startswith("extract:") for w in agent.warmed[:first_formula])

    def test_cyclic_metric_set_is_best_effort_no_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dataraum.graphs.agent.ExecutionContext.with_rich_context",
            classmethod(lambda cls, **kw: MagicMock()),
        )
        a = _real_formula("a", "x_one - y_two", ["b"])
        b = _real_formula("b", "y_two - x_one", ["a"])
        cyclic = _real_graph("cyclic", {"a": a, "b": b})
        agent = _RecordingWarmAgent()

        # A cyclic set must not raise — warming is skipped, execute surfaces it.
        gep._warm_shared_nodes(
            {"cyclic": cyclic},
            _StubCtx(),  # type: ignore[arg-type]
            agent,  # type: ignore[arg-type]
            _WORKSPACE_ID,
            ["t1"],
            _VERTICAL,
            om_run_id="run-test",
        )
        assert agent.warmed == []


def test_parallel_dispatch_runs_on_a_threadpool_no_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The parallel path uses a ThreadPoolExecutor, not an asyncio event loop.

    The metrics activity is a SYNC Temporal activity on a thread engine; the
    fan-out is a plain ``ThreadPoolExecutor`` (the codebase's standard primitive)
    — no nested ``asyncio.run`` in a worker thread. Sanity-check it dispatches
    cleanly from a sync caller with no running loop in scope.
    """
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    monkeypatch.setattr(
        "dataraum.graphs.agent.ExecutionContext.with_rich_context",
        classmethod(lambda cls, **kw: MagicMock()),
    )
    agent = _ConcurrencyTrackingAgent(sleep_seconds=0.0)
    manager = _stub_manager()
    prep = [(f"g{i}", _graph(f"g{i}"), None, None) for i in range(3)]

    out = gep._execute_metrics_parallel(
        prep, manager, agent, "src", ["t"], _VERTICAL, om_run_id="run-test"
    )
    assert len(out) == 3
