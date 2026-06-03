"""Unit tests for the ``ProgressSnapshot`` contract + ``get_progress`` (DAT-406).

These pin the FROZEN cross-package shape ``{phase, tables_total,
tables_completed}`` (mirrored TS-side in DAT-352) and the read-only query
handler's wiring — fast, no Temporal runtime. The live "snapshot advances
through the phases as children resolve" assertion runs against a real dev-server
in ``tests/integration/worker/test_progress_query.py``.
"""

from __future__ import annotations

import dataclasses

from temporalio.contrib.pydantic import pydantic_data_converter

from dataraum.worker.contracts import ProgressFailure, ProgressSnapshot, TableProgress
from dataraum.worker.workflows import AddSourceWorkflow


class TestProgressSnapshotContract:
    """The wire shape the cockpit mirrors — fields, defaults, and JSON form."""

    def test_fields_and_defaults(self) -> None:
        snap = ProgressSnapshot(phase="import")
        assert snap.phase == "import"
        assert snap.tables_total == 0
        assert snap.tables_completed == 0
        assert snap.tables == []
        assert snap.failure is None

    def test_is_a_plain_dataclass_not_pydantic(self) -> None:
        # A plain stdlib dataclass (NOT a Pydantic BaseModel): it lives in the
        # determinism sandbox and serializes via the worker's pydantic converter.
        assert dataclasses.is_dataclass(ProgressSnapshot)
        assert not hasattr(ProgressSnapshot, "model_fields")

    def test_serializes_to_json_shape_healthy(self) -> None:
        # The cockpit Client (TS) consumes the query result off the wire, so the
        # JSON keys are the contract. Assert the shape byte-for-byte — a healthy
        # mid-fan-out run carries the per-table steps and a null failure.
        snap = ProgressSnapshot(
            phase="processing_tables",
            tables_total=2,
            tables_completed=1,
            tables=[
                TableProgress(raw_table_id="r1", status="done"),
                TableProgress(raw_table_id="r2", status="running"),
            ],
        )
        payload = pydantic_data_converter.payload_converter.to_payload(snap)
        assert payload.data == (
            b'{"phase":"processing_tables","tables_total":2,"tables_completed":1,'
            b'"tables":[{"raw_table_id":"r1","status":"done"},'
            b'{"raw_table_id":"r2","status":"running"}],"failure":null}'
        )

    def test_serializes_failure_shape(self) -> None:
        # A failed run carries the reason inline so the cockpit needn't open the
        # Temporal UI; table_id pins a table-scoped failure.
        snap = ProgressSnapshot(
            phase="processing_tables",
            tables_total=1,
            tables_completed=0,
            tables=[TableProgress(raw_table_id="r1", status="failed")],
            failure=ProgressFailure(
                message="typing failed: bad cast", phase="processing_tables", table_id="r1"
            ),
        )
        payload = pydantic_data_converter.payload_converter.to_payload(snap)
        assert payload.data == (
            b'{"phase":"processing_tables","tables_total":1,"tables_completed":0,'
            b'"tables":[{"raw_table_id":"r1","status":"failed"}],'
            b'"failure":{"message":"typing failed: bad cast",'
            b'"phase":"processing_tables","table_id":"r1"}}'
        )

    def test_roundtrips_through_the_worker_converter(self) -> None:
        snap = ProgressSnapshot(
            phase="done",
            tables_total=2,
            tables_completed=2,
            tables=[
                TableProgress(raw_table_id="r1", status="done"),
                TableProgress(raw_table_id="r2", status="done"),
            ],
        )
        payload = pydantic_data_converter.payload_converter.to_payload(snap)
        back = pydantic_data_converter.payload_converter.from_payload(payload, ProgressSnapshot)
        assert back == snap


class TestGetProgressQuery:
    """The read-only query handler is registered and returns the live snapshot."""

    def test_query_is_registered(self) -> None:
        # Decorated with @workflow.query → carries the Temporal query definition.
        assert hasattr(AddSourceWorkflow.get_progress, "__temporal_query_definition")

    def test_initial_snapshot_is_pre_import_state(self) -> None:
        # __init__ seeds the snapshot so a query landing before the first stage
        # still returns a well-formed value (never None).
        wf = AddSourceWorkflow()
        snap = wf.get_progress()
        assert snap == ProgressSnapshot(phase="import", tables_total=0, tables_completed=0)

    def test_query_returns_the_live_mutable_snapshot(self) -> None:
        # The handler returns ``self._progress`` by reference, so the body's
        # in-place phase/counter mutations are visible to a subsequent query.
        wf = AddSourceWorkflow()
        wf._progress.tables_total = 3  # noqa: SLF001 — exercising the body's mutation
        wf._progress.phase = "processing_tables"  # noqa: SLF001
        wf._progress.tables_completed = 2  # noqa: SLF001
        snap = wf.get_progress()
        assert snap.phase == "processing_tables"
        assert snap.tables_total == 3
        assert snap.tables_completed == 2

    def test_mark_table_flips_one_entrys_status(self) -> None:
        # ``_mark_table`` is how the fan-out flips a table done/failed by id; the
        # query then reflects it. Unknown ids are a no-op (defensive).
        wf = AddSourceWorkflow()
        wf._progress.tables = [  # noqa: SLF001 — exercising the body's mutation
            TableProgress(raw_table_id="r1", status="running"),
            TableProgress(raw_table_id="r2", status="running"),
        ]
        wf._mark_table("r2", "done")  # noqa: SLF001
        wf._mark_table("nope", "failed")  # noqa: SLF001 — no matching entry → no-op
        snap = wf.get_progress()
        assert [(t.raw_table_id, t.status) for t in snap.tables] == [
            ("r1", "running"),
            ("r2", "done"),
        ]


class TestFailureMessage:
    """``_failure_message`` unwraps a workflow error chain to its root message."""

    def test_unwraps_to_innermost_cause(self) -> None:
        from dataraum.worker.workflows import _failure_message

        root = ValueError("typing failed: unparseable date")
        wrapped = RuntimeError("activity error")
        wrapped.__cause__ = root
        outer = RuntimeError("child workflow error")
        outer.__cause__ = wrapped
        assert _failure_message(outer) == "typing failed: unparseable date"

    def test_falls_back_to_type_name_when_no_message(self) -> None:
        from dataraum.worker.workflows import _failure_message

        assert _failure_message(RuntimeError()) == "RuntimeError"
