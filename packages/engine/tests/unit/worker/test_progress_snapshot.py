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

from dataraum.worker.contracts import ProgressSnapshot
from dataraum.worker.workflows import AddSourceWorkflow


class TestProgressSnapshotContract:
    """The wire shape DAT-352 mirrors — fields, defaults, and JSON form."""

    def test_fields_and_defaults(self) -> None:
        snap = ProgressSnapshot(phase="import")
        assert snap.phase == "import"
        assert snap.tables_total == 0
        assert snap.tables_completed == 0

    def test_is_a_plain_dataclass_not_pydantic(self) -> None:
        # A plain stdlib dataclass (NOT a Pydantic BaseModel): it lives in the
        # determinism sandbox and serializes via the worker's pydantic converter.
        assert dataclasses.is_dataclass(ProgressSnapshot)
        assert not hasattr(ProgressSnapshot, "model_fields")

    def test_serializes_to_frozen_json_shape(self) -> None:
        # The cockpit Client (TS) consumes the query result off the wire, so the
        # JSON keys are the contract. Assert the flat shape byte-for-byte.
        snap = ProgressSnapshot(phase="processing_tables", tables_total=3, tables_completed=1)
        payload = pydantic_data_converter.payload_converter.to_payload(snap)
        assert payload.data == (
            b'{"phase":"processing_tables","tables_total":3,"tables_completed":1}'
        )

    def test_roundtrips_through_the_worker_converter(self) -> None:
        snap = ProgressSnapshot(phase="done", tables_total=2, tables_completed=2)
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
