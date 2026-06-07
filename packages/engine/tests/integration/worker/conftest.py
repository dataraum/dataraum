"""Shared Temporal dev-server fixtures for worker workflow tests.

A single-container Temporal CLI dev server (``server start-dev``) + a client
bound to it with the worker's pydantic data converter. Used by the workflow
exec + offline-``Replayer`` determinism tests (``test_progress_query`` for
add_source, ``test_begin_session_progress_query`` for begin_session).

We use the CLI dev server (in-memory SQLite, one container) instead of
``WorkflowEnvironment.start_time_skipping()`` — that downloads a test-server
binary that stalls CI (project convention: Temporal tests use testcontainers;
determinism is covered offline by the ``Replayer``).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import RPCError
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)
from testcontainers.core.container import DockerContainer

# Passthrough modules for the workflow sandbox in tests. `dataraum`/`pydantic`
# mirror the production worker. `coverage`+`platform`: under `pytest --cov` (CI)
# the coverage sysmon branch tracer fires inside the sandboxed workflow and
# lazily imports `coverage.env`, which calls `platform.python_implementation()` —
# a call the sandbox forbids, failing workflow activation. Passing them through
# routes coverage to the host modules so the tracer never trips the determinism
# guard. Test-only — the production worker doesn't run under coverage.
_SANDBOX_PASSTHROUGH = ("dataraum", "pydantic", "pydantic_core", "coverage", "platform")


def make_sandboxed_runner() -> SandboxedWorkflowRunner:
    """A sandbox runner with the test passthrough modules applied."""
    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(*_SANDBOX_PASSTHROUGH)
    )


@pytest.fixture(scope="module")
def temporal_dev_address() -> Iterator[str]:
    """A single-container Temporal CLI dev server (``server start-dev``).

    Addressing uses the standard testcontainers port-MAPPING idiom — expose the
    frontend gRPC (7233) and reach it via ``get_container_host_ip()`` + the
    mapped host port (NOT ``network_mode="host"`` + a fixed port, which routes on
    local Docker but not on CI runners).
    """
    container = (
        DockerContainer("temporalio/temporal:latest")
        .with_command("server start-dev --ip 0.0.0.0 --namespace default")
        .with_exposed_ports(7233)
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(7233)
        yield f"{host}:{port}"
    finally:
        container.stop()


@pytest.fixture
async def temporal_client(temporal_dev_address: str) -> AsyncIterator[Client]:
    """Client bound to the dev server with the worker's pydantic data converter.

    The dev server's gRPC frontend lags the container start, so connect with a
    bounded retry — a log line doesn't prove the frontend is accepting RPCs.
    ``Client.connect`` is eager (runs ``get_system_info``) and raises a bare
    ``RuntimeError`` ("connection closed" / Cancelled) while the server is still
    booting — so that, not just ``RPCError``, is the retry signal.
    """
    last_err: Exception | None = None
    for _ in range(120):  # ~60s budget after the image is warmed
        try:
            client = await Client.connect(
                temporal_dev_address,
                namespace="default",
                data_converter=pydantic_data_converter,
            )
            yield client
            return
        except (RPCError, RuntimeError, OSError) as err:
            last_err = err
            await asyncio.sleep(0.5)
    raise RuntimeError(
        f"Temporal dev server at {temporal_dev_address} never accepted a connection: {last_err}"
    )
