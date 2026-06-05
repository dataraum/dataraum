"""Temporal activity-worker entrypoint (DAT-344, P2).

Run as ``python -m dataraum.worker.main`` (the engine container's command).

Boots the substrate strictly before polling: open the DuckLake anchor + one
workspace ``ConnectionManager`` (:func:`bootstrap_worker_substrate`), connect to
Temporal, then run a worker that serves the phase activities on a
``ThreadPoolExecutor`` (sync activities). On SIGTERM/SIGINT it stops polling and
tears the substrate down.
"""

from __future__ import annotations

import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings
from dataraum.worker.activities import PhaseActivities
from dataraum.worker.bootstrap import (
    bootstrap_worker_substrate,
    shutdown_worker_substrate,
)
from dataraum.worker.workflows import (
    AddSourceWorkflow,
    BeginSessionWorkflow,
    ProcessTableWorkflow,
)

logger = get_logger(__name__)

# Bounded by the substrate's connection pools (SQLAlchemy pool_size+overflow and
# the DuckLake Postgres-extension pool raised in bootstrap_lake), not by a fear
# of DuckDB concurrency — DuckDB/DuckLake handle concurrent writers via MVCC +
# optimistic concurrency, and Temporal retries the rare commit conflict.
_MAX_CONCURRENT_ACTIVITIES = 8


async def run_worker() -> None:
    """Bootstrap the substrate, then poll the task queue until interrupted."""
    # TEMPORAL_* are required in Settings (DAT-369): get_settings() fails loud
    # at construction naming any unset field, so the reads below are total.
    settings = get_settings()
    host = settings.temporal_host
    namespace = settings.temporal_namespace
    task_queue = settings.temporal_task_queue

    # Substrate bootstrap strictly precedes worker.run() — the worker must not
    # advertise itself as polling until its DuckLake anchor + ConnectionManager
    # are open (the worker-health invariant P4 relies on).
    manager = bootstrap_worker_substrate()
    try:
        client = await Client.connect(
            host,
            namespace=namespace,
            data_converter=pydantic_data_converter,  # PhaseActivity{Input,Result} are Pydantic
        )
        phase_activities = PhaseActivities(manager)

        interrupt = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, interrupt.set)

        with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_ACTIVITIES) as executor:
            # Bundled worker (Temporal's recommended default): this one worker
            # runs the workflow (asyncio, in the determinism sandbox) AND the
            # phase activities (sync, on the ThreadPoolExecutor) on one task
            # queue. Split activities onto a dedicated task queue later if the
            # heavy phases need to scale independently of orchestration.
            worker = Worker(
                client,
                task_queue=task_queue,
                workflows=[AddSourceWorkflow, ProcessTableWorkflow, BeginSessionWorkflow],
                activities=[
                    phase_activities.run_import,
                    # DAT-430 run-scoped column gate — between the import loop
                    # and the per-table fan-out.
                    phase_activities.run_check_column_limit,
                    phase_activities.run_typing,
                    phase_activities.run_statistics,
                    phase_activities.run_column_eligibility,
                    phase_activities.run_statistical_quality,
                    phase_activities.run_temporal,
                    phase_activities.run_semantic_per_column,
                    phase_activities.run_detect,
                    phase_activities.run_promote_to_latest,
                    # DAT-401 begin_session spine — source-free, session-scoped.
                    phase_activities.run_begin_session_select,
                    phase_activities.run_relationships,
                    phase_activities.run_semantic_per_table,
                    phase_activities.run_enriched_views,
                    # DAT-403 value layer — runs after enriched_views.
                    phase_activities.run_slicing,
                    phase_activities.run_slicing_view,
                    phase_activities.run_slice_analysis,
                    phase_activities.run_temporal_slice_analysis,
                    phase_activities.run_correlations,
                    # DAT-408/409 begin_session: materialize durable overlays →
                    # terminal detect → silent-accept keepers → promote.
                    phase_activities.run_session_materialize_overlays,
                    phase_activities.run_session_detect,
                    phase_activities.run_session_write_keepers,
                    phase_activities.run_session_promote_to_latest,
                ],
                activity_executor=executor,
                max_concurrent_activities=_MAX_CONCURRENT_ACTIVITIES,
                # The workflow module lives in the `dataraum` package, whose
                # import chain loads the engine — including duckdb's native
                # extension, which cannot be reimported inside the workflow
                # sandbox. The workflow only uses the pure-data contracts (it
                # never calls engine code), so pass `dataraum` through to the
                # host's already-imported modules. Runtime determinism guards
                # (banned time/random/etc.) still apply.
                workflow_runner=SandboxedWorkflowRunner(
                    restrictions=SandboxRestrictions.default.with_passthrough_modules(
                        "dataraum", "pydantic", "pydantic_core"
                    )
                ),
            )
            logger.info(
                "worker_started",
                task_queue=task_queue,
                namespace=namespace,
                host=host,
                workflows=[
                    "addSourceWorkflow",
                    "processTableWorkflow",
                    "beginSessionWorkflow",
                ],
                activities=[
                    "import",
                    "check_column_limit",
                    "typing",
                    "statistics",
                    "column_eligibility",
                    "statistical_quality",
                    "temporal",
                    "semantic_per_column",
                    "detect",
                    "promote_to_latest",
                    "begin_session_select",
                    "relationships",
                    "semantic_per_table",
                    "enriched_views",
                    "slicing",
                    "slicing_view",
                    "slice_analysis",
                    "temporal_slice_analysis",
                    "correlations",
                    "session_materialize_overlays",
                    "session_detect",
                    "session_write_keepers",
                    "session_promote_to_latest",
                ],
            )
            async with worker:
                await interrupt.wait()
            logger.info("worker_stopping")
    finally:
        shutdown_worker_substrate(manager)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
