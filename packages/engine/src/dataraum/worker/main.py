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
from temporalio.contrib.opentelemetry import TracingInterceptor
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
from dataraum.worker.telemetry import init_telemetry
from dataraum.worker.workflows import (
    AddSourceWorkflow,
    BeginSessionWorkflow,
    GroundingLoopWorkflow,
    OperatingModelWorkflow,
    ProcessTableWorkflow,
    SessionCascadeWorkflow,
)

logger = get_logger(__name__)

# Bounded by the substrate's connection pools (SQLAlchemy pool_size+overflow and
# the DuckLake Postgres-extension pool raised in bootstrap_lake), not by a fear
# of DuckDB concurrency — DuckDB/DuckLake handle concurrent writers via MVCC +
# optimistic concurrency, and Temporal retries the rare commit conflict.
_MAX_CONCURRENT_ACTIVITIES = 8


def worker_activities(phase_activities: PhaseActivities) -> list[object]:
    """Every activity the bundled worker registers — the single source of truth.

    The workflows call activities by NAME string, so a phase present in a
    workflow chain but missing here fails only at runtime (``NotFoundError``
    mid-workflow — how the DAT-491 ``aggregation_lineage`` miss surfaced).
    ``tests/unit/worker/test_worker_registration.py`` guards this list against
    the workflow chains.
    """
    return [
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
        # DAT-491/536 events→measure lineage — the structural witness's supply
        # step (inline aggregation over the enriched views, in the value order).
        phase_activities.run_aggregation_lineage,
        phase_activities.run_enriched_views,
        # DAT-403 value layer — runs after enriched_views.
        phase_activities.run_slicing,
        # DAT-537 g3 drill-down / alias discovery over the slice catalog.
        phase_activities.run_dimension_hierarchies,
        phase_activities.run_correlations,
        # DAT-546: per-measure driver rankings persisted run-versioned (last value phase).
        phase_activities.run_driver_rankings,
        # DAT-408/409 begin_session: materialize durable overlays →
        # terminal detect → silent-accept keepers → promote.
        phase_activities.run_session_materialize_overlays,
        # DAT-277: mint surrogate keys for confirmed composites — after the
        # overlays, before enriched_views consumes the catalog.
        phase_activities.run_surrogate_mint,
        phase_activities.run_session_detect,
        phase_activities.run_session_write_keepers,
        phase_activities.run_session_promote_to_latest,
        # DAT-438/455/456 operating_model spine — resolve (pins) →
        # validation → terminal detect (DAT-432: cross_table_consistency
        # bands) → business_cycles → metrics lifecycle families → promote
        # the stage head.
        phase_activities.run_operating_model_resolve,
        phase_activities.run_validation,
        phase_activities.run_operating_model_detect,
        phase_activities.run_business_cycles,
        phase_activities.run_metrics,
        phase_activities.run_operating_model_promote,
    ]


def _activity_names(activities: list[object]) -> list[str]:
    """The registered Temporal names, read off the ``@activity.defn`` metadata."""
    return [
        getattr(act, "__temporal_activity_definition").name  # noqa: B009 — dunder set by temporalio, not a static attr
        for act in activities
    ]


async def run_worker() -> None:
    """Bootstrap the substrate, then poll the task queue until interrupted."""
    # TEMPORAL_* are required in Settings (DAT-369): get_settings() fails loud
    # at construction naming any unset field, so the reads below are total.
    settings = get_settings()
    host = settings.temporal_host
    namespace = settings.temporal_namespace
    task_queue = settings.temporal_task_queue

    # OTel tracing + log shipping (ADR-0019/DAT-705/707) — no-op unless
    # OTEL_EXPORTER_OTLP_ENDPOINT is set. On the Client, TracingInterceptor covers BOTH directions: outbound
    # client calls AND this worker's workflow/activity spans. Every workflow
    # start arrives from the traced cockpit client (ADR-0020 — the orchestration
    # workflows run HERE and the cockpit is the only starter), so each run is
    # one connected trace over the shared `_tracer-data` header (W3C). The
    # workflow OUTBOUND side propagates that context to activities, children AND
    # continue_as_new — the grounding loop's replay chain stays one trace — and
    # across the queue hop to the cockpit's activity-only worker (its run
    # writers + teach agent join via the same header). Deliberately NO
    # `always_create_workflow_spans`: it exists for client-context-LESS starts
    # (CLI/schedules/smoke scripts), which stay untraced rather than minting
    # orphan-parented trace shards.
    telemetry = init_telemetry(settings)

    # Substrate bootstrap strictly precedes worker.run() — the worker must not
    # advertise itself as polling until its DuckLake anchor + ConnectionManager
    # are open (the worker-health invariant P4 relies on).
    manager = bootstrap_worker_substrate()
    try:
        client = await Client.connect(
            host,
            namespace=namespace,
            data_converter=pydantic_data_converter,  # PhaseActivity{Input,Result} are Pydantic
            interceptors=[TracingInterceptor()] if telemetry else [],
        )
        phase_activities = PhaseActivities(manager)
        activities = worker_activities(phase_activities)

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
                workflows=[
                    AddSourceWorkflow,
                    ProcessTableWorkflow,
                    BeginSessionWorkflow,
                    OperatingModelWorkflow,
                    # Orchestration (DAT-708, ADR-0020): the grounding loop +
                    # session cascade run HERE, starting the analysis workflows
                    # above as native children; their cockpit-bound activities
                    # (run writers, teach agent) are scheduled by name on the
                    # cockpit's activity-only queue, NOT registered here.
                    GroundingLoopWorkflow,
                    SessionCascadeWorkflow,
                ],
                activities=activities,  # type: ignore[arg-type]  # bound @activity.defn methods
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
                        # opentelemetry: the contrib TracingWorkflowInboundInterceptor
                        # imports it inside the sandbox; the contrib module is
                        # engineered to survive a sandboxed import (it eagerly
                        # loads the otel context at import time), but passing it
                        # through skips a per-workflow re-import of the whole
                        # otel package. Deterministic-safe: workflow-side spans
                        # are created by the interceptor around task boundaries.
                        "dataraum",
                        "pydantic",
                        "pydantic_core",
                        "opentelemetry",
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
                    "operatingModelWorkflow",
                    "groundingLoopWorkflow",
                    "sessionCascadeWorkflow",
                ],
                activities=_activity_names(activities),
            )
            async with worker:
                await interrupt.wait()
            logger.info("worker_stopping")
    finally:
        shutdown_worker_substrate(manager)
        if telemetry:
            telemetry.shutdown()  # flush buffered spans + log records before exit


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
