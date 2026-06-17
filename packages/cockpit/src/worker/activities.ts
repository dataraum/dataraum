// JourneyWorkflow activities (DAT-529) — the side-effecting half of the worker.
//
// MAIN-THREAD (not sandboxed): unlike the workflow, activities run as ordinary
// Bun code and may import the existing cockpit driver/DB modules in-process —
// this is the whole point of co-locating the worker (reuse `recordRun` etc.,
// no IPC). Activities may be non-deterministic and do IO freely.
//
// P2 (Phase 0): a stub that proves activity dispatch through the worker without
// pulling in the DB/config (so the runtime can be smoke-tested with no env).
// Phase 1 swaps the body for the real `recordRun`/`markRunStatus` writes.

/**
 * Run one journey stage for the workspace. Phase-0 stub: logs and returns.
 * Phase 1 records a `session_runs` row (tab-independently) via the existing
 * control-plane driver; P3 starts the real engine stage between the records.
 */
export async function startStage(
	workspaceId: string,
	vertical: string,
): Promise<void> {
	console.log(
		`[orchestration-worker] startStage ws=${workspaceId} vertical=${vertical}`,
	);
}
