// Cockpit task-queue convention (DAT-818) — cockpit side.
//
// One cockpit per workspace (DD/51740673): the activity-only worker polls the
// workspace's OWN queue, `cockpit-<workspace_id>`, derived from the boot
// identity (`config.dataraumWorkspaceId`) — never from per-request state and
// never from a config knob (the singleton `cockpitOrchestrationTaskQueue` is
// retired). Hand-mirror of the engine's
// `dataraum.worker.contracts.cockpit_task_queue_for`, which the orchestration
// workflows use to derive the SAME name from their input `workspace_id` when
// scheduling the run writers + the grounding-teach agent back onto the cockpit.
// A drift on either side strands those callbacks on an unpolled queue — the
// mirror is pinned by a literal test on each side (task-queue.test.ts here,
// test_workflow_ids.py in the engine).
//
// Sibling of `engineTaskQueueFor` (`#/db/cockpit/registry`), which derives the
// workspace's ENGINE queue (`engine-<ws>`) for starting workflows; this one
// names the queue the cockpit itself polls.

/**
 * The workspace's cockpit activity queue — `cockpit-<workspace_id>`. The boot
 * plugin polls it; the engine-hosted orchestration workflows schedule the
 * cockpit-bound activities onto it.
 */
export function cockpitTaskQueueFor(workspaceId: string): string {
	return `cockpit-${workspaceId}`;
}
