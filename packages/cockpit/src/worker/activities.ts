// Orchestration-worker activities (DAT-529, reshaped DAT-530, DAT-609) — the
// side-effecting half of the worker.
//
// MAIN-THREAD (not sandboxed): activities run as ordinary Bun code and reuse the
// existing cockpit control-plane driver in-process — the co-location payoff. The
// orchestration workflows (DAT-609: groundingLoopWorkflow / sessionCascadeWorkflow,
// via the shared `runStage`) start each engine stage as a cross-language CHILD
// workflow (a deterministic command, in the sandbox) and use these activities only
// for the cockpit_db writes around it: record the run before start, attach the
// child's real execution id, mark it terminal on completion.
//
// `recordRun` takes an EXPLICIT conversationId from the workflow (the worker has no
// request ALS) so a stage's completion still narrates into the chat that triggered
// it (DAT-528). These are thin re-exports — the SQL + idempotency are tested in
// db/cockpit/runs.test; the workflows' orchestration is exercised by the DAT-579
// compose-smoke (the sandbox bundle is guarded offline by workflow-bundle.test).
//
// DAT-551 P3c adds `assessAndGround` — a heavier activity that reads the run's
// readiness and runs an LLM to auto-apply mechanical grounding teaches. It is the
// non-deterministic half of the post-add_source grounding loop; the grounding-loop
// workflow's deterministic loop drives the replays around it.

export {
	markRunAwaitingInput,
	markRunStatus,
	recordRun,
} from "#/db/cockpit/runs";
export type { AssessAndGroundResult } from "#/worker/grounding-agent";
export { assessAndGround } from "#/worker/grounding-agent";
