// JourneyWorkflow activities (DAT-529, reshaped DAT-530) — the side-effecting half
// of the worker.
//
// MAIN-THREAD (not sandboxed): activities run as ordinary Bun code and reuse the
// existing cockpit control-plane driver in-process — the co-location payoff. The
// journey (P3b) starts the engine stage as a cross-language CHILD workflow (a
// deterministic workflow command, in the sandbox) and uses these activities only
// for the cockpit_db writes around it: record the run before start, attach the
// child's real execution id, mark it terminal on completion.
//
// `recordRun` takes an EXPLICIT conversationId from the journey (the worker has no
// request ALS) so a stage's completion still narrates into the chat that triggered
// it (DAT-528). These are thin re-exports — the SQL + idempotency are tested in
// db/cockpit/runs.test; the journey's orchestration is tested via the Replayer +
// compose-smoke.

export { attachRunId, markRunStatus, recordRun } from "#/db/cockpit/runs";
