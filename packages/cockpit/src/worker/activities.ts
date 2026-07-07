// The cockpit's Temporal activities (DAT-529, reshaped DAT-530/609/708) — the
// full registration surface of the activity-only worker.
//
// The orchestration WORKFLOWS that call these run in Python on the ENGINE worker
// (DAT-708, ADR-0020); they schedule these activities cross-language BY NAME on
// the cockpit's activity queue. The names + IO shapes are therefore a
// cross-PACKAGE contract: the engine mirrors them as camelCase Pydantic models
// (`worker/contracts.py` — `RecordRunInput`, `AssessAndGroundInput/Result`, the
// positional `markRunStatus`/`markRunAwaitingInput` args). Renaming an export or
// reshaping its IO here is a silent wire break — change both sides in lockstep.
//
// The activities run as ordinary Bun code on the main isolate (no vm sandbox)
// and reuse the cockpit control-plane driver in-process — the co-location
// payoff. The workflows use them to bracket each engine stage: record the run
// (with the child's real execution id) after start, mark it terminal on
// completion, park it awaiting input.
//
// `recordRun` takes an EXPLICIT conversationId from the workflow (the worker has
// no request ALS) so a stage's completion still narrates into the chat that
// triggered it (DAT-528). These are thin re-exports — the SQL + idempotency are
// tested in db/cockpit/runs.test; the orchestration around them is exercised by
// the compose smoke (a live grounding loop / session cascade run).
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
