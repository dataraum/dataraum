// Shared orchestration contracts (DAT-529; collapsed to short-lived workflows in
// DAT-609) — the names + shapes the WORKFLOW (sandbox) and the CLIENT (server
// functions that start it) must agree on.
//
// Pure constants + types only: no @temporalio/* or IO imports, so it is safe to
// import from inside the workflow sandbox AND from the server-side client. The
// client refers to the workflows by these string type names (mirroring how the
// engine-workflow drivers name the Python workflows) rather than importing the
// workflow function, which would drag workflow-runtime guards into the client.
//
// The singleton journey (signals / breaker state / JourneyState query) is gone
// (DAT-609): each trigger STARTS one of the two short-lived per-trigger workflows
// below by its deterministic per-workspace id (single-flight = the id-reuse policy),
// and the single-shot stages (replay, operating_model) start the engine workflow
// directly. State rides the start payload, not replayed history.

/** Registered type name of the onboarding import + grounding-teach loop workflow
 * (matches the exported `groundingLoopWorkflow`). */
export const GROUNDING_LOOP_WORKFLOW_TYPE = "groundingLoopWorkflow";

/** Registered type name of the begin_session → operating_model cascade workflow
 * (matches the exported `sessionCascadeWorkflow`). */
export const SESSION_CASCADE_WORKFLOW_TYPE = "sessionCascadeWorkflow";

/** Start payload of {@link GROUNDING_LOOP_WORKFLOW_TYPE}. The tool (which has the
 * request context) computes the derived ids/queue and captures the conversationId,
 * so the sandboxed workflow stays free of any workspace IO — it just runs the import
 * child + the bounded teach loop. Triggered ONLY by the onboarding import (`select`);
 * a manual `replay` is a DIRECT engine start, not this loop. */
export interface GroundingLoopInput {
	/** The workspace id — the routing key + the recordRun scope. */
	workspaceId: string;
	/** The deterministic ENGINE child id (`addsource-<ws>`) the import + its replays
	 * run under (reused across attempts; the SDK groups the iterations). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) the child runs on. */
	engineTaskQueue: string;
	/** The source ids this run imports — a run is over a SET of objects (DAT-422). */
	sources: string[];
	/** The workspace verticals (one today; born-loud on >1). */
	verticals: string[];
	/** The originating chat (DAT-528) for the import run's progress routing. The
	 * onboarding import is recorded under this id (so the watcher tracks its progress)
	 * but never narrated into chat (DAT-597). Null = none. */
	conversationId: string | null;
	/** How many grounding-teach replay attempts the loop may make (default 3). */
	numberOfAttempts?: number;
}

/** Start payload of {@link SESSION_CASCADE_WORKFLOW_TYPE}. begin_session runs first;
 * a clean result cascades into operating_model (the OM child id is derived inside the
 * workflow via the pure `operatingModelWorkflowId` helper — it reuses the same queue +
 * verticals + conversationId). */
export interface SessionCascadeInput {
	/** The workspace id — routing key + recordRun scope. */
	workspaceId: string;
	/** The deterministic ENGINE child id for begin_session (`beginsession-<ws>`). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) both children run on. */
	engineTaskQueue: string;
	/** The typed table ids to stage. */
	tables: string[];
	/** The workspace verticals (one today; born-loud on >1). */
	verticals: string[];
	/** The originating chat (DAT-528) — rides to BOTH children so the watcher narrates
	 * each completion into the originating chat. Null = none. */
	conversationId: string | null;
}
