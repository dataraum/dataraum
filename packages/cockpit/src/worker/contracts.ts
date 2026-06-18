// Shared orchestration contracts (DAT-529) — the names + shapes the WORKFLOW
// (sandbox) and the CLIENT (server functions that signal it) must agree on.
//
// Pure constants + types only: no @temporalio/* or IO imports, so it is safe to
// import from inside the workflow sandbox AND from the server-side client. The
// client refers to the workflow + signal by these string names (mirroring how
// the existing drivers name the Python workflows) rather than importing the
// workflow function, which would drag workflow-runtime guards into the client.

/** The registered type name of the journey workflow (matches the exported
 * `journeyWorkflow` function the worker registers). */
export const JOURNEY_WORKFLOW_TYPE = "journeyWorkflow";

/** The entry signal: a vertical was established for the workspace. */
export const VERTICAL_ESTABLISHED_SIGNAL = "verticalEstablished";

/** One journey per workspace — its workflow id is keyed by the workspace id, so
 * `signalWithStart` always targets the same long-lived execution (continue-as-new
 * preserves the id). */
export function journeyWorkflowId(workspaceId: string): string {
	return `journey-${workspaceId}`;
}

/** Payload of the `verticalEstablished` signal — the vertical the workspace just
 * acquired (a `frame` promotion or a `use_vertical` adoption). */
export interface VerticalEstablished {
	vertical: string;
}

/** The add_source trigger (DAT-551 slice 1): `select` (a fresh import) or `replay`
 * (re-run a session's sources to apply teaches) signals the journey, which runs
 * `addSourceWorkflow` as a cross-language child. The journey is the single owner of
 * stage execution; the agentic grounding-teach loop (slice 2) rides on this. */
export const RUN_ADD_SOURCE_SIGNAL = "runAddSource";

/** Payload of `runAddSource`. Carries `kind` because add_source originates two ways
 * — `onboarding` (select) and `replay` — unlike begin_session (always one kind);
 * the journey threads it to `recordRun`. */
export interface RunAddSource {
	/** The deterministic engine workflow id (`addsource-<ws>`, tool-computed via
	 * addSourceWorkflowId). One per workspace — DAT-562 retired the session segment. */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) the child runs on. */
	engineTaskQueue: string;
	/** The source ids this run imports — a run is over a SET of objects (DAT-422). */
	sources: string[];
	/** The workspace verticals (one today; born-loud on >1). */
	verticals: string[];
	/** How the session originated: `onboarding` (select) or `replay` (re-run). */
	kind: "onboarding" | "replay";
	/** The originating chat (DAT-528) for narration routing. Null = none. */
	conversationId: string | null;
	/** How many grounding-teach replay attempts the autonomous loop may make after
	 * this import (DAT-551 P3c). Optional — the journey defaults it; a future UI can
	 * carry a user-chosen bound on the trigger. */
	numberOfAttempts?: number;
}

/** The INTENTIONAL begin_session trigger (DAT-530): the user chose a table set,
 * so the journey runs `beginSessionWorkflow` as a cross-language child. */
export const RUN_BEGIN_SESSION_SIGNAL = "runBeginSession";

/** Payload of `runBeginSession`. The tool (which has the request context) computes
 * the derived values and passes them, so the sandboxed journey stays free of any
 * workspace IO — it just orchestrates the child + records the run. */
export interface RunBeginSession {
	/** The deterministic engine workflow id (`beginsession-<ws>`, tool-computed via
	 * beginSessionWorkflowId); the journey starts the child + records the run under
	 * it. One per workspace, stable across re-runs (DAT-562). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) the child runs on. */
	engineTaskQueue: string;
	/** The typed table ids to stage. */
	tables: string[];
	/** The workspace verticals (one today; the engine is born-loud on >1). */
	verticals: string[];
	/** The originating chat (DAT-528), captured at the tool boundary so the journey
	 * — which has no request ALS — stamps the run for narration routing. Null = none. */
	conversationId: string | null;
}

/** The MANUAL operating_model trigger (DAT-530 P3b.2): the autonomous cascade runs
 * operating_model automatically after a clean begin_session, but the tool is kept
 * as a re-trigger (a teach re-run, P3c). It signals the journey rather than starting
 * the workflow directly, so the journey stays the single owner of stage execution. */
export const RUN_OPERATING_MODEL_SIGNAL = "runOperatingModel";

/** Payload of `runOperatingModel`. Like {@link RunBeginSession} but for the third
 * stage — no `tables` (the engine re-reads the table set from the catalog head;
 * DAT-506). A manual re-trigger always runs (it is user-intentional, like
 * begin_session — the breaker / auto-mode gate only the AUTONOMOUS cascade). */
export interface RunOperatingModel {
	/** The deterministic engine workflow id (`operatingmodel-<ws>`, tool-computed via
	 * operatingModelWorkflowId). One per workspace (DAT-562). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) the child runs on. */
	engineTaskQueue: string;
	/** The workspace verticals (one today; born-loud on >1) — drive the declared
	 * validations/cycles/metrics. */
	verticals: string[];
	/** The originating chat (DAT-528) for narration routing. Null = none. */
	conversationId: string | null;
}

/** Pause the AUTONOMOUS cascade (the breaker's manual counterpart) — stop
 * auto-advancing begin_session → operating_model. Does NOT kill the journey or a
 * stage already in flight (pause-don't-kill). */
export const PAUSE_AUTO_MODE_SIGNAL = "pauseAutoMode";
/** Re-enable the autonomous cascade and clear the failure tally (the breaker's
 * manual reset). */
export const RESUME_AUTO_MODE_SIGNAL = "resumeAutoMode";

/** The journey-state query (DAT-530) — read the breaker's live state for ops
 * (`temporal workflow query --type journeyState`) and tests. */
export const JOURNEY_STATE_QUERY = "journeyState";

/** The breaker's observable state. `autoMode` off ⇒ the cascade is suspended
 * (tripped by repeated failures or a manual pause); `consecutiveFailures` is the
 * running tally that trips it. */
export interface JourneyState {
	autoMode: boolean;
	consecutiveFailures: number;
}
