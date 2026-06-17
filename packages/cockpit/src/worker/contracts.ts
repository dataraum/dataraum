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

/** The INTENTIONAL begin_session trigger (DAT-530): the user chose a table set,
 * so the journey runs `beginSessionWorkflow` as a cross-language child. */
export const RUN_BEGIN_SESSION_SIGNAL = "runBeginSession";

/** Payload of `runBeginSession`. The tool (which has the request context) computes
 * the derived values and passes them, so the sandboxed journey stays free of any
 * workspace IO — it just orchestrates the child + records the run. */
export interface RunBeginSession {
	/** Cockpit-minted session id (the run-correlation key + workflow-id segment). */
	sessionId: string;
	/** The deterministic engine workflow id (tool-computed via beginSessionWorkflowId);
	 * the journey starts the child + records the run under it. Stable across re-runs. */
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
