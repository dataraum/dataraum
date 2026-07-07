// Orchestration trigger seam (DAT-609; workflows moved to the engine in DAT-708)
// — the cockpit-side functions that START the short-lived orchestration workflows
// (grounding-loop, session-cascade) and the DIRECT single-shot engine runs
// (replay, operating_model).
//
// DAT-708 (ADR-0020): the orchestration workflows are Python on the ENGINE
// worker, so each trigger starts them by type name on the workspace's OWN engine
// queue — the same client-by-string pattern as the analysis workflows. The wire
// payloads are engine-owned (hand-mirrored in ./types.ts); the trigger maps its
// caller-facing camelCase spec onto them and injects the cockpit's activity
// queue (`config.cockpitOrchestrationTaskQueue`), which the workflow schedules
// the run writers + teach agent back onto.
//
// Each trigger `start`s a workflow by its deterministic per-workspace id.
// Single-flight is the workflow-id reuse policy (ALLOW_DUPLICATE once the prior
// is CLOSED + conflict FAIL while one is RUNNING): a second start while one runs
// raises `WorkflowExecutionAlreadyStartedError`, which the caller turns into an
// actionable message (`RunAlreadyRunningError`).
//
// SERVER-ONLY (Temporal client + cockpit_db).

import {
	Client,
	Connection,
	WorkflowExecutionAlreadyStartedError,
} from "@temporalio/client";

import { config } from "#/config";
import { type RunKind, type RunStage, recordRun } from "#/db/cockpit/runs";
import { AgentActionableError } from "#/tools/agent-error";
import type { GroundingLoopInput, SessionCascadeInput } from "./types";
import {
	groundingLoopWorkflowId,
	sessionCascadeWorkflowId,
} from "./workflow-id";

// Registered type names of the two engine-hosted orchestration workflows
// (`@workflow.defn(name=...)` in the engine's worker/workflows.py) — the client
// refers to them by string, exactly like the analysis workflow drivers.
const GROUNDING_LOOP_WORKFLOW_TYPE = "groundingLoopWorkflow";
const SESSION_CASCADE_WORKFLOW_TYPE = "sessionCascadeWorkflow";

/** Temporal-unconfigured guard, mirroring the engine-workflow drivers: Temporal
 * config is OPTIONAL in config.ts, so fail loud (not silent) when it isn't wired. */
function requireTemporalConfig(): { host: string; namespace: string } {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}
	return { host: config.temporalHost, namespace: config.temporalNamespace };
}

/** Run `fn` with a short-lived Temporal client, always closing the connection. */
async function withClient<T>(fn: (client: Client) => Promise<T>): Promise<T> {
	const { host, namespace } = requireTemporalConfig();
	const connection = await Connection.connect({ address: host });
	try {
		return await fn(new Client({ connection, namespace }));
	} finally {
		await connection.close();
	}
}

// Single-flight per workflow id: re-start once the prior is CLOSED (ALLOW_DUPLICATE),
// reject (FAIL) while one is RUNNING — so two executions of one id never overlap.
// (String literals: the SDK's policy enums are string unions and aren't re-exported
// as values from @temporalio/client; `as const` keeps them assignable to the union.)
const SINGLE_FLIGHT = {
	workflowIdReusePolicy: "ALLOW_DUPLICATE",
	workflowIdConflictPolicy: "FAIL",
} as const;

/** Raised when a `start` is rejected because the same workflow id is already running.
 * An AGENT-ACTIONABLE precondition ("wait for it to finish"), so it extends
 * `AgentActionableError` — `catchActionable` turns it into a clean `{ error }` and
 * infra errors still propagate. */
export class RunAlreadyRunningError extends AgentActionableError {
	constructor(message: string) {
		super(message);
		this.name = "RunAlreadyRunningError";
	}
}

/** Caller-facing spec for {@link startGroundingLoop}. camelCase like the rest of
 * the cockpit; the trigger maps it onto the engine-owned snake_case wire payload
 * (`GroundingLoopInput` in ./types.ts) and injects the cockpit activity queue. */
export interface StartGroundingLoopSpec {
	/** The workspace id — the routing key + the recordRun scope. */
	workspaceId: string;
	/** The deterministic ENGINE child id (`addsource-<ws>`) the import + its
	 * replays run under. */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) — where the
	 * orchestration workflow itself runs (DAT-708); its engine children inherit it. */
	engineTaskQueue: string;
	/** The source ids this run imports — a run is over a SET of objects (DAT-422). */
	sources: string[];
	/** The workspace verticals (one today; born-loud on >1). */
	verticals: string[];
	/** The originating chat (DAT-528); null = a non-narrating run. */
	conversationId: string | null;
	/** Grounding-teach replay budget (workflow defaults to 3 when omitted). */
	numberOfAttempts?: number;
}

/**
 * Start the workspace's grounding-loop workflow (onboarding import + autonomous
 * teach-and-replay loop). The workflow records its own runs (via its stage
 * bracket); this only kicks it off on the workspace's engine queue under the
 * per-ws id.
 */
export function startGroundingLoop(
	spec: StartGroundingLoopSpec,
): Promise<void> {
	const input: GroundingLoopInput = {
		workspace_id: spec.workspaceId,
		workflow_id: spec.workflowId,
		cockpit_task_queue: config.cockpitOrchestrationTaskQueue,
		sources: spec.sources,
		verticals: spec.verticals,
		conversation_id: spec.conversationId,
		...(spec.numberOfAttempts !== undefined
			? { number_of_attempts: spec.numberOfAttempts }
			: {}),
	};
	return startOrchestration(
		GROUNDING_LOOP_WORKFLOW_TYPE,
		groundingLoopWorkflowId(spec.workspaceId),
		spec.engineTaskQueue,
		input,
		"An import is already running for this workspace — wait for it to finish.",
	);
}

/** Caller-facing spec for {@link startSessionCascade} — see
 * {@link StartGroundingLoopSpec} for the mapping rationale. */
export interface StartSessionCascadeSpec {
	workspaceId: string;
	/** The deterministic ENGINE child id for begin_session (`beginsession-<ws>`). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`) — where the
	 * orchestration workflow itself runs (DAT-708). */
	engineTaskQueue: string;
	/** The typed table ids to stage. */
	tables: string[];
	/** The workspace verticals (one today; born-loud on >1). */
	verticals: string[];
	/** The originating chat (DAT-528) — rides to BOTH children; null = none. */
	conversationId: string | null;
}

/**
 * Start the workspace's session-cascade workflow (begin_session → clean →
 * operating_model). The workflow records both runs; this only kicks it off.
 */
export function startSessionCascade(
	spec: StartSessionCascadeSpec,
): Promise<void> {
	const input: SessionCascadeInput = {
		workspace_id: spec.workspaceId,
		workflow_id: spec.workflowId,
		cockpit_task_queue: config.cockpitOrchestrationTaskQueue,
		tables: spec.tables,
		verticals: spec.verticals,
		conversation_id: spec.conversationId,
	};
	return startOrchestration(
		SESSION_CASCADE_WORKFLOW_TYPE,
		sessionCascadeWorkflowId(spec.workspaceId),
		spec.engineTaskQueue,
		input,
		"A session is already running for this workspace — wait for it to finish.",
	);
}

async function startOrchestration(
	type: string,
	workflowId: string,
	taskQueue: string,
	input: unknown,
	busyMessage: string,
): Promise<void> {
	try {
		await withClient((client) =>
			client.workflow.start(type, {
				taskQueue,
				workflowId,
				args: [input],
				...SINGLE_FLIGHT,
			}),
		);
	} catch (err) {
		if (err instanceof WorkflowExecutionAlreadyStartedError) {
			throw new RunAlreadyRunningError(busyMessage);
		}
		throw err;
	}
}

/** A direct single-shot engine run (replay add_source, manual operating_model) — no
 * orchestration workflow, because there is no follow-on stage to await. The tool
 * starts the engine workflow, then records the run with its REAL execution id
 * (DAT-595). */
export interface DirectRunSpec {
	workspaceId: string;
	/** The run's origin for the run row (replay → "replay"; manual OM → "begin_session"). */
	kind: RunKind;
	stage: RunStage;
	/** The engine workflow type name + its deterministic id + queue. */
	workflowType: string;
	workflowId: string;
	taskQueue: string;
	args: unknown[];
	/** The message surfaced when the id is already running. */
	busyMessage: string;
}

/**
 * Run a direct single-shot engine workflow: start it, then record the run with the
 * REAL execution id (DAT-595 — recording post-start with the real id keeps every run
 * a distinct `(workflowId, runId)` row under the reused `addsource-<ws>` id, retiring
 * the placeholder + attachRunId swap that conflated runs). Recording AFTER start drops
 * nothing on a conflict/failure — the run never started, so there's no row to clean up.
 * `recordRun` omits `conversationId` ⇒ it falls back to the request-scoped ALS (these
 * run inside the chat turn, unlike the worker) so the completion still narrates into
 * THIS chat. Orphan-safety differs from the workflow's durable stage bracket
 * (`_run_stage` on the engine worker): this is a request
 * handler, so a crash in the tiny start→record window fails the HTTP request and the
 * user re-triggers — idempotent via recordRun's `onConflictDoNothing`.
 */
export async function startDirectRun(spec: DirectRunSpec): Promise<void> {
	let runId: string;
	try {
		runId = await withClient((client) =>
			client.workflow
				.start(spec.workflowType, {
					taskQueue: spec.taskQueue,
					workflowId: spec.workflowId,
					args: spec.args,
					...SINGLE_FLIGHT,
				})
				.then((handle) => handle.firstExecutionRunId),
		);
	} catch (err) {
		if (err instanceof WorkflowExecutionAlreadyStartedError) {
			throw new RunAlreadyRunningError(spec.busyMessage);
		}
		throw err;
	}
	// Record with the real execution id, right after start.
	await recordRun({
		workspaceId: spec.workspaceId,
		kind: spec.kind,
		stage: spec.stage,
		workflowId: spec.workflowId,
		runId,
	});
}
