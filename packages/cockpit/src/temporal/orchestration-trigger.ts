// Orchestration trigger seam (DAT-609) — the cockpit-side functions that START the
// short-lived orchestration workflows (grounding-loop, session-cascade) and the
// DIRECT single-shot engine runs (replay, operating_model).
//
// Replaces the DAT-529 journey-trigger: the singleton journey is gone, so there is no
// `signalWithStart`. Each trigger `start`s a workflow by its deterministic
// per-workspace id. Single-flight is the workflow-id reuse policy
// (ALLOW_DUPLICATE once the prior is CLOSED + conflict FAIL while one is RUNNING): a
// second start while one runs raises `WorkflowExecutionAlreadyStartedError`, which the
// caller turns into an actionable message (`RunAlreadyRunningError`).
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
import {
	GROUNDING_LOOP_WORKFLOW_TYPE,
	type GroundingLoopInput,
	SESSION_CASCADE_WORKFLOW_TYPE,
	type SessionCascadeInput,
} from "#/worker/contracts";
import {
	groundingLoopWorkflowId,
	sessionCascadeWorkflowId,
} from "./workflow-id";

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

/**
 * Start the workspace's grounding-loop workflow (onboarding import + autonomous
 * teach-and-replay loop). The workflow records its own runs (via `runStage`); this
 * only kicks it off on the `cockpit-orchestration` queue under the per-ws id.
 */
export function startGroundingLoop(input: GroundingLoopInput): Promise<void> {
	return startOrchestration(
		GROUNDING_LOOP_WORKFLOW_TYPE,
		groundingLoopWorkflowId(input.workspaceId),
		input,
		"An import is already running for this workspace — wait for it to finish.",
	);
}

/**
 * Start the workspace's session-cascade workflow (begin_session → clean →
 * operating_model). The workflow records both runs; this only kicks it off.
 */
export function startSessionCascade(input: SessionCascadeInput): Promise<void> {
	return startOrchestration(
		SESSION_CASCADE_WORKFLOW_TYPE,
		sessionCascadeWorkflowId(input.workspaceId),
		input,
		"A session is already running for this workspace — wait for it to finish.",
	);
}

async function startOrchestration(
	type: string,
	workflowId: string,
	input: unknown,
	busyMessage: string,
): Promise<void> {
	try {
		await withClient((client) =>
			client.workflow.start(type, {
				taskQueue: config.cockpitOrchestrationTaskQueue,
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
 * THIS chat. Orphan-safety differs from the durable `runStage` path: this is a request
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
