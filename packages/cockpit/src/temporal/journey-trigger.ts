// JourneyWorkflow ENTRY trigger (DAT-529) — signal that a workspace's vertical
// is established, starting or advancing its orchestration journey.
//
// This is the cockpit-side seam the vertical-acquisition step calls (a `frame`
// promotion or a `use_vertical` adoption — wired in a later phase). It targets
// the cockpit's OWN orchestration worker (the `cockpit-orchestration` queue +
// the co-located TS JourneyWorkflow), NOT the engine's per-workspace analysis
// queues — so it refers to the workflow + signal by their string names (from
// ../worker/contracts), mirroring how the engine-workflow drivers name theirs.
//
// `signalWithStart` is idempotent for a long-lived per-workspace workflow: if the
// journey is already running (incl. across continue-as-new — the workflow id is
// stable), it just delivers the signal; otherwise it starts the journey and
// signals it in one call. So a workspace has exactly one journey execution.

import { Client, Connection } from "@temporalio/client";

import { config } from "#/config";
import {
	JOURNEY_WORKFLOW_TYPE,
	journeyWorkflowId,
	PAUSE_AUTO_MODE_SIGNAL,
	RESUME_AUTO_MODE_SIGNAL,
	RUN_ADD_SOURCE_SIGNAL,
	RUN_BEGIN_SESSION_SIGNAL,
	RUN_OPERATING_MODEL_SIGNAL,
	type RunAddSource,
	type RunBeginSession,
	type RunOperatingModel,
	VERTICAL_ESTABLISHED_SIGNAL,
	type VerticalEstablished,
} from "#/worker/contracts";

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

/**
 * `signalWithStart` the workspace's journey with `signal`, starting it if it
 * isn't running yet. Idempotent for the long-lived per-workspace journey: a
 * running journey (incl. across continue-as-new — the workflow id is stable) just
 * receives the signal; otherwise it's started and signalled in one call. So a
 * workspace has exactly one journey execution. Returns the journey workflow id.
 */
async function signalJourney(
	workspaceId: string,
	signal: string,
	signalArgs: unknown[],
): Promise<string> {
	const { host, namespace } = requireTemporalConfig();
	const workflowId = journeyWorkflowId(workspaceId);
	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		await client.workflow.signalWithStart(JOURNEY_WORKFLOW_TYPE, {
			taskQueue: config.cockpitOrchestrationTaskQueue,
			workflowId,
			args: [workspaceId],
			signal,
			signalArgs,
		});
		return workflowId;
	} finally {
		await connection.close();
	}
}

/** Signal `verticalEstablished` (the vertical gate / entry event). */
export function signalVerticalEstablished(
	workspaceId: string,
	vertical: string,
): Promise<string> {
	return signalJourney(workspaceId, VERTICAL_ESTABLISHED_SIGNAL, [
		{ vertical } satisfies VerticalEstablished,
	]);
}

/** Signal `runAddSource` — the journey runs the engine add_source stage as a
 * cross-language child (DAT-551). `select` (kind onboarding) and `replay` (kind
 * replay) both route here. The tool passes the derived ids/queue + sources +
 * conversationId so the journey (no request ALS) records + narrates correctly. */
export function signalRunAddSource(
	workspaceId: string,
	req: RunAddSource,
): Promise<string> {
	return signalJourney(workspaceId, RUN_ADD_SOURCE_SIGNAL, [req]);
}

/** Signal `runBeginSession` — the journey runs the engine begin_session stage as
 * a cross-language child (DAT-530). The tool passes the derived ids/queue + the
 * conversationId so the journey (no request ALS) records + narrates correctly. */
export function signalRunBeginSession(
	workspaceId: string,
	req: RunBeginSession,
): Promise<string> {
	return signalJourney(workspaceId, RUN_BEGIN_SESSION_SIGNAL, [req]);
}

/** Signal `runOperatingModel` — the MANUAL operating_model re-trigger (DAT-530).
 * The autonomous cascade runs operating_model automatically after a clean
 * begin_session; this is the tool path (a teach re-run, P3c) routed through the
 * journey so it stays the single owner of stage execution. */
export function signalRunOperatingModel(
	workspaceId: string,
	req: RunOperatingModel,
): Promise<string> {
	return signalJourney(workspaceId, RUN_OPERATING_MODEL_SIGNAL, [req]);
}

/** Signal `pauseAutoMode` — suspend the autonomous cascade (the breaker's manual
 * counterpart). Pause-don't-kill: a stage in flight finishes; only the next
 * cascade decision is gated. */
export function signalPauseAutoMode(workspaceId: string): Promise<string> {
	return signalJourney(workspaceId, PAUSE_AUTO_MODE_SIGNAL, []);
}

/** Signal `resumeAutoMode` — re-arm the cascade and clear the failure tally. */
export function signalResumeAutoMode(workspaceId: string): Promise<string> {
	return signalJourney(workspaceId, RESUME_AUTO_MODE_SIGNAL, []);
}
