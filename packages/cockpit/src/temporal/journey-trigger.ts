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
 * Signal `verticalEstablished` to the workspace's journey, starting it if it
 * isn't running yet. Returns the journey's workflow id.
 */
export async function signalVerticalEstablished(
	workspaceId: string,
	vertical: string,
): Promise<string> {
	const { host, namespace } = requireTemporalConfig();
	const workflowId = journeyWorkflowId(workspaceId);
	const signalArgs: [VerticalEstablished] = [{ vertical }];

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		await client.workflow.signalWithStart(JOURNEY_WORKFLOW_TYPE, {
			taskQueue: config.cockpitOrchestrationTaskQueue,
			workflowId,
			args: [workspaceId],
			signal: VERTICAL_ESTABLISHED_SIGNAL,
			signalArgs,
		});
		return workflowId;
	} finally {
		await connection.close();
	}
}
