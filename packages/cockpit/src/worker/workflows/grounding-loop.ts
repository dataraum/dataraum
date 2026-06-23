// groundingLoopWorkflow (DAT-609) — the onboarding import + autonomous teach loop.
//
// SANDBOXED: imports ONLY `@temporalio/workflow`, the pure `./run-stage` +
// `./grounding-step` helpers, and *types*. Replaces the singleton journey's
// add_source arm + `runGroundingLoop`, extracted into a short-lived per-trigger
// workflow (id `grounding-<ws>`): NO `patched()`, NO continue-as-new, NO breaker,
// NO cross-run state — all state rides the start payload.
//
// Started by the `select` import trigger only (a manual `replay` is a DIRECT engine
// start — the user is doing teach+replay by hand, so it must NOT re-enter this
// autonomous loop). On a clean import it auto-applies the mechanical grounding teaches
// a detector can verify and replays (re-runs add_source to re-measure), bounded by the
// attempt budget; a human-judgement gap or exhaustion parks the run `awaiting_input`
// (the "Needs you" inbox) and returns — it NEVER blocks on human input.
//
// It does NOT narrate: the server-side completion-watcher narrates on the run's
// done edge (and skips the onboarding import per DAT-597). This workflow only runs
// the stages + records the runs.

import { log, proxyActivities } from "@temporalio/workflow";
import type { AddSourceResult } from "../../temporal/types";
import type * as activities from "../activities";
import type { GroundingLoopInput } from "../contracts";
import { decideGroundingStep } from "./grounding-step";
import { runStage } from "./run-stage";

// The grounding-teach agent (DAT-551) — an LLM tool-loop, so a much longer timeout
// than the cockpit_db writes, and only one retry (a re-run is expensive + the loop
// tolerates a failed round by stopping, never crashing).
const { assessAndGround } = proxyActivities<typeof activities>({
	startToCloseTimeout: "10 minutes",
	retry: { maximumAttempts: 2 },
});

// The park writer (its own proxy — quick local write).
const { markRunAwaitingInput } = proxyActivities<typeof activities>({
	startToCloseTimeout: "1 minute",
	retry: { maximumAttempts: 3 },
});

// Default replay budget when the trigger carries none.
const DEFAULT_GROUNDING_ATTEMPTS = 3;

/** The typed table ids from an add_source result — the readiness scope the grounding
 * agent assesses. Narrows the child's `unknown` result defensively. */
function typedTableIds(result: unknown): string[] {
	const r = result as Partial<AddSourceResult> | null;
	if (!r?.tables) return [];
	return r.tables
		.map((t) => t.typed_table_id)
		.filter((id): id is string => typeof id === "string");
}

/** The engine add_source input for this workspace's source set. */
function addSourceArgs(input: GroundingLoopInput): unknown[] {
	return [
		{
			workspace_id: input.workspaceId,
			sources: input.sources,
			verticals: input.verticals,
		},
	];
}

/**
 * Run the onboarding import, then the bounded grounding-teach loop. Short-lived: one
 * execution per import trigger, all state on the payload + re-read from the engine
 * result each round.
 */
export async function groundingLoopWorkflow(
	input: GroundingLoopInput,
): Promise<void> {
	// 1) The onboarding import. Recorded with the real conversationId so the watcher
	//    tracks its progress widget, but `kind:"onboarding"` ⇒ the watcher skips its
	//    chat narration (DAT-597).
	const first = await runStage({
		workspaceId: input.workspaceId,
		workflowType: "addSourceWorkflow",
		workflowId: input.workflowId,
		taskQueue: input.engineTaskQueue,
		stage: "add_source",
		kind: "onboarding",
		conversationId: input.conversationId,
		args: addSourceArgs(input),
	});
	if (!first.ok) return; // import failed (already marked) — nothing to ground.

	// 2) The grounding-teach loop (the autonomy step). Bounded by `numberOfAttempts`.
	let tableIds = typedTableIds(first.result);
	if (tableIds.length === 0) return;
	let attemptsRemaining = input.numberOfAttempts ?? DEFAULT_GROUNDING_ATTEMPTS;

	while (true) {
		let verdict: activities.AssessAndGroundResult;
		try {
			verdict = await assessAndGround({ tableIds, attemptsRemaining });
		} catch (err) {
			// The assessment died (LLM error after retries) — stop grounding, don't
			// crash. The import itself is already recorded complete.
			log.warn("grounding assess failed", {
				workflowId: input.workflowId,
				err: String(err),
			});
			return;
		}

		const step = decideGroundingStep(verdict, attemptsRemaining);
		if (step.action === "done") return;
		if (step.action === "surface") {
			await markRunAwaitingInput(input.workflowId, step.note).catch(() => {});
			log.info("grounding surfaced for input", {
				workflowId: input.workflowId,
				reason: step.reason,
			});
			return;
		}

		// action === "replay": re-run add_source for the SAME workspace to apply the
		// teaches + re-measure. conversationId=null: these are INTERNAL autonomous
		// re-runs — they must NOT fire the watcher's narration (the user already heard
		// the import landed; the loop's outcome surfaces via the run monitor /
		// awaiting_input, not N chat messages).
		attemptsRemaining -= 1;
		const { ok, result } = await runStage({
			workspaceId: input.workspaceId,
			workflowType: "addSourceWorkflow",
			workflowId: input.workflowId,
			taskQueue: input.engineTaskQueue,
			stage: "add_source",
			kind: "onboarding",
			conversationId: null,
			args: addSourceArgs(input),
		});
		if (!ok) return; // a failed replay stops the loop (the run is marked).
		tableIds = typedTableIds(result);
		if (tableIds.length === 0) return;
	}
}
