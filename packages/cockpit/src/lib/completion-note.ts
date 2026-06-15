// The model-only nudge that triggers a run-completion narration (Phase 2A.2).
// Pure (no I/O, no cockpit_db/Temporal imports) so the name-leak protection is
// unit-testable in isolation — the watcher (completion-watcher.ts) computes the
// outcome and calls this.

import { randomUUID } from "node:crypto";
import type { UIMessage } from "@tanstack/ai-react";
import type { RunStage } from "#/db/cockpit/runs";
import { stripSrcDigests } from "#/lib/display-names";

/** Friendly name for the finished run — never an internal stage id. */
const STAGE_LABEL: Record<RunStage, string> = {
	add_source: "data import",
	begin_session: "analysis session",
	operating_model: "operating-model run",
};

/** The run's terminal outcome, as the watcher observed it from Temporal. */
export interface RunOutcome {
	failed: boolean;
	/** The engine's root-cause message on failure (sanitized here), else null. */
	failureMessage: string | null;
}

/** Join labels into a readable "a", "a and b", "a, b and c" phrase. Called only
 * with a non-empty list (the caller guards on `stillRunning.length`). */
function joinLabels(labels: string[]): string {
	if (labels.length <= 1) return labels.join("");
	return `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`;
}

/**
 * Build the model-only completion note. role "user" so the transcript ends on a
 * user turn (a no-prefill model requires that) and the converter keeps it;
 * persisted modelOnly so it NEVER shows as a visible bubble — only the agent's
 * reply does.
 *
 * The failure message is `stripSrcDigests`-sanitized — the SAME name-leak
 * protection the retired `workflow_status` projection applied (DAT-433): a
 * content-keyed `src_<digest>` or a staged-upload s3 URI must never reach the
 * model. The agent is told NOT to echo run/workflow ids either.
 *
 * `inFlight` is the set of OTHER stages still running for this workspace at
 * narration time. The note runs against the full transcript, which carries the
 * user's whole-journey intent, so without an explicit boundary the agent
 * narrates one stage ahead — announcing a stage that's only just started, or
 * still running, as finished (DAT-510). We anchor it to THIS run and name the
 * still-running stages as off-limits.
 */
export function completionNote(
	stage: RunStage,
	outcome: RunOutcome,
	inFlight: RunStage[] = [],
): UIMessage {
	const label = STAGE_LABEL[stage];
	const result = outcome.failed
		? `failed${
				outcome.failureMessage
					? ` — ${stripSrcDigests(outcome.failureMessage)}`
					: ""
			}`
		: "finished successfully";
	// Dedup + drop this run's own stage; map to user-facing labels.
	const stillRunning = [...new Set(inFlight)]
		.filter((s) => s !== stage)
		.map((s) => STAGE_LABEL[s]);
	const boundary = stillRunning.length
		? ` Note: the ${joinLabels(stillRunning)} ${
				stillRunning.length > 1 ? "are" : "is"
			} still running — narrate ONLY the ${label}, and do NOT say or imply ${
				stillRunning.length > 1 ? "they have" : "it has"
			} finished.`
		: ` Narrate ONLY the ${label} — do not state or imply any other run finished.`;
	const body =
		`[system event] The ${label} just ${result}.${boundary} ` +
		"Tell the user it's done in one or two sentences and suggest the next step " +
		"in the onboarding journey. Inspect the workspace with your tools if you " +
		"need specifics; don't mention this note or any run/workflow ids.";
	return {
		id: randomUUID(),
		role: "user",
		parts: [{ type: "text", content: body }],
	};
}
