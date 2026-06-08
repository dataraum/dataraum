// OperatingModelProgress widget (DAT-440, DAT-435 follow-on) — live phase
// progress for an operating_model run the `operating_model` tool started.
//
// A display config over the shared WorkflowProgressView core
// (workflow-progress.tsx), mirroring session-progress.tsx. The spine is short
// (resolve → validation → promote → done) so the groups map 1:1 — almost all
// wall-clock sits in `validation` (one LLM SQL generation per declared spec),
// which the caption names. Receives ONLY {state}; run identity is on the state.

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import {
	type WorkflowProgressDisplay,
	WorkflowProgressView,
} from "#/ui/cockpit/widgets/workflow-progress";

const OPERATING_MODEL_GROUPS = [
	{ key: "set-up", label: "Set up", phases: ["operating_model_resolve"] },
	{ key: "validations", label: "Validations", phases: ["validation"] },
	{ key: "finalize", label: "Finalize", phases: ["operating_model_promote"] },
	{ key: "done", label: "Done", phases: ["done"] },
] as const;

const OPERATING_MODEL_CAPTIONS: Record<string, string> = {
	operating_model_resolve: "Reading the session's tables and pinned runs…",
	validation: "Grounding and executing the declared validations…",
	operating_model_promote: "Publishing results…",
};

const OPERATING_MODEL_DISPLAY: WorkflowProgressDisplay = {
	title: "Validation run — progress",
	testId: "operating-model",
	groups: OPERATING_MODEL_GROUPS,
	captions: OPERATING_MODEL_CAPTIONS,
	// No tallyPhase: operating_model has no per-table fan-out.
	failurePrefix: "Validation run",
	startingLabel: "Starting the validation run…",
	doneMessage: () =>
		"Done — validations executed. Use look_validation to see what passed, " +
		"failed, or could not be grounded.",
};

export function OperatingModelProgressWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "operating-model-progress" }>;
}) {
	return (
		<WorkflowProgressView
			display={OPERATING_MODEL_DISPLAY}
			workflowId={state.workflowId}
			runId={state.runId}
		/>
	);
}
