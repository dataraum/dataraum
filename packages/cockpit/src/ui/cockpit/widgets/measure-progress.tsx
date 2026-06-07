// MeasureProgress widget (DAT-352) — live per-phase progress for an add_source
// (or replay) run the TRIGGER started.
//
// A display config over the shared WorkflowProgressView core
// (workflow-progress.tsx), which owns the polling (`/api/workflow-progress` on
// a TanStack Query `refetchInterval`) and the rendering. add_source's pipeline
// is ungrouped — every raw engine phase (workflows.py advance sequence) is its
// own badge — and it is the one workflow with a per-table fan-out, so it
// supplies the tally config. Receives ONLY {state} (canvas widgets have no
// sendMessage) — the run identity is on the state.

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import {
	type WorkflowProgressDisplay,
	WorkflowProgressView,
} from "#/ui/cockpit/widgets/workflow-progress";

// The phase pipeline in order, with friendly labels. Mirrors the engine's
// advance sequence (workflows.py): import → check_column_limit →
// processing_tables → semantic_per_column → detect → promote → done. A `phase`
// the engine reports that isn't here (forward-compat) renders no highlight
// rather than crashing.
const PHASES = [
	{ key: "import", label: "Import" },
	{ key: "check_column_limit", label: "Check size" },
	{ key: "processing_tables", label: "Type tables" },
	{ key: "semantic_per_column", label: "Semantic" },
	{ key: "detect", label: "Detect" },
	{ key: "promote", label: "Promote" },
	{ key: "done", label: "Done" },
] as const;

// Live caption for the phases that carry NO per-table signal: `import` /
// `check_column_limit` (before the fan-out exists) and `semantic_per_column` /
// `detect` / `promote` (each ONE run-level activity, workflows.py). For these the
// table tally is frozen/empty, so the caption keeps the surface alive ("still
// working, here's on what") instead of dead air. `processing_tables` is
// intentionally absent — it has its own tally bar.
const PHASE_CAPTION: Record<string, string> = {
	import: "Importing rows…",
	check_column_limit: "Checking the run's column count against the limit…",
	semantic_per_column: "Analyzing column semantics across all tables…",
	detect: "Scoring readiness across all columns…",
	promote: "Promoting this run's results…",
};

const ADD_SOURCE_DISPLAY: WorkflowProgressDisplay = {
	title: "Add source — progress",
	testId: "measure",
	groups: PHASES.map((p) => ({ key: p.key, label: p.label, phases: [p.key] })),
	captions: PHASE_CAPTION,
	tallyPhase: "processing_tables",
	tallyLabel: "Typing tables",
	failurePrefix: "Add source",
	startingLabel: "Starting add source…",
	doneMessage: (data) =>
		data.tables.length > 0
			? `Done — ${data.tables.length} table${
					data.tables.length === 1 ? "" : "s"
				} imported and analyzed. Ask about any table to see its readiness.`
			: "Done — the source is imported and analyzed.",
};

export function MeasureProgressWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "add-source-progress" }>;
}) {
	return (
		<WorkflowProgressView
			display={ADD_SOURCE_DISPLAY}
			workflowId={state.workflowId}
			runId={state.runId}
		/>
	);
}
