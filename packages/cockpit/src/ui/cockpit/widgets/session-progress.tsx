// SessionProgress widget (DAT-435) — live phase progress for a begin_session
// run the `begin_session` tool started.
//
// A display config over the shared WorkflowProgressView core
// (workflow-progress.tsx). begin_session is sequential (13 raw engine phases,
// no fan-out — the snapshot's table fields stay empty), so the pipeline GROUPS
// the raw phases into six user-meaningful badges: not all phases are equally
// informative, and 14 badges read as noise where 6 read as a journey. The live
// caption below the badges still names the precise running stage, and a
// failure names the group plus the precise stage ("Slice analysis (profiling
// each slice)") — grouping de-emphasizes, it never hides. Receives ONLY
// {state}; the run identity is on the state.

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import {
	type WorkflowProgressDisplay,
	WorkflowProgressView,
} from "#/ui/cockpit/widgets/workflow-progress";

// The six display groups over the engine's 13-phase session chain
// (workflows.py: select → relationships/semantic/overlays → enriched_views →
// the DAT-403 value layer → detect/keepers/promote → done). Cut lines: the
// user's question per badge — "checking my selection", "how do my tables
// connect?", "building the views I'll query", "deep in analysis", "wrapping
// up". A phase the engine reports that isn't covered (forward-compat) renders
// no highlight rather than crashing.
const SESSION_GROUPS = [
	{ key: "set-up", label: "Set up", phases: ["begin_session_select"] },
	{
		key: "relationships",
		label: "Relationships",
		// The LLM confirm + the teach-overlay fold are mechanics of answering
		// "how do my tables connect?" — one badge.
		phases: [
			"relationships",
			"semantic_per_table",
			"session_materialize_overlays",
		],
	},
	{
		key: "enriched-views",
		label: "Enriched views",
		phases: ["enriched_views"],
	},
	{
		key: "slice-analysis",
		label: "Slice analysis",
		// The whole DAT-403 value layer: one arc (find slices → build → profile →
		// trend → correlate); the captions carry the differentiation.
		phases: [
			"slicing",
			"slicing_view",
			"slice_analysis",
			"temporal_slice_analysis",
			"correlations",
		],
	},
	{
		key: "finalize",
		label: "Finalize",
		// Readiness scoring + run-versioning bookkeeping a user never parses.
		phases: [
			"session_detect",
			"session_write_keepers",
			"session_promote_to_latest",
		],
	},
	{ key: "done", label: "Done", phases: ["done"] },
] as const;

// Live caption per raw engine phase — the precise stage behind the badge. No
// tally phase exists (sequential workflow), so every running phase captions.
const SESSION_CAPTIONS: Record<string, string> = {
	begin_session_select: "Checking the selected tables…",
	relationships: "Detecting relationship candidates…",
	semantic_per_table: "Classifying tables and confirming relationships…",
	session_materialize_overlays: "Applying your saved teachings…",
	enriched_views: "Building combined views across related tables…",
	slicing: "Identifying meaningful data slices…",
	slicing_view: "Building slice views…",
	slice_analysis: "Profiling each slice…",
	temporal_slice_analysis: "Analyzing trends over time…",
	correlations: "Finding correlated columns…",
	session_detect: "Scoring relationship readiness…",
	session_write_keepers: "Saving accepted relationships…",
	session_promote_to_latest: "Publishing results…",
};

const SESSION_DISPLAY: WorkflowProgressDisplay = {
	title: "Session analysis — progress",
	testId: "session",
	groups: SESSION_GROUPS,
	captions: SESSION_CAPTIONS,
	// No tallyPhase: begin_session has no per-table fan-out.
	failurePrefix: "Session analysis",
	startingLabel: "Starting the session…",
	doneMessage: () =>
		"Done — the session is ready. Ask about the tables' relationships or readiness.",
};

export function SessionProgressWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "session-progress" }>;
}) {
	return (
		<WorkflowProgressView
			display={SESSION_DISPLAY}
			workflowId={state.workflowId}
			runId={state.runId}
		/>
	);
}
