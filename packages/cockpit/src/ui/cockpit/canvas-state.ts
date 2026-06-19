// What the focus canvas is currently showing (DAT-347, C1).
//
// A discriminated union over `kind`. C1 ships ONLY the baseline members below;
// C2-C6 each add one member here, one widget file, one register() line, and one
// tool→canvas mapper case — they never touch the canvas/stream/shell plumbing.
// Keep the union sorted baseline-first so the extension point is obvious.

import type { ConnectSchema } from "#/duckdb/connect";
import type { FrameResult } from "#/tools/frame";
import type { AvailableSource } from "#/tools/list-sources";
import type { InventoryTable } from "#/tools/list-tables";
import type { LookCycleResult } from "#/tools/look-cycle";
import type { LookMetricResult } from "#/tools/look-metric";
import type { LookProfileResult } from "#/tools/look-profile";
import type { LookRelationshipsResult } from "#/tools/look-relationships";
import type { LookTableResult } from "#/tools/look-table";
import type { LookValidationResult } from "#/tools/look-validation";
import type { WhyColumnResult } from "#/tools/why-column";
import type { WhyCycleResult } from "#/tools/why-cycle";
import type { WhyMetricResult } from "#/tools/why-metric";
import type { WhyRelationshipResult } from "#/tools/why-relationship";
import type { WhyTableResult } from "#/tools/why-table";
import type { WhyValidationResult } from "#/tools/why-validation";

export type CanvasState =
	| { kind: "empty" }
	// Optimistic placeholder while a turn runs before a tool result maps to a
	// richer canvas member. `label` says WHAT is in flight (e.g. "Reading the
	// file…", "Explaining the column…") so it reads as progress, not a bare wheel;
	// defaults to "Working…".
	| { kind: "loading"; label?: string }
	| { kind: "error"; message: string }
	| { kind: "source-list"; sources: AvailableSource[] }
	// DAT-349: the workspace table inventory — one row per table with its
	// provenance + a rolled-up readiness band. Carries the (enriched) list_tables
	// result; the widget derives the per-source SourceCard drill-in locally.
	| { kind: "workspace-inventory"; tables: InventoryTable[] }
	| { kind: "schema-preview"; schema: ConnectSchema }
	// DAT-382/DAT-469/DAT-471: the frame-stage co-design surface — the user's
	// framed model (business concepts + the validations AND metric DAGs over them).
	// Carries the frame tool result; the widget renders every family read-only for
	// accept/edit.
	| { kind: "model-frame"; frame: FrameResult }
	// DAT-350: per-table readiness traffic-light grid. Carries the look_table
	// tool result (calibrated bands per column × intent, read from the persisted
	// entropy_readiness rows — the cockpit never re-derives the band).
	| { kind: "table-readiness"; readiness: LookTableResult }
	// DAT-351: per-column readiness explanation. Carries the why_column result —
	// per-intent drivers + detector evidence + the synthesized narrative.
	| { kind: "column-why"; why: WhyColumnResult }
	// DAT-475: per-column descriptive profile — the heavy deep-dive (semantic,
	// statistics, type candidates + decision, quality, temporal, derived). Carries
	// the look_profile result; the widget renders every populated block read-only.
	| { kind: "column-profile"; profile: LookProfileResult }
	// DAT-434: per-table readiness explanation (the begin_session table-grain
	// analog of column-why). Carries the why_table result.
	| { kind: "table-why"; why: WhyTableResult }
	// DAT-434: per-relationship readiness explanation. Carries the
	// why_relationship result.
	| { kind: "relationship-why"; why: WhyRelationshipResult }
	// DAT-434: the begin_session relationship-readiness list — one row per
	// relationship pair with its band; rows drill down to relationship-why.
	| { kind: "relationship-list"; look: LookRelationshipsResult }
	// DAT-440: the operating_model validation list — one row per declared
	// validation with its lifecycle state + executed verdict; rows drill down
	// to validation-why. Carries the look_validation result.
	| { kind: "validation-list"; look: LookValidationResult }
	// DAT-440: per-validation drill-down — state with its blocked reason
	// first-class, the executed result, SQL + grounding detail. Carries the
	// why_validation result.
	| { kind: "validation-why"; why: WhyValidationResult }
	// DAT-465: the operating_model business-cycle list — one row per declared
	// cycle with its lifecycle state + structural completion; rows drill down to
	// cycle-why. Carries the look_cycle result.
	| { kind: "cycle-list"; look: LookCycleResult }
	// DAT-465: per-cycle drill-down — state with its blocked reason first-class,
	// the measured completion, the status column + detected stages/flows/evidence.
	// Carries the why_cycle result.
	| { kind: "cycle-why"; why: WhyCycleResult }
	// DAT-466: the operating_model metric list — one row per declared metric with
	// its lifecycle state + SQL-step count (no value — ephemeral by design); rows
	// drill down to metric-why. Carries the look_metric result.
	| { kind: "metric-list"; look: LookMetricResult }
	// DAT-466: per-metric drill-down — state with its ungroundable reason
	// first-class + the per-step SQL fragments (how it computes). Carries the
	// why_metric result.
	| { kind: "metric-why"; why: WhyMetricResult }
	// DAT-482: the shipped metric DAG a teach OVERRIDE replaces. Carries ONLY the
	// (vertical, graph_id) key — the widget RE-FETCHES the shipped output +
	// dependencies via getShippedMetricDag, so the heavy graph never rides the lean
	// teach tool result into the model's context (the run_sql carry pattern).
	// Projected ONLY for an override (teach_metric result.override) by the mapper.
	| { kind: "metric-shadow"; vertical: string; graphId: string }
	// DAT-352/DAT-436: live add_source workflow progress. Carries ONLY the
	// (workflowId, runId) of the started run — the widget polls `get_progress`
	// for the snapshot; the run id pins the precise iteration (the id is reused
	// per session under ALLOW_DUPLICATE). Projected from the select/replay TOOL
	// RESULTS by the tool-result mapper (tool-result-to-canvas.ts): calling
	// select STARTS the import, so its result carries the run ids. (The old
	// trigger-button UI action that used to seed this member is retired.)
	| { kind: "add-source-progress"; workflowId: string; runId: string }
	// DAT-435: live begin_session workflow progress — the session analogue of
	// add-source-progress. Same carry (the started run's ids; the widget polls
	// `get_progress`), projected from the begin_session TOOL RESULT.
	| { kind: "session-progress"; workflowId: string; runId: string }
	// DAT-440 (DAT-435 follow-on): live operating_model workflow progress —
	// same carry, projected from the operating_model TOOL RESULT.
	| { kind: "operating-model-progress"; workflowId: string; runId: string }
	// DAT-385 P2: the human-facing SQL grid. The P1 stream server is stateless
	// (no queryId→SQL registry), so the grid re-issues the query — it carries the
	// `sql` (+ optional bind `params`) the mapper lifts off the `run_sql` tool
	// CALL input, not a server handle. Columns/types arrive on the stream header.
	| {
			kind: "result-grid";
			sql: string;
			params?: (string | number | boolean | null)[];
	  }
	// DAT-500: the `answer` tool's result — the streaming table PLUS the
	// confidence the answer carries (quality band, grounded ratio, per-concept
	// reuse, assumptions). All of it is already in the AnswerSchema result; the
	// projector surfaces it here instead of dropping it. The grid streams via the
	// same result-grid path (the table is unchanged; confidence rides on top).
	| { kind: "answer-result"; sql: string; confidence: AnswerConfidence }
	// A file-upload area (redesign) — projected by the `upload` UI tool so the user
	// can drop local files; NOT a permanent chat fixture. Carries nothing; the
	// widget owns the dropzone + drives connect on upload.
	| { kind: "upload-area" }
	// DAT-576: the editable probe surface — the user picks a configured DB source,
	// writes/edits read-only SQL, and runs it against the external DB BEFORE ingest
	// (streamed via /api/probe-sql into the same result grid). Projected EMPTY by the
	// `open_probe` UI tool, or SEEDED with the agent's `probe` call input (source +
	// sql) so a generated query lands in the editor for the user to edit + re-run.
	| {
			kind: "probe";
			source?: { name: string; backend: string };
			sql?: string;
	  };

/** Every `kind` a canvas member can have — handy for registry/test exhaustion. */
export type CanvasKind = CanvasState["kind"];

/** The confidence an `answer` result carries, lifted onto the canvas (DAT-500).
 * Every field comes straight from the AnswerSchema result — nothing is recomputed;
 * the projector only renames to the canvas's camelCase idiom. */
export interface AnswerConfidence {
	/** Worst readiness band across the touched tables, or null if none analyzed. */
	band: "ready" | "investigate" | "blocked" | null;
	/** Optional band note from `data_quality`. */
	note?: string;
	/** Share of the answer's SQL grounded in validated snippets (0..1). */
	groundedRatio: number;
	/** The per-concept reuse counts behind the grounded ratio. */
	reuse: { exactReuse: number; adapted: number; fresh: number };
	/** Plain-sentence ambiguity decisions the answer made (may be empty). */
	assumptions: string[];
	/** The business concepts the answer draws on (provenance; may be empty). */
	conceptsUsed: string[];
}
