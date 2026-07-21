// What the focus canvas is currently showing (DAT-347).
//
// A discriminated union over `kind`, one member per canvas surface, each tagged
// with the ticket that added it. Adding a surface is FOUR additive edits and
// nothing else: a member here, a widget file, one register() line in
// canvas-registry.ts, and one tool→canvas mapper case — the canvas, stream, and
// shell plumbing stay untouched. Keep the union sorted with the three baseline
// members (empty / loading / error) first so that extension point stays obvious.

import type { ConversationKind } from "#/db/cockpit/conversations";
import type { WorkspaceBriefing } from "#/db/metadata/briefing/types";
import type { AvailableSource } from "#/tools/list-sources";
import type { InventoryTable } from "#/tools/list-tables";
import type { LookCycleResult } from "#/tools/look-cycle";
import type { LookDriversResult } from "#/tools/look-drivers";
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
	// DAT-546/DAT-579: the begin_session driver rankings — one row per ranked
	// measure with its target type, grain, effective sample, and the dimensions
	// that best explain its variation. Read-only (no per-row drill tool). Carries
	// the look_drivers result.
	| { kind: "driver-list"; look: LookDriversResult }
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
	// DAT-385: the human-facing SQL grid. The `/api/run-sql` stream server is
	// stateless (no queryId→SQL registry), so the grid re-issues the query — it carries the
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
	// `summary` is the answer narrative (the AnswerSchema `answer` field) — carried
	// so the Report mint (DAT-624) can freeze it alongside the SQL + confidence.
	// Discriminated on `sql`: a grounded result carries the grid SQL + confidence; a
	// NO-RESULT answer (the sub-agent couldn't compose a runnable query — a legitimate
	// outcome) carries `sql: null` + `confidence: null`, and the widget shows an
	// explicit "no result" state (with the narrative) rather than a stale/blank canvas.
	| {
			kind: "answer-result";
			sql: string;
			summary: string;
			confidence: AnswerConfidence;
	  }
	| {
			kind: "answer-result";
			sql: null;
			summary: string;
			confidence: null;
	  }
	// DAT-576/DAT-597: the editable probe surface — the staging hub default. The user
	// picks a configured DB source, writes/edits read-only SQL, and runs it against
	// the external DB BEFORE ingest (streamed via /api/probe-sql into the same result
	// grid), then assembles the import set. Carries an optional seeded (source + sql).
	| {
			kind: "probe";
			source?: { name: string; backend: string };
			sql?: string;
	  }
	// DAT-634: the chat-open "Workspace Briefing" — the LANDING orientation for a
	// fresh stage/analyse chat (the idle fallback, replacing blank `empty`). Carries
	// the full briefing + this chat's kind; the widget projects client-side
	// (`projectBriefing`) to foreground this chat's actions. It yields to any live
	// tool canvas on the first turn; the durable, always-fresh view is the
	// Governance route (DAT-633), so this is never re-fetched or kept live here.
	| {
			kind: "briefing";
			briefing: WorkspaceBriefing;
			chatKind: ConversationKind;
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
