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
import type { LookRelationshipsResult } from "#/tools/look-relationships";
import type { LookTableResult } from "#/tools/look-table";
import type { WhyColumnResult } from "#/tools/why-column";
import type { WhyRelationshipResult } from "#/tools/why-relationship";
import type { WhyTableResult } from "#/tools/why-table";

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
	| { kind: "concept-frame"; frame: FrameResult }
	// DAT-350: per-table readiness traffic-light grid. Carries the look_table
	// tool result (calibrated bands per column × intent, read from the persisted
	// entropy_readiness rows — the cockpit never re-derives the band).
	| { kind: "table-readiness"; readiness: LookTableResult }
	// DAT-351: per-column readiness explanation. Carries the why_column result —
	// per-intent drivers + detector evidence + the synthesized narrative.
	| { kind: "column-why"; why: WhyColumnResult }
	// DAT-434: per-table readiness explanation (the begin_session table-grain
	// analog of column-why). Carries the why_table result.
	| { kind: "table-why"; why: WhyTableResult }
	// DAT-434: per-relationship readiness explanation. Carries the
	// why_relationship result.
	| { kind: "relationship-why"; why: WhyRelationshipResult }
	// DAT-434: the begin_session relationship-readiness list — one row per
	// relationship pair with its band; rows drill down to relationship-why.
	| { kind: "relationship-list"; look: LookRelationshipsResult }
	// DAT-352/DAT-436: live add_source workflow progress. Carries ONLY the
	// (workflowId, runId) of the started run — the widget polls `get_progress`
	// for the snapshot; the run id pins the precise iteration (the id is reused
	// per session under ALLOW_DUPLICATE). Projected from the select/replay TOOL
	// RESULTS by the tool-result mapper (tool-result-to-canvas.ts): approving
	// select STARTS the import, so its result carries the run ids. (The old
	// trigger-button UI action that used to seed this member is retired.)
	| { kind: "add-source-progress"; workflowId: string; runId: string }
	// DAT-435: live begin_session workflow progress — the session analogue of
	// add-source-progress. Same carry (the started run's ids; the widget polls
	// `get_progress`), projected from the begin_session TOOL RESULT.
	| { kind: "session-progress"; workflowId: string; runId: string }
	// DAT-385 P2: the human-facing SQL grid. The P1 stream server is stateless
	// (no queryId→SQL registry), so the grid re-issues the query — it carries the
	// `sql` (+ optional bind `params`) the mapper lifts off the `run_sql` tool
	// CALL input, not a server handle. Columns/types arrive on the stream header.
	| {
			kind: "result-grid";
			sql: string;
			params?: (string | number | boolean | null)[];
	  }
	// A file-upload area (redesign) — projected by the `upload` UI tool so the user
	// can drop local files; NOT a permanent chat fixture. Carries nothing; the
	// widget owns the dropzone + drives connect on upload.
	| { kind: "upload-area" };

/** Every `kind` a canvas member can have — handy for registry/test exhaustion. */
export type CanvasKind = CanvasState["kind"];
