// What the focus canvas is currently showing (DAT-347, C1).
//
// A discriminated union over `kind`. C1 ships ONLY the baseline members below;
// C2-C6 each add one member here, one widget file, one register() line, and one
// tool→canvas mapper case — they never touch the canvas/stream/shell plumbing.
// Keep the union sorted baseline-first so the extension point is obvious.

import type { ConnectSchema } from "#/duckdb/connect";
import type { FrameResult } from "#/tools/frame";
import type { SourceSummary } from "#/tools/list-sources";
import type { TableSummary } from "#/tools/list-tables";
import type { SelectResult } from "#/tools/select";

export type CanvasState =
	| { kind: "empty" }
	| { kind: "loading" }
	| { kind: "error"; message: string }
	| { kind: "source-list"; sources: SourceSummary[] }
	| { kind: "table-list"; tables: TableSummary[] }
	| { kind: "schema-preview"; schema: ConnectSchema }
	| { kind: "concept-frame"; frame: FrameResult }
	| { kind: "selected-source"; selection: SelectResult }
	// DAT-385 P2: the human-facing SQL grid. The P1 stream server is stateless
	// (no queryId→SQL registry), so the grid re-issues the query — it carries the
	// `sql` (+ optional bind `params`) the mapper lifts off the `run_sql` tool
	// CALL input, not a server handle. Columns/types arrive on the stream header.
	| {
			kind: "result-grid";
			sql: string;
			params?: (string | number | boolean | null)[];
	  };

/** Every `kind` a canvas member can have — handy for registry/test exhaustion. */
export type CanvasKind = CanvasState["kind"];
