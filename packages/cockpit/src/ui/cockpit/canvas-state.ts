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

export type CanvasState =
	| { kind: "empty" }
	| { kind: "loading" }
	| { kind: "error"; message: string }
	| { kind: "source-list"; sources: SourceSummary[] }
	| { kind: "table-list"; tables: TableSummary[] }
	| { kind: "schema-preview"; schema: ConnectSchema }
	| { kind: "concept-frame"; frame: FrameResult };

/** Every `kind` a canvas member can have — handy for registry/test exhaustion. */
export type CanvasKind = CanvasState["kind"];
