// run_sql tool (DAT-367) — the agent runs read-only DuckDB SQL over the lake.
//
// Thin LLM-facing wrapper over `duckdb/run-sql`, registered as a TanStack AI
// `toolDefinition().server(...)` so it lands in the agent loop (registry.ts).
// The core query logic + connection lifecycle live in `src/duckdb/` so the same
// lake connection is reusable by non-tool callers (e.g. the future schema-sniff
// for DAT-381). Read-only → no `needsApproval`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { AGENT_SAMPLE_ROWS } from "../duckdb/agent-sample";
import { HARD_ROW_CEILING } from "../duckdb/limit";
import { runSql } from "../duckdb/run-sql";

// Agent-result shape ({columns, rows, rowCount, truncated}). `rows` is
// intentionally permissive — `runSql` returns `Record<string, Json>[]`
// (arbitrary JSON-safe values via the neo driver's getRowObjectsJson), so we
// keep the value type as `z.unknown()` rather than enumerating per-column types
// we can't know ahead of the query.
//
// `truncated` MUST be a real field on this schema (not an extra runtime
// property): the TanStack AI `chat()` loop feeds the tool's VALIDATED `output`
// back into model context, so a property absent from the schema would be
// stripped before the model ever sees the signal (DAT-400).
const QueryResultSchema = z.object({
	columns: z.array(z.string()),
	rows: z.array(z.record(z.string(), z.unknown())),
	rowCount: z.number(),
	truncated: z
		.boolean()
		.describe(
			"True when this in-context sample was trimmed below the full result " +
				"(by the row or serialized-size bound). The COMPLETE result is " +
				"already streaming in the result grid the user sees — do NOT re-run " +
				"with a larger limit to get more rows into chat; instead point the " +
				"user at the grid and/or refine via aggregation (GROUP BY, COUNT, " +
				"summary stats).",
		),
});

export const runSqlTool = toolDefinition({
	name: "run_sql",
	description:
		"Run read-only DuckDB SQL over the data lake and return JSON rows. " +
		"Address tables by their fully-qualified lake name using the " +
		"`physical_name` from list_tables / look_table (NOT the display " +
		"table_name): `lake.typed.<physical_name>` (type-resolved), " +
		"`lake.raw.<physical_name>` (VARCHAR staging), or " +
		"`lake.quarantine.<physical_name>` (failed casts). The rows you " +
		`receive are a BOUNDED in-context SAMPLE (at most ${AGENT_SAMPLE_ROWS} ` +
		"rows, and trimmed further if the serialized result is large) — they are " +
		"for YOUR inspection, not the user's full answer. When `truncated` is " +
		"true the full result is already streaming in the result grid the user " +
		"sees; point them there and/or refine via aggregation rather than asking " +
		"for more raw rows. `limit` bounds the underlying query but does NOT " +
		"raise the in-context sample. Use `params` for any literal value instead " +
		"of concatenating it into the SQL.",
	inputSchema: z.object({
		sql: z.string().describe("DuckDB SQL to run (read-only)."),
		params: z
			.array(z.union([z.string(), z.number(), z.boolean(), z.null()]))
			.optional()
			.describe("Optional positional bind values for $1, $2, … placeholders."),
		limit: z
			.number()
			.max(HARD_ROW_CEILING)
			.optional()
			.describe("Max rows to return (default 1000, capped at 200000)."),
	}),
	outputSchema: QueryResultSchema,
	// ctx.abortSignal deliberately NOT forwarded (DAT-449): duckdb-neo has no
	// per-query cancellation — its only primitive is connection-level
	// `interrupt()`, and the lake connection is process-wide memoized
	// (duckdb/lake.ts), so interrupting would kill CONCURRENT queries (the
	// grid's /api/run-sql, other tool calls), not just this one. Revisit only
	// with a per-query connection or a driver-level signal API.
}).server((input) => runSql(input));
