// run_sql tool (DAT-367) — the agent runs read-only DuckDB SQL over the lake.
//
// Thin LLM-facing wrapper over `duckdb/run-sql`, registered as a TanStack AI
// `toolDefinition().server(...)` so it lands in the agent loop (registry.ts).
// The core query logic + connection lifecycle live in `src/duckdb/` so the same
// lake connection is reusable by non-tool callers (e.g. the future schema-sniff
// for DAT-381). Read-only → no `needsApproval`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { HARD_ROW_CEILING } from "../duckdb/limit";
import { runSql } from "../duckdb/run-sql";

// QueryResult shape ({columns, rows, rowCount}). `rows` is intentionally
// permissive — `runSql` returns `Record<string, Json>[]` (arbitrary JSON-safe
// values via the neo driver's getRowObjectsJson), so we keep the value type as
// `z.unknown()` rather than enumerating per-column types we can't know ahead of
// the query.
const QueryResultSchema = z.object({
	columns: z.array(z.string()),
	rows: z.array(z.record(z.string(), z.unknown())),
	rowCount: z.number(),
});

export const runSqlTool = toolDefinition({
	name: "run_sql",
	description:
		"Run read-only DuckDB SQL over the data lake and return JSON rows. " +
		"Address tables by their fully-qualified lake name, e.g. " +
		"`lake.typed.<table>` (type-resolved), `lake.raw.<table>` (VARCHAR " +
		"staging), or `lake.quarantine.<table>` (failed casts). The result is " +
		"capped (default 1000 rows, hard ceiling 200000); pass `limit` to change " +
		"it within that ceiling. Use `params` for any literal value instead of " +
		"concatenating it into the SQL.",
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
}).server((input) => runSql(input));
