// probe tool (DAT-367) — the agent runs read-only SQL against an external
// database source BEFORE it is materialized into the lake.
//
// Thin LLM-facing wrapper over `duckdb/probe`, registered as a TanStack AI
// `toolDefinition().server(...)` so it lands in the agent loop (registry.ts).
// Credentials are resolved by source name from `DATARAUM_<NAME>_URL` and never
// surfaced to the agent. Read-only → no `needsApproval`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { HARD_ROW_CEILING } from "../duckdb/limit";
import { probe, SUPPORTED_BACKENDS } from "../duckdb/probe";

// QueryResult shape ({columns, rows, rowCount}). `rows` is intentionally
// permissive — `probe` returns `Record<string, Json>[]` (arbitrary JSON-safe
// values), so we keep the value type as `z.unknown()` rather than enumerating
// per-column types we can't know ahead of the query.
const QueryResultSchema = z.object({
	columns: z.array(z.string()),
	rows: z.array(z.record(z.string(), z.unknown())),
	rowCount: z.number(),
});

export const probeTool = toolDefinition({
	name: "probe",
	description:
		"Run read-only SQL against an external database source (not yet in the " +
		"lake) via a READ_ONLY DuckDB ATTACH — for schema sniffing and sample " +
		"reads before ingest. Credentials are resolved by source name from the " +
		"environment and are never exposed. The result is capped (default 1000 " +
		"rows, hard ceiling 200000). Supported backends: " +
		`${SUPPORTED_BACKENDS.join(", ")}.`,
	inputSchema: z.object({
		source_name: z
			.string()
			.describe(
				"Configured source name; the key for the DATARAUM_<NAME>_URL credential.",
			),
		backend: z
			.enum(SUPPORTED_BACKENDS as [string, ...string[]])
			.describe("Database backend kind."),
		sql: z
			.string()
			.describe("Read-only SQL to run against the attached source."),
		limit: z
			.number()
			.max(HARD_ROW_CEILING)
			.optional()
			.describe("Max rows to return (default 1000, capped at 200000)."),
	}),
	outputSchema: QueryResultSchema,
}).server((input) => probe(input));
