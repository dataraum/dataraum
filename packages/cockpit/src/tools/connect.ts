// connect tool (DAT-381) — the agent peeks a source's schema + sample values
// BEFORE any data is imported or moved.
//
// Thin LLM-facing wrapper over `duckdb/connect`, registered as a TanStack AI
// `toolDefinition().server(...)` so it lands in the agent loop (registry.ts).
// Covers a configured database source (by name, via the READ_ONLY probe ATTACH)
// and a server-readable file path (via DuckDB's file readers). Read-only — no
// ATTACH writes, no ingest — so no `needsApproval`.
//
// The cross-field requirement (database needs source_name + backend; file needs
// path) is enforced inside `connect()` rather than in this flat input schema, so
// the agent sees a single simple object and a clear error message if it omits a
// required field.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { ConnectSchema, connect } from "../duckdb/connect";
import { SUPPORTED_BACKENDS } from "../duckdb/probe";

export const connectTool = toolDefinition({
	name: "connect",
	description:
		"Peek a data source's schema and sample values BEFORE importing it — no " +
		"data is moved. Use it to show the user what a source looks like. Two " +
		"kinds: a configured database source (set source_kind='database' with " +
		"source_name + backend; introspected via a READ_ONLY attach) or a " +
		"server-readable file path (set source_kind='file' with path; CSV/TSV, " +
		"Parquet, or JSON sniffed in place). Returns tables with columns " +
		"(name, type, nullability) and a capped handful of sample values. " +
		`Supported database backends: ${SUPPORTED_BACKENDS.join(", ")}.`,
	inputSchema: z.object({
		source_kind: z
			.enum(["database", "file"])
			.describe("Which kind of source to peek."),
		source_name: z
			.string()
			.optional()
			.describe(
				"Configured database source name (required when source_kind=database); " +
					"the key for the DATARAUM_<NAME>_URL credential.",
			),
		backend: z
			.enum(SUPPORTED_BACKENDS as [string, ...string[]])
			.optional()
			.describe("Database backend (required when source_kind=database)."),
		path: z
			.string()
			.optional()
			.describe(
				"Server-readable file path (required when source_kind=file): a " +
					".csv/.tsv/.txt, .parquet, or .json/.ndjson/.jsonl file.",
			),
	}),
	outputSchema: ConnectSchema,
}).server((input) => connect(input));
