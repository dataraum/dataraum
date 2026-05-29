// probe tool (DAT-367) — the agent runs read-only SQL against an external
// database source BEFORE it is materialized into the lake.
//
// Thin LLM-facing wrapper over `duckdb/probe`. Credentials are resolved by
// source name from `DATARAUM_<NAME>_URL` and never surfaced to the agent.

import type { Tool } from "@anthropic-ai/sdk/resources/messages";

import { type ProbeInput, probe, SUPPORTED_BACKENDS } from "../duckdb/probe";
import type { QueryResult } from "../duckdb/query-result";

export const definition: Tool = {
	name: "probe",
	description:
		"Run read-only SQL against an external database source (not yet in the " +
		"lake) via a READ_ONLY DuckDB ATTACH — for schema sniffing and sample " +
		"reads before ingest. Credentials are resolved by source name from the " +
		"environment and are never exposed. The result is capped (default 1000 " +
		`rows). Supported backends: ${SUPPORTED_BACKENDS.join(", ")}.`,
	input_schema: {
		type: "object",
		properties: {
			source_name: {
				type: "string",
				description:
					"Configured source name; the key for the DATARAUM_<NAME>_URL credential.",
			},
			backend: {
				type: "string",
				enum: SUPPORTED_BACKENDS,
				description: "Database backend kind.",
			},
			sql: {
				type: "string",
				description: "Read-only SQL to run against the attached source.",
			},
			limit: {
				type: "integer",
				description: "Max rows to return (default 1000).",
			},
		},
		required: ["source_name", "backend", "sql"],
	},
};

export async function handler(input: ProbeInput): Promise<QueryResult> {
	return probe(input);
}
