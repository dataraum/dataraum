// run_sql tool (DAT-367) — the agent runs read-only DuckDB SQL over the lake.
//
// Thin LLM-facing wrapper over `duckdb/run-sql`. The core query logic +
// connection lifecycle live in `src/duckdb/` so the same lake connection is
// reusable by non-tool callers (e.g. the future schema-sniff for DAT-381).

import type { Tool } from "@anthropic-ai/sdk/resources/messages";

import type { QueryResult } from "../duckdb/query-result";
import { type RunSqlInput, runSql } from "../duckdb/run-sql";

export const definition: Tool = {
	name: "run_sql",
	description:
		"Run read-only DuckDB SQL over the data lake and return JSON rows. " +
		"Address tables by their fully-qualified lake name, e.g. " +
		"`lake.typed.<table>` (type-resolved), `lake.raw.<table>` (VARCHAR " +
		"staging), or `lake.quarantine.<table>` (failed casts). The result is " +
		"capped (default 1000 rows); pass `limit` to change it. Use `params` " +
		"for any literal value instead of concatenating it into the SQL.",
	input_schema: {
		type: "object",
		properties: {
			sql: {
				type: "string",
				description: "DuckDB SQL to run (read-only).",
			},
			params: {
				type: "array",
				description:
					"Optional positional bind values for $1, $2, … placeholders.",
				items: {
					type: ["string", "number", "boolean", "null"],
				},
			},
			limit: {
				type: "integer",
				description: "Max rows to return (default 1000).",
			},
		},
		required: ["sql"],
	},
};

export async function handler(input: RunSqlInput): Promise<QueryResult> {
	return runSql(input);
}
