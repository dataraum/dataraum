// run_sql tool schema (DAT-400). No DB — this pins the AGENT-FACING contract:
// `truncated` MUST be a validated field on the tool's outputSchema, because the
// TanStack AI chat() loop validates the tool `output` against that schema
// before feeding it back into model context. A `truncated` that lived only as a
// runtime property (absent from the schema) would be stripped and the model
// would never learn the sample was bounded.
//
// Importing the tool transitively pulls config.ts + the Postgres metadata
// client (via the duckdb modules' import graph), so mock both — see
// registry.test.ts / look-table.test.ts.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { runSqlTool } from "./run_sql";

describe("run_sql tool outputSchema (DAT-400)", () => {
	it("declares `truncated` as a required boolean on the validated output", () => {
		const outputSchema = runSqlTool.outputSchema;
		// The tool must declare an output schema at all — chat() only validates
		// (and forwards) `output` when one is present.
		expect(outputSchema).toBeDefined();
		if (!outputSchema) return;

		// A result WITHOUT `truncated` must fail validation — proving the field is
		// a real part of the schema (so the SDK feeds it to the model), not an
		// extra property that would be silently dropped.
		const missing = outputSchema.safeParse({
			columns: ["n"],
			rows: [{ n: 1 }],
			rowCount: 1,
		});
		expect(missing.success).toBe(false);

		// With `truncated` it validates, and the value round-trips.
		const ok = outputSchema.safeParse({
			columns: ["n"],
			rows: [{ n: 1 }],
			rowCount: 1,
			truncated: true,
		});
		expect(ok.success).toBe(true);
		if (ok.success) {
			// outputSchema is now `QueryResult | { error }` (pass 2) — assert via a
			// matcher rather than dot-access so the error branch doesn't break tsc.
			expect(ok.data).toHaveProperty("truncated", true);
		}
	});
});
