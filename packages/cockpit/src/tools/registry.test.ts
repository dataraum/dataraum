// Tool-registry contract tests (DAT-353). Pure — asserts the LLM-facing surface
// of the registry (names + approval gating). The DB query paths (list_sources /
// list_tables) are covered by gated integration tests; teach/replay write paths
// by the integration smoke.
//
// Importing the registry transitively pulls config.ts + the Postgres metadata
// client (via the tools). We MOCK both so the test needs no real env and opens
// no connection — and, critically, sets NO process.env, which would leak across
// files in a reused worker and un-skip the gated integration tests (that bug
// hung the whole suite while it tried to connect to a Postgres that isn't up).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { tools } from "./registry";

describe("tool registry (DAT-353)", () => {
	it("registers the toolset with unique names", () => {
		const names = tools.map((t) => t.name);
		expect(names).toHaveLength(new Set(names).size); // no dupes
		expect(new Set(names)).toEqual(
			new Set([
				"list_sources",
				"list_tables",
				"look_table",
				"why_column",
				"run_sql",
				"probe",
				"connect",
				"frame",
				"select",
				"teach",
				"replay",
			]),
		);
	});

	it("gates the write/compute tools behind approval, leaves reads open", () => {
		const byName = new Map(tools.map((t) => [t.name, t]));
		expect(byName.get("frame")?.needsApproval).toBe(true);
		expect(byName.get("select")?.needsApproval).toBe(true);
		expect(byName.get("teach")?.needsApproval).toBe(true);
		expect(byName.get("replay")?.needsApproval).toBe(true);
		// Reads must NOT require approval — they run unattended in the loop.
		expect(byName.get("list_sources")?.needsApproval ?? false).toBe(false);
		expect(byName.get("list_tables")?.needsApproval ?? false).toBe(false);
		expect(byName.get("look_table")?.needsApproval ?? false).toBe(false);
		expect(byName.get("why_column")?.needsApproval ?? false).toBe(false);
		expect(byName.get("run_sql")?.needsApproval ?? false).toBe(false);
		expect(byName.get("probe")?.needsApproval ?? false).toBe(false);
		expect(byName.get("connect")?.needsApproval ?? false).toBe(false);
	});
});
