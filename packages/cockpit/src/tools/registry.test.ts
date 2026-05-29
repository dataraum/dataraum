// Tool-registry contract tests (DAT-353). Pure — asserts the LLM-facing surface
// of the registry (names + approval gating) without a DB. The DB query paths
// (list_sources / list_tables) are covered by gated integration tests;
// teach/replay write paths by the integration smoke.
//
// Importing the registry transitively boots config.ts (via metadataDb), which
// throws unless these are set. Placeholders only: the contract assertions never
// query, and the postgres client is lazy (no connect on import). The registry
// is dynamic-imported in beforeAll so this env block runs first (static imports
// hoist above it). vitest isolates env per file, so setting METADATA_DATABASE_URL
// here does NOT unskip the gated integration tests in other files.

import { beforeAll, describe, expect, it } from "vitest";

const ENV_DEFAULTS: Record<string, string> = {
	COCKPIT_DATABASE_URL: "postgresql://u:p@127.0.0.1:5432/cockpit_db",
	METADATA_DATABASE_URL: "postgresql://u:p@127.0.0.1:5432/ws_test",
	DATARAUM_WORKSPACE_ID: "test",
	DATARAUM_LAKE_PATH: "/tmp/lake",
	ANTHROPIC_API_KEY: "sk-ant-test-placeholder",
};
for (const [k, v] of Object.entries(ENV_DEFAULTS)) {
	if (!process.env[k]) process.env[k] = v;
}

let tools: ReadonlyArray<{ name: string; needsApproval?: boolean }>;
beforeAll(async () => {
	({ tools } = await import("./registry"));
});

describe("tool registry (DAT-353)", () => {
	it("registers the slice-1 toolset with unique names", () => {
		const names = tools.map((t) => t.name);
		expect(names).toHaveLength(new Set(names).size); // no dupes
		expect(new Set(names)).toEqual(
			new Set(["list_sources", "list_tables", "teach", "replay"]),
		);
	});

	it("gates the write/compute tools behind approval, leaves reads open", () => {
		const byName = new Map(tools.map((t) => [t.name, t]));
		expect(byName.get("teach")?.needsApproval).toBe(true);
		expect(byName.get("replay")?.needsApproval).toBe(true);
		// Reads must NOT require approval — they run unattended in the loop.
		expect(byName.get("list_sources")?.needsApproval ?? false).toBe(false);
		expect(byName.get("list_tables")?.needsApproval ?? false).toBe(false);
	});
});
