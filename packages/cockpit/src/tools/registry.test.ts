// Tool-registry contract tests (DAT-353). Pure — asserts the LLM-facing surface
// of the registry (names + that no tool declares an approval gate). The DB query
// paths (list_sources / list_tables) are covered by gated integration tests;
// teach/replay write paths by the integration smoke.
//
// Importing the registry transitively pulls config.ts + the Postgres metadata
// client (via the tools). We MOCK both so the test needs no real env and opens
// no connection — and, critically, sets NO process.env, which would leak across
// files in a reused worker and un-skip the gated integration tests (that bug
// hung the whole suite while it tried to connect to a Postgres that isn't up).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
// The driver tools import the cockpit control plane (DAT-461/506); mock the seams
// so the registry import never loads the live cockpit_db (bun:sql) client.
vi.mock("#/db/cockpit/client", () => ({ cockpitDb: {} }));
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspace: async () => "ws-test",
	resolveActiveWorkspaceRow: async () => ({
		id: "ws-test",
		taskQueue: "engine-ws-test",
		vertical: "_adhoc",
	}),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: async () => {},
	attachRunId: async () => {},
	hasRunningRun: async () => false,
}));

import { tools } from "./registry";

describe("tool registry (DAT-353)", () => {
	it("registers the toolset with unique names", () => {
		const names = tools.map((t) => t.name);
		expect(names).toHaveLength(new Set(names).size); // no dupes
		expect(new Set(names)).toEqual(
			new Set([
				"list_sources",
				"list_tables",
				"list_verticals",
				"look_table",
				"look_profile",
				"why_column",
				"why_table",
				"look_relationships",
				"why_relationship",
				"run_sql",
				"answer",
				"probe",
				"connect",
				"frame",
				"select",
				"teach",
				"teach_validation",
				"teach_cycle",
				"teach_metric",
				"begin_session",
				"operating_model",
				"look_validation",
				"why_validation",
				"look_cycle",
				"why_cycle",
				"look_metric",
				"why_metric",
				"replay",
				"upload",
			]),
		);
	});

	it("declares NO approval gate on any tool — every tool runs directly", () => {
		// The approval gate is gone: every tool (reads AND write/compute alike) runs
		// on the user's natural-language instruction, so none may declare approval.
		// A regression guard — a re-introduced `needsApproval: true` on any tool
		// fails here.
		for (const tool of tools) {
			expect(
				tool.needsApproval ?? false,
				`${tool.name} must not declare an approval gate`,
			).toBe(false);
		}
	});
});
