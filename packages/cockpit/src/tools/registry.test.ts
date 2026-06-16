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

import { CANVAS_TOOLS, CHIP_ONLY } from "#/ui/cockpit/tool-result-to-canvas";
import { toolsByKind } from "./registry";

/** The reachable tool surface = the union of the per-kind buckets, deduped by
 * name (a tool may sit in several kinds). Derived — never a hand-maintained
 * const — so it cannot drift from what a chat can actually call (DAT-532). */
const allTools = [
	...new Map(
		Object.values(toolsByKind)
			.flat()
			.map((t) => [t.name, t] as const),
	).values(),
];
const namesOf = (ts: ReadonlyArray<{ name: string }>) => ts.map((t) => t.name);

describe("tool registry (DAT-353)", () => {
	it("registers the toolset with unique names", () => {
		const names = namesOf(allTools);
		expect(names).toHaveLength(new Set(names).size); // no dupes
		expect(new Set(names)).toEqual(
			new Set([
				"list_sources",
				"list_tables",
				"list_verticals",
				"use_vertical",
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

	it("routes every registered tool to exactly one surface — canvas XOR chip-only (DAT-527)", () => {
		// The guardrail for the DAT-526 P1 registry split: a tool must be consciously
		// routed — projected to the canvas (PROJECTORS/CANVAS_TOOLS) OR explicitly
		// chip-only — never silently un-routed. XOR catches BOTH gaps: in neither
		// (forgotten) and in both (contradiction).
		for (const tool of allTools) {
			const inCanvas = CANVAS_TOOLS.has(tool.name);
			const inChip = CHIP_ONLY.has(tool.name);
			expect(
				inCanvas !== inChip,
				`${tool.name} must be in exactly one of PROJECTORS / CHIP_ONLY (canvas=${inCanvas}, chip=${inChip})`,
			).toBe(true);
		}
	});

	it("has no stale CHIP_ONLY entries — every name is a registered tool (DAT-527)", () => {
		const names = new Set<string>(namesOf(allTools));
		for (const name of CHIP_ONLY) {
			expect(
				names.has(name),
				`CHIP_ONLY '${name}' is not a registered tool`,
			).toBe(true);
		}
	});

	it("places kind-discriminating tools in exactly the right bucket (DAT-532)", () => {
		const connect = new Set(namesOf(toolsByKind.connect));
		const stage = new Set(namesOf(toolsByKind.stage));
		const analyse = new Set(namesOf(toolsByKind.analyse));

		// begin_session is a Stage-only tool — absent from a Connect chat (the AC).
		expect(stage.has("begin_session")).toBe(true);
		expect(connect.has("begin_session")).toBe(false);
		expect(analyse.has("begin_session")).toBe(false);

		// answer is the Analyse surface; raw run_sql is Stage-only (answer replaces
		// it for analysis — run_sql overflows context). probe is Connect-only.
		expect(analyse.has("answer")).toBe(true);
		expect(stage.has("answer")).toBe(false);
		expect(connect.has("answer")).toBe(false);
		expect(stage.has("run_sql")).toBe(true);
		expect(analyse.has("run_sql")).toBe(false);
		expect(connect.has("probe")).toBe(true);
		expect(analyse.has("probe")).toBe(false);

		// select/frame/use_vertical are Connect's acquisition tools.
		for (const t of ["select", "frame", "use_vertical", "upload"]) {
			expect(connect.has(t)).toBe(true);
			expect(stage.has(t)).toBe(false);
		}

		// The read+explain set overlaps Stage and Analyse (overlap is by design).
		for (const t of ["look_table", "why_column", "look_metric"]) {
			expect(stage.has(t) && analyse.has(t)).toBe(true);
		}
	});

	it("declares NO approval gate on any tool — every tool runs directly", () => {
		// The approval gate is gone: every tool (reads AND write/compute alike) runs
		// on the user's natural-language instruction, so none may declare approval.
		// A regression guard — a re-introduced `needsApproval: true` on any tool
		// fails here.
		for (const tool of allTools) {
			expect(
				(tool as { needsApproval?: boolean }).needsApproval ?? false,
				`${tool.name} must not declare an approval gate`,
			).toBe(false);
		}
	});
});
