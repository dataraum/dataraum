// Unit tests for readWorkspaceCycleTypes (DAT-881 — the WORKSPACE reader over
// the typed cycle_types home). Split from teach-cycle.test.ts because this one
// DB-mocks the reader-role metadata client; the rest of teach_cycle's tests
// inject a fake reader and never touch this module's real implementation.
// Renamed from teach-cycle-shipped-read.test.ts (DAT-881 rework): the function
// under test is no longer named `readShippedCycles` — that name now belongs to
// the restored LIBRARY reader (fs/YAML), which this file does NOT cover (its
// narrowing is unit-tested in teach-cycle.test.ts; its own fs read is
// browser/integration-smoke territory).

import { describe, expect, it, vi } from "vitest";

// Importing teach-cycle.ts (for readWorkspaceCycleTypes) also pulls in the
// restored LIBRARY reader's top-level `import { config } from "../config"` —
// mock it (config.ts throws on missing env with no .env in this worktree) so
// the module loads cleanly; this test never touches dataraumConfigPath.
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));

// `readWorkspaceCycleTypes` lazily imports the reader-role metadata client
// (DAT-881) — stub it so the unit test drives a fixed row set (or a forced read
// error) instead of a real Postgres. The `#/` alias is load-bearing: a relative
// `./db/...` mock silently would not intercept (mirrors
// prompts/conventions.test.ts's pattern). The chain includes `.orderBy()` (item
// 5 — the retired fs read was deterministic; a DB read needs an explicit order)
// so the mock must resolve there, not at `.where()`.
const mockState = vi.hoisted(() => ({
	rows: [] as Array<Record<string, unknown>>,
	error: null as Error | null,
}));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: () => ({
					orderBy: async () => {
						if (mockState.error) throw mockState.error;
						return mockState.rows;
					},
				}),
			}),
		}),
	},
}));

import { readWorkspaceCycleTypes } from "./teach-cycle";

describe("readWorkspaceCycleTypes (DAT-881 — reads the typed cycle_types home)", () => {
	it("maps typed rows to ShippedCycleSpec", async () => {
		mockState.error = null;
		mockState.rows = [
			{
				name: "order_to_cash",
				description: "Revenue cycle from order through payment.",
				businessValue: "high",
				completionIndicators: ["paid", "closed"],
			},
			{
				name: "procure_to_pay",
				description: null,
				businessValue: null,
				completionIndicators: null,
			},
		];
		const shipped = await readWorkspaceCycleTypes("finance");
		expect(shipped).toEqual([
			{
				name: "order_to_cash",
				description: "Revenue cycle from order through payment.",
				business_value: "high",
				completion_indicators: ["paid", "closed"],
			},
			{
				name: "procure_to_pay",
				description: null,
				business_value: null,
				completion_indicators: null,
			},
		]);
	});

	it("returns [] when the vocabulary is unseeded (empty read)", async () => {
		mockState.error = null;
		mockState.rows = [];
		expect(await readWorkspaceCycleTypes("finance")).toEqual([]);
	});

	it(
		"degrades to [] on a metadata-read blip — never throws (the shadow " +
			"affordance is a nice-to-have, not load-bearing)",
		async () => {
			mockState.error = new Error("connection reset");
			expect(await readWorkspaceCycleTypes("finance")).toEqual([]);
			mockState.error = null;
		},
	);
});
