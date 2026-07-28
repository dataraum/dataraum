// Unit tests for readShippedCycles (DAT-881 — reads the typed cycle_types home).
// Split from teach-cycle.test.ts because this one DB-mocks the reader-role
// metadata client; the rest of teach_cycle's tests inject a fake reader and never
// touch this module's real implementation.

import { describe, expect, it, vi } from "vitest";

// `readShippedCycles` lazily imports the reader-role metadata client (DAT-881) —
// stub it so the unit test drives a fixed row set (or a forced read error) instead
// of a real Postgres. The `#/` alias is load-bearing: a relative `./db/...` mock
// silently would not intercept (mirrors prompts/conventions.test.ts's pattern).
const mockState = vi.hoisted(() => ({
	rows: [] as Array<Record<string, unknown>>,
	error: null as Error | null,
}));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: async () => {
					if (mockState.error) throw mockState.error;
					return mockState.rows;
				},
			}),
		}),
	},
}));

import { readShippedCycles } from "./teach-cycle";

describe("readShippedCycles (DAT-881 — reads the typed cycle_types home)", () => {
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
		const shipped = await readShippedCycles("finance");
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
		expect(await readShippedCycles("finance")).toEqual([]);
	});

	it(
		"degrades to [] on a metadata-read blip — never throws (the shadow " +
			"affordance is a nice-to-have, not load-bearing)",
		async () => {
			mockState.error = new Error("connection reset");
			expect(await readShippedCycles("finance")).toEqual([]);
			mockState.error = null;
		},
	);
});
