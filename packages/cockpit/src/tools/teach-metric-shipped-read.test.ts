// Unit tests for readShippedMetrics (DAT-882 — reads the typed metric-DAG home).
// Split from teach-metric.test.ts because this one DB-mocks the reader-role
// metadata client; the rest of teach_metric's tests inject a fake reader and never
// touch this module's real implementation.

import { describe, expect, it, vi } from "vitest";

// `readShippedMetrics` lazily imports the reader-role metadata client (DAT-882) —
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

import { readShippedMetrics } from "./teach-metric";

describe("readShippedMetrics (DAT-882 — reads the typed metric-DAG home)", () => {
	it("maps typed rows to ShippedMetricSpec, DAG body passed through opaque", async () => {
		mockState.error = null;
		mockState.rows = [
			{
				graphId: "ebitda",
				name: "EBITDA",
				description: "Earnings before interest, taxes, D&A.",
				category: "profitability",
				output: { type: "scalar", unit: "currency" },
				dependencies: { revenue: { type: "extract" } },
			},
			{
				graphId: "gross_profit",
				name: null,
				description: null,
				category: null,
				output: null,
				dependencies: null,
			},
		];
		const shipped = await readShippedMetrics("finance");
		expect(shipped).toEqual([
			{
				graph_id: "ebitda",
				name: "EBITDA",
				description: "Earnings before interest, taxes, D&A.",
				category: "profitability",
				output: { type: "scalar", unit: "currency" },
				dependencies: { revenue: { type: "extract" } },
			},
			{
				graph_id: "gross_profit",
				name: null,
				description: null,
				category: null,
				output: null,
				dependencies: null,
			},
		]);
	});

	it("returns [] when the typed home is unseeded (empty read)", async () => {
		mockState.error = null;
		mockState.rows = [];
		expect(await readShippedMetrics("finance")).toEqual([]);
	});

	it("degrades to [] on a metadata-read blip — never throws", async () => {
		mockState.error = new Error("connection reset");
		expect(await readShippedMetrics("finance")).toEqual([]);
		mockState.error = null;
	});
});
