// Unit tests for readWorkspaceMetricDag (DAT-882 — the WORKSPACE reader over
// the typed metric-DAG home). Split from teach-metric.test.ts because this one
// DB-mocks the reader-role metadata client; the rest of teach_metric's tests
// inject a fake reader and never touch this module's real implementation.
// Renamed from teach-metric-shipped-read.test.ts (DAT-882 rework): the function
// under test is no longer named `readShippedMetrics` — that name now belongs to
// the restored LIBRARY reader (fs/YAML rglob), which this file does NOT cover
// (its narrowing is unit-tested in teach-metric.test.ts; its own fs read is
// browser/integration-smoke territory).

import { describe, expect, it, vi } from "vitest";

// Importing teach-metric.ts (for readWorkspaceMetricDag) also pulls in the
// restored LIBRARY reader's top-level `import { config } from "../config"` —
// mock it (config.ts throws on missing env with no .env in this worktree) so
// the module loads cleanly; this test never touches dataraumConfigPath.
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));

// `readWorkspaceMetricDag` lazily imports the reader-role metadata client
// (DAT-882) — stub it so the unit test drives a fixed row set (or a forced read
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

import { readWorkspaceMetricDag } from "./teach-metric";

describe("readWorkspaceMetricDag (DAT-882 — reads the typed metric-DAG home)", () => {
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
		const shipped = await readWorkspaceMetricDag("finance");
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
		expect(await readWorkspaceMetricDag("finance")).toEqual([]);
	});

	it("degrades to [] on a metadata-read blip — never throws", async () => {
		mockState.error = new Error("connection reset");
		expect(await readWorkspaceMetricDag("finance")).toEqual([]);
		mockState.error = null;
	});
});
