// Unit tests for handleShippedMetricDag (DAT-482, DAT-882 vertical-mismatch
// guard). Deps injected (the handleMint convention, ../reports/mint.test.ts) so
// the vertical-mismatch gate + the shadow-narrow are asserted with no real
// Postgres.
//
// The guard exists because `readWorkspaceMetricDag` (teach-metric.ts) IGNORES
// its own `vertical` argument and always serves the workspace's bound
// active_vertical (correct for every OTHER caller, which derives `vertical`
// FROM the bound one) — but this route is a standalone API any client can call
// with an arbitrary vertical string. Silently ignoring a mismatch would render
// vertical Y's DAG under a request that named vertical X; this route rejects
// that instead.

import { describe, expect, it, vi } from "vitest";

// handleShippedMetricDag's default deps reference teach-metric.ts's exports at
// module scope, which transitively pulls in (a) the restored LIBRARY reader's
// `import { config } from "../config"` — config.ts throws on missing env with
// no .env in this worktree — and (b) teach.ts's static `import { metadataDb }
// from "../db/metadata/client"`, which imports `SQL` from "bun" (cannot load
// under Node, the unit project's runtime — mirrors frame-family.test.ts's
// mock). Every test here injects fakes anyway, so neither default is ever
// actually invoked; both are mocked purely to let the module graph load.
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: vi.fn() })) },
	metadataWriteDb: { insert: vi.fn(() => ({ values: vi.fn() })) },
}));

import type { ShippedMetricSpec } from "../../tools/metric-spec";
import { handleShippedMetricDag } from "./shipped-metric-dag";

const EBITDA: ShippedMetricSpec = {
	graph_id: "ebitda",
	name: "EBITDA",
	description: "Earnings before interest, taxes, D&A.",
	category: "profitability",
	output: { type: "scalar", unit: "currency" },
	dependencies: {
		revenue: { type: "extract", source: { standard_field: "revenue" } },
	},
};

function deps(
	overrides: Partial<Parameters<typeof handleShippedMetricDag>[1]> = {},
) {
	return {
		getBoundVertical: vi.fn(async () => "finance"),
		getWorkspaceMetrics: vi.fn(async () => [EBITDA]),
		...overrides,
	};
}

describe("handleShippedMetricDag", () => {
	it("400s when the request's vertical does not match the workspace's bound one", async () => {
		const d = deps({ getBoundVertical: vi.fn(async () => "finance") });
		const result = await handleShippedMetricDag(
			{ vertical: "retail", graph_id: "ebitda" },
			d,
		);
		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toMatch(
			/does not match the workspace's bound vertical/,
		);
		// The mismatch is caught BEFORE the workspace read runs — no wrong-vertical
		// query even attempted.
		expect(d.getWorkspaceMetrics).not.toHaveBeenCalled();
	});

	it("400s against an UNBOUND workspace (_adhoc) naming any other vertical", async () => {
		const d = deps({ getBoundVertical: vi.fn(async () => "_adhoc") });
		const result = await handleShippedMetricDag(
			{ vertical: "finance", graph_id: "ebitda" },
			d,
		);
		expect(result.status).toBe(400);
	});

	it("serves the shadowed metric's narrowed DAG when the vertical matches", async () => {
		const d = deps();
		const result = await handleShippedMetricDag(
			{ vertical: "finance", graph_id: "ebitda" },
			d,
		);
		expect(result.status).toBe(200);
		expect(result.body).toMatchObject({
			graph_id: "ebitda",
			name: "EBITDA",
			category: "profitability",
		});
		expect(d.getWorkspaceMetrics).toHaveBeenCalledWith("finance");
	});

	it("returns null (not an error) when the graph_id shadows nothing", async () => {
		const d = deps();
		const result = await handleShippedMetricDag(
			{ vertical: "finance", graph_id: "not_shipped" },
			d,
		);
		expect(result.status).toBe(200);
		expect(result.body).toBeNull();
	});
});
