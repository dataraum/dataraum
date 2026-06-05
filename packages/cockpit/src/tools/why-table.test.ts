// Unit tests for why_table's pure data assembly (DAT-415). No DB, no LLM — the
// Drizzle reads + the Anthropic synthesis are smoke-covered; here we pin the
// per-intent driver pass-through, evidence dimension_path + detail rendering,
// signal_count, and the found/analyzed distinctions.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the metadata client + the anthropic
// adapter; mock the env-dependent ones (sets no process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectWhyTable,
	type WhyTableEvidenceRow,
	type WhyTableReadinessRow,
} from "./why-table";

const TABLE_ID = "t_payments";
const TABLE_NAME = "payments";

function readiness(
	overrides: Partial<WhyTableReadinessRow> = {},
): WhyTableReadinessRow {
	return {
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{
				intent: "reporting_intent",
				band: "investigate",
				risk: 0.42,
				drivers: [
					{
						node: "dimension_coverage",
						dimension_path: "semantic.coverage.dimension_coverage",
						label: "Dimension Coverage",
						state: "high",
						impact_delta: 0.3,
					},
				],
			},
		],
		...overrides,
	};
}

function evidenceRow(
	overrides: Partial<WhyTableEvidenceRow> = {},
): WhyTableEvidenceRow {
	return {
		layer: "semantic",
		dimension: "coverage",
		subDimension: "dimension_coverage",
		score: 0.22,
		detectorId: "dimension_coverage",
		evidence: { uncovered_dimensions: 2, covered: 7 },
		...overrides,
	};
}

describe("projectWhyTable (DAT-415)", () => {
	it("assembles table identity, per-intent drivers, and correlated evidence", () => {
		const out = projectWhyTable(
			TABLE_ID,
			TABLE_NAME,
			readiness(),
			[evidenceRow()],
			0,
		);
		expect(out.table_id).toBe(TABLE_ID);
		expect(out.table_name).toBe(TABLE_NAME);
		expect(out.found).toBe(true);
		expect(out.band).toBe("investigate");
		expect(out.analyzed).toBe(true);
		// why_table KEEPS the full per-intent drivers (look_table drops them).
		expect(out.intents[0].drivers).toHaveLength(1);
		expect(out.intents[0].drivers[0].label).toBe("Dimension Coverage");
		// Evidence: dimension_path composed layer.dimension.subDimension; detail = JSON.
		expect(out.evidence[0].dimension_path).toBe(
			"semantic.coverage.dimension_coverage",
		);
		expect(out.evidence[0].detector_id).toBe("dimension_coverage");
		expect(out.evidence[0].detail).toContain("uncovered_dimensions");
		expect(out.signal_count).toBe(1);
	});

	it("treats a missing readiness row with evidence as found-but-unanalyzed", () => {
		const out = projectWhyTable(TABLE_ID, TABLE_NAME, null, [evidenceRow()], 0);
		expect(out.found).toBe(true); // evidence exists
		expect(out.analyzed).toBe(false); // but no band
		expect(out.band).toBeNull();
		expect(out.intents).toEqual([]);
		expect(out.signal_count).toBe(1);
	});

	it("treats no readiness and no evidence as not-found", () => {
		const out = projectWhyTable(TABLE_ID, TABLE_NAME, null, [], 0);
		expect(out.found).toBe(false);
		expect(out.analyzed).toBe(false);
		expect(out.evidence).toEqual([]);
	});

	it("carries a null table_name (stale id) through without throwing", () => {
		const out = projectWhyTable(TABLE_ID, null, null, [], 3);
		expect(out.table_name).toBeNull();
		expect(out.found).toBe(false);
		expect(out.pending_teaches).toBe(3);
	});

	it("degrades a malformed intents blob to empty rather than throwing", () => {
		const out = projectWhyTable(
			TABLE_ID,
			TABLE_NAME,
			readiness({ intents: "garbage" }),
			[],
			2,
		);
		expect(out.intents).toEqual([]);
		expect(out.band).toBe("investigate"); // scalar still comes through
		expect(out.pending_teaches).toBe(2);
	});

	it("strips the content-keyed `src_<digest>__` prefix from table_name (DAT-431)", () => {
		// The result feeds the agent's context + the synthesis prompt — never the
		// hash form. The round-trip key is table_id; the caller keeps the raw name
		// for the readiness target.
		const out = projectWhyTable(
			TABLE_ID,
			"src_204bc8e118543a6c35654c1f68c43539a2e226f2__payments",
			readiness(),
			[],
			0,
		);
		expect(out.table_name).toBe("payments");
	});
});
