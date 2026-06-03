// Unit tests for why_column's pure data assembly (DAT-351). No DB, no LLM — the
// Drizzle reads + the Anthropic synthesis are smoke-covered; here we pin the
// per-intent driver pass-through, the evidence dimension_path composition +
// detail rendering, signal_count, and the not-analyzed (left-join miss) case.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the metadata client + the anthropic
// adapter; mock the env-dependent ones so the pure helper test needs no env and
// opens no connection (sets no process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectWhyData,
	type WhyEvidenceRow,
	type WhyReadinessRow,
} from "./why-column";

function readiness(overrides: Partial<WhyReadinessRow> = {}): WhyReadinessRow {
	return {
		columnId: "c_amount",
		columnName: "amount",
		tableName: "orders",
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{
				intent: "aggregation_intent",
				band: "investigate",
				risk: 0.42,
				drivers: [
					{
						node: "unit_declaration",
						dimension_path: "semantic.units.unit_declaration",
						label: "Unit Documentation",
						state: "high",
						impact_delta: 0.3,
					},
				],
			},
		],
		...overrides,
	};
}

const evidenceRows: WhyEvidenceRow[] = [
	{
		layer: "semantic",
		dimension: "units",
		subDimension: "unit_declaration",
		score: 0.8,
		detectorId: "unit_entropy",
		evidence: [{ metric: "undeclared_ratio", value: 0.8 }],
	},
];

describe("projectWhyData (DAT-351)", () => {
	it("keeps the full per-intent drivers and composes evidence signals", () => {
		const out = projectWhyData(readiness(), evidenceRows, 0);
		expect(out.column_name).toBe("amount");
		expect(out.table_name).toBe("orders");
		expect(out.found).toBe(true);
		expect(out.band).toBe("investigate");
		expect(out.analyzed).toBe(true);
		// why_column keeps the per-intent drivers (look_table dropped them).
		expect(out.intents[0].drivers[0].label).toBe("Unit Documentation");
		expect(out.intents[0].drivers[0].impact_delta).toBe(0.3);
		// dimension_path is composed layer.dimension.sub_dimension; detail is the
		// compact JSON of the detector evidence blob.
		expect(out.evidence[0].dimension_path).toBe(
			"semantic.units.unit_declaration",
		);
		expect(out.evidence[0].detector_id).toBe("unit_entropy");
		expect(out.evidence[0].detail).toContain("undeclared_ratio");
		// signal_count = number of evidence signals (the "based on N signals" basis).
		expect(out.signal_count).toBe(1);
	});

	it("treats a left-join miss as not-analyzed with no signals", () => {
		const out = projectWhyData(
			readiness({ band: null, worstIntentRisk: null, intents: null }),
			[],
			0,
		);
		expect(out.band).toBeNull();
		expect(out.analyzed).toBe(false);
		expect(out.intents).toEqual([]);
		expect(out.evidence).toEqual([]);
		expect(out.signal_count).toBe(0);
	});

	it("degrades a malformed intents blob to empty, keeping the scalar band", () => {
		const out = projectWhyData(
			readiness({ intents: "garbage" }),
			evidenceRows,
			0,
		);
		expect(out.intents).toEqual([]);
		expect(out.band).toBe("investigate");
		// Evidence is independent of the intents blob — still composed.
		expect(out.signal_count).toBe(1);
	});

	it("passes the workspace-wide pending-teach count through", () => {
		const out = projectWhyData(readiness(), evidenceRows, 3);
		expect(out.pending_teaches).toBe(3);
	});

	it("renders a null evidence blob as an empty detail (no throw)", () => {
		const out = projectWhyData(
			readiness(),
			[{ ...evidenceRows[0], evidence: null }],
			0,
		);
		expect(out.evidence[0].detail).toBe("");
	});
});
