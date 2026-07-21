// Unit tests for why_column's pure data assembly (DAT-351). No DB, no LLM — the
// Drizzle reads + the Anthropic synthesis are smoke-covered; here we pin the
// per-intent driver pass-through, the evidence dimension_path composition +
// detail rendering, signal_count, and the not-analyzed (left-join miss) case.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the metadata client + the anthropic
// adapter; mock the env-dependent ones so the pure helper test needs no env and
// opens no connection (sets no process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));
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
		bandStage: "session_detect",
		bandComputedAt: new Date("2026-06-11T10:00:00Z"),
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

	it("renders the narrow table_name as-is (DAT-639)", () => {
		// The result feeds the agent's context + the synthesis prompt — never the
		// hash form. Round-trips key on column_id, so nothing depends on the raw name.
		const out = projectWhyData(
			readiness({
				tableName: "orders",
			}),
			evidenceRows,
			0,
		);
		expect(out.table_name).toBe("orders");
	});

	// --- DAT-853: coverage + abstention rendering.

	it("carries coverage + the abstention trace so an unmeasured column is not a bare 'ready'", () => {
		// The engine keeps the band vocabulary frozen: a never-measured column reads
		// band='ready' with coverage='unmeasured' — the loss-path detectors ALL
		// abstained. The reader must surface coverage + reasons, never a bare 'ready'.
		const out = projectWhyData(
			readiness({
				band: "ready",
				coverage: "unmeasured",
				intents: [],
				abstentions: [
					{
						detector: "unit_entropy",
						reason: "insufficient_data",
						intents: ["aggregation_intent"],
					},
				],
			}),
			[],
			0,
		);
		expect(out.band).toBe("ready");
		expect(out.coverage).toBe("unmeasured");
		expect(out.abstentions).toEqual([
			{
				detector: "unit_entropy",
				reason: "insufficient_data",
				intents: ["aggregation_intent"],
			},
		]);
	});

	it("qualifies a partial-coverage band and keeps the real band", () => {
		const out = projectWhyData(
			readiness({ band: "investigate", coverage: "partial" }),
			evidenceRows,
			0,
		);
		expect(out.band).toBe("investigate");
		expect(out.coverage).toBe("partial");
	});

	it("null coverage + empty abstentions when there is no readiness row", () => {
		const out = projectWhyData(
			readiness({ band: null, coverage: null, abstentions: null }),
			[],
			0,
		);
		expect(out.coverage).toBeNull();
		expect(out.abstentions).toEqual([]);
	});

	it("renders an abstained detector as null score + reason, never a fabricated 0", () => {
		// An abstained entropy_object carries a NULL score — the old `score ?? 0`
		// turned that into a real 0.0 measurement. It must stay null and carry its
		// status + reason, and it must NOT count toward signal_count.
		const out = projectWhyData(
			readiness(),
			[
				evidenceRows[0],
				{
					layer: "value",
					dimension: "distribution",
					subDimension: "benford_compliance",
					score: null,
					status: "abstained",
					abstainReason: "not_applicable",
					detectorId: "benford",
					evidence: null,
				},
			],
			0,
		);
		const abstained = out.evidence.find((e) => e.detector_id === "benford");
		expect(abstained?.score).toBeNull();
		expect(abstained?.status).toBe("abstained");
		expect(abstained?.abstain_reason).toBe("not_applicable");
		// Two evidence rows surfaced, but only the ONE measured signal backs the band.
		expect(out.evidence).toHaveLength(2);
		expect(out.signal_count).toBe(1);
	});
});
