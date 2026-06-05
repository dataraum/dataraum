// Unit tests for look_table's pure row→shape projection (DAT-350). No DB — the
// Drizzle join is smoke-covered; here we pin the JSONB parsing, the not-analyzed
// (left-join miss) case, the top-driver cap, and graceful degradation of a
// malformed blob.

import { describe, expect, it, vi } from "vitest";

// Importing the tool transitively pulls config.ts + the Postgres metadata client.
// Mock both so this pure-helper test needs no env and opens no connection — and,
// per registry.test.ts, set NO process.env (which would leak across files in a
// reused worker and un-skip the gated integration tests).
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectColumnReadiness,
	projectLookTable,
	projectTableBand,
	type ReadinessRow,
	type TableBandRow,
} from "./look-table";

function row(overrides: Partial<ReadinessRow> = {}): ReadinessRow {
	return {
		columnId: "col_1",
		columnName: "amount",
		resolvedType: "DECIMAL(18,2)",
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{ intent: "query", band: "ready", risk: 0.1, drivers: [] },
			{
				intent: "aggregation",
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
		topDrivers: [
			{
				node: "unit_declaration",
				dimension_path: "semantic.units.unit_declaration",
				label: "Unit Documentation",
				state: "high",
				impact_delta: 0.3,
			},
		],
		...overrides,
	};
}

describe("projectColumnReadiness (DAT-350)", () => {
	it("projects per-intent bands (without the heavy per-intent drivers) + top drivers", () => {
		const out = projectColumnReadiness(row());
		expect(out.column_id).toBe("col_1");
		expect(out.column_name).toBe("amount");
		expect(out.resolved_type).toBe("DECIMAL(18,2)");
		expect(out.band).toBe("investigate");
		expect(out.worst_intent_risk).toBe(0.42);
		// Per-intent overview carries band + risk only — NOT the drivers (that's
		// why_column's drill-down).
		expect(out.intents).toEqual([
			{ intent: "query", band: "ready", risk: 0.1 },
			{ intent: "aggregation", band: "investigate", risk: 0.42 },
		]);
		// Top drivers keep their self-describing label (no node dictionary needed).
		expect(out.top_drivers).toEqual([
			{ label: "Unit Documentation", state: "high", impact_delta: 0.3 },
		]);
	});

	it("treats a left-join miss (no readiness row) as not-analyzed", () => {
		const out = projectColumnReadiness(
			row({
				band: null,
				worstIntentRisk: null,
				intents: null,
				topDrivers: null,
			}),
		);
		expect(out.band).toBeNull();
		expect(out.worst_intent_risk).toBeNull();
		expect(out.intents).toEqual([]);
		expect(out.top_drivers).toEqual([]);
	});

	it("caps top drivers at 3 (the overview shows the worst few)", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectColumnReadiness(row({ topDrivers: many }));
		expect(out.top_drivers).toHaveLength(3);
		expect(out.top_drivers.map((d) => d.label)).toEqual(["L0", "L1", "L2"]);
	});

	it("degrades a malformed JSONB blob to empty rather than throwing", () => {
		const out = projectColumnReadiness(
			row({ intents: { not: "an array" }, topDrivers: "garbage" }),
		);
		expect(out.intents).toEqual([]);
		expect(out.top_drivers).toEqual([]);
		// The scalar band still comes through — it's a real column, just with a
		// blob we couldn't parse.
		expect(out.band).toBe("investigate");
	});
});

function tableRow(overrides: Partial<TableBandRow> = {}): TableBandRow {
	return {
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{ intent: "query", band: "ready", risk: 0.1, drivers: [] },
			{
				intent: "reporting",
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
		topDrivers: [
			{
				node: "dimension_coverage",
				dimension_path: "semantic.coverage.dimension_coverage",
				label: "Dimension Coverage",
				state: "high",
				impact_delta: 0.3,
			},
		],
		...overrides,
	};
}

describe("projectTableBand (DAT-415)", () => {
	it("projects the table-grain band + per-intent bands (no drivers) + top drivers", () => {
		const out = projectTableBand(tableRow());
		expect(out.band).toBe("investigate");
		expect(out.worst_intent_risk).toBe(0.42);
		// The overview carries band + risk per intent only — drivers are why_table's.
		expect(out.intents).toEqual([
			{ intent: "query", band: "ready", risk: 0.1 },
			{ intent: "reporting", band: "investigate", risk: 0.42 },
		]);
		expect(out.top_drivers).toEqual([
			{ label: "Dimension Coverage", state: "high", impact_delta: 0.3 },
		]);
	});

	it("caps top drivers at 3", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectTableBand(tableRow({ topDrivers: many }));
		expect(out.top_drivers).toHaveLength(3);
		expect(out.top_drivers.map((d) => d.label)).toEqual(["L0", "L1", "L2"]);
	});

	it("degrades a malformed JSONB blob to empty rather than throwing", () => {
		const out = projectTableBand(
			tableRow({ intents: "garbage", topDrivers: { not: "an array" } }),
		);
		expect(out.intents).toEqual([]);
		expect(out.top_drivers).toEqual([]);
		expect(out.band).toBe("investigate"); // scalar still comes through
	});
});

describe("projectLookTable (DAT-433)", () => {
	const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";

	it("splits the raw name into display table_name + raw physical_name", () => {
		const out = projectLookTable(
			"t_orders",
			`src_${DIGEST}__orders`,
			[projectColumnReadiness(row())],
			null,
			2,
		);
		expect(out.table_name).toBe("orders");
		expect(out.physical_name).toBe(`src_${DIGEST}__orders`);
		expect(out.analyzed).toBe(true);
		expect(out.pending_teaches).toBe(2);
		// The digest appears ONLY in the sanctioned physical_name (the run_sql
		// round-trip key) — nowhere else in the projection.
		const { physical_name: _pn, ...rest } = out;
		expect(JSON.stringify(rest)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("returns the empty not-found shell for a stale table id", () => {
		const out = projectLookTable("t_gone", null, [], null, 0);
		expect(out).toEqual({
			table_id: "t_gone",
			table_name: "",
			physical_name: "",
			analyzed: false,
			pending_teaches: 0,
			columns: [],
			table_readiness: null,
		});
	});

	it("analyzed reflects whether any column carries a band", () => {
		const unanalyzed = projectLookTable(
			"t_orders",
			"orders",
			[
				projectColumnReadiness(
					row({ band: null, worstIntentRisk: null, intents: null }),
				),
			],
			null,
			0,
		);
		expect(unanalyzed.analyzed).toBe(false);
	});
});
