// Unit tests for look_relationships' pure row→shape projection (DAT-409). No DB —
// the Drizzle reads are smoke-covered; here we pin the target→pair parsing, the
// endpoint-name resolution (and its degrade-to-null miss), the JSONB parsing, the
// top-driver cap, and the non-relationship-target guard.

import { describe, expect, it, vi } from "vitest";

// Importing the tool transitively pulls config.ts + the metadata client. Mock
// both so this pure-helper test needs no env and opens no connection (sets no
// process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	type ColumnNameLookup,
	projectRelationshipReadiness,
	type RelationshipReadinessRow,
} from "./look-relationships";

const FROM = "c_from";
const TO = "c_to";

function row(
	overrides: Partial<RelationshipReadinessRow> = {},
): RelationshipReadinessRow {
	return {
		target: `relationship:${FROM}::${TO}`,
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{ intent: "query_intent", band: "ready", risk: 0.1, drivers: [] },
			{
				intent: "aggregation_intent",
				band: "investigate",
				risk: 0.42,
				drivers: [
					{
						node: "referential_integrity",
						dimension_path: "structural.relations.referential_integrity",
						label: "Referential Integrity",
						state: "high",
						impact_delta: 0.3,
					},
				],
			},
		],
		topDrivers: [
			{
				node: "referential_integrity",
				dimension_path: "structural.relations.referential_integrity",
				label: "Referential Integrity",
				state: "high",
				impact_delta: 0.3,
			},
		],
		...overrides,
	};
}

function names(): ColumnNameLookup {
	return new Map([
		[FROM, { columnName: "invoice_id", tableName: "payments" }],
		[TO, { columnName: "invoice_id", tableName: "invoices" }],
	]);
}

describe("projectRelationshipReadiness (DAT-409)", () => {
	it("projects the pair, endpoint names, per-intent bands, and top drivers", () => {
		const out = projectRelationshipReadiness(row(), names());
		expect(out).not.toBeNull();
		if (!out) return;
		expect(out.from_column_id).toBe(FROM);
		expect(out.to_column_id).toBe(TO);
		expect(out.from_table_name).toBe("payments");
		expect(out.from_column_name).toBe("invoice_id");
		expect(out.to_table_name).toBe("invoices");
		expect(out.to_column_name).toBe("invoice_id");
		expect(out.band).toBe("investigate");
		expect(out.worst_intent_risk).toBe(0.42);
		// Per-intent overview carries band + risk only (drivers are why_relationship).
		expect(out.intents).toEqual([
			{ intent: "query_intent", band: "ready", risk: 0.1 },
			{ intent: "aggregation_intent", band: "investigate", risk: 0.42 },
		]);
		expect(out.top_drivers).toEqual([
			{ label: "Referential Integrity", state: "high", impact_delta: 0.3 },
		]);
	});

	it("returns null for a non-relationship target (defensive guard)", () => {
		expect(
			projectRelationshipReadiness(row({ target: "table:t1" }), names()),
		).toBeNull();
	});

	it("degrades a missing endpoint name to null rather than dropping the row", () => {
		const out = projectRelationshipReadiness(row(), new Map());
		expect(out).not.toBeNull();
		if (!out) return;
		expect(out.from_table_name).toBeNull();
		expect(out.from_column_name).toBeNull();
		expect(out.to_table_name).toBeNull();
		// The pair + band still come through — the relationship is real.
		expect(out.from_column_id).toBe(FROM);
		expect(out.band).toBe("investigate");
	});

	it("caps top drivers at 3", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectRelationshipReadiness(
			row({ topDrivers: many }),
			names(),
		);
		expect(out?.top_drivers).toHaveLength(3);
		expect(out?.top_drivers.map((d) => d.label)).toEqual(["L0", "L1", "L2"]);
	});

	it("degrades a malformed JSONB blob to empty rather than throwing", () => {
		const out = projectRelationshipReadiness(
			row({ intents: { not: "an array" }, topDrivers: "garbage" }),
			names(),
		);
		expect(out?.intents).toEqual([]);
		expect(out?.top_drivers).toEqual([]);
		expect(out?.band).toBe("investigate");
	});

	it("strips the content-keyed `src_<digest>__` prefix from endpoint table names (DAT-431)", () => {
		// This result goes back to the agent — never the hash form. The drill-down
		// round-trip (why_relationship) keys on the column ids, which pass through raw.
		const lookup: ColumnNameLookup = new Map([
			[
				FROM,
				{
					columnName: "invoice_id",
					tableName: "src_204bc8e118543a6c35654c1f68c43539a2e226f2__payments",
				},
			],
			[
				TO,
				{
					columnName: "invoice_id",
					tableName: "src_3cb4f3325aa757324f32b2dbe30b4ca5a55a8b50__invoices",
				},
			],
		]);
		const out = projectRelationshipReadiness(row(), lookup);
		expect(out?.from_table_name).toBe("payments");
		expect(out?.to_table_name).toBe("invoices");
		expect(out?.from_column_id).toBe(FROM);
		expect(out?.to_column_id).toBe(TO);
	});
});
