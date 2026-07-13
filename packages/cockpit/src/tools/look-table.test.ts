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
	projectColumnSemantic,
	projectLookTable,
	projectTableBand,
	projectTableEntity,
	type ReadinessRow,
	type TableBandRow,
	type TableEntityRow,
	tableEntityWhere,
} from "./look-table";

function row(overrides: Partial<ReadinessRow> = {}): ReadinessRow {
	return {
		columnId: "col_1",
		columnName: "amount",
		resolvedType: "DECIMAL(18,2)",
		band: "investigate",
		worstIntentRisk: 0.42,
		// Light per-column semantics (DAT-476) — populated by default; the
		// unannotated case overrides all three to null.
		businessConcept: "monetary_amount",
		semanticRole: "measure",
		businessName: "Order Amount",
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
		// The light semantic triple rides alongside (DAT-476).
		expect(out.semantic).toEqual({
			business_concept: "monetary_amount",
			semantic_role: "measure",
			business_name: "Order Amount",
		});
	});

	it("treats a left-join miss (no readiness row) as not-analyzed", () => {
		const out = projectColumnReadiness(
			row({
				band: null,
				worstIntentRisk: null,
				intents: null,
				topDrivers: null,
				businessConcept: null,
				semanticRole: null,
				businessName: null,
			}),
		);
		expect(out.band).toBeNull();
		expect(out.worst_intent_risk).toBeNull();
		expect(out.intents).toEqual([]);
		expect(out.top_drivers).toEqual([]);
		// Unannotated column (semantic left-join also missed) → null semantic block.
		expect(out.semantic).toBeNull();
	});

	it("surfaces ALL top drivers (no cap) — every ranked driver is served (DAT-649)", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectColumnReadiness(row({ topDrivers: many }));
		expect(out.top_drivers).toHaveLength(6);
		expect(out.top_drivers.map((d) => d.label)).toEqual([
			"L0",
			"L1",
			"L2",
			"L3",
			"L4",
			"L5",
		]);
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

describe("projectColumnSemantic (DAT-476)", () => {
	it("projects the light semantic triple when the column is annotated", () => {
		expect(projectColumnSemantic(row())).toEqual({
			business_concept: "monetary_amount",
			semantic_role: "measure",
			business_name: "Order Amount",
		});
	});

	it("is null when the column is unannotated (every semantic field absent)", () => {
		const out = projectColumnSemantic(
			row({ businessConcept: null, semanticRole: null, businessName: null }),
		);
		expect(out).toBeNull();
	});

	it("keeps the block (partial fields) when at least one field is present", () => {
		// A partial annotation (e.g. only a business_concept) is still an annotation —
		// the block survives with the absent fields null, not collapsed to null.
		const out = projectColumnSemantic(
			row({
				businessConcept: "monetary_amount",
				semanticRole: null,
				businessName: null,
			}),
		);
		expect(out).toEqual({
			business_concept: "monetary_amount",
			semantic_role: null,
			business_name: null,
		});
	});
});

function entityRow(overrides: Partial<TableEntityRow> = {}): TableEntityRow {
	return {
		detectedEntityType: "transaction",
		// Raw row carries only the engine's role string now (DAT-728); the
		// projection derives is_fact_table:true / is_dimension_table:false from "fact".
		tableRole: "fact",
		// The engine ALWAYS persists grain as the DICT shape `{"columns": [...]}`
		// (`analysis/semantic/processor.py`), NOT a bare array — fixture it that way
		// so the projection's real-shape parse is exercised (the bare-array form
		// only ever shows up as a tolerated fallback).
		grainColumns: { columns: ["order_id", "line_no"] },
		timeColumns: [
			{
				column: "order_date",
				aspect: "order",
				note: "When the order was placed.",
			},
		],
		identityColumns: [
			{ column: "order_id", note: "Recurring order identity (would-be FK)." },
		],
		description: "One row per order line item.",
		...overrides,
	};
}

describe("projectTableEntity (DAT-476)", () => {
	it("maps the descriptive header through, grain from the engine's {columns:[…]} dict", () => {
		expect(projectTableEntity(entityRow())).toEqual({
			entity_type: "transaction",
			is_fact_table: true,
			is_dimension_table: false,
			grain: ["order_id", "line_no"],
			time_columns: [
				{
					column: "order_date",
					aspect: "order",
					note: "When the order was placed.",
				},
			],
			identity_columns: [
				{ column: "order_id", note: "Recurring order identity (would-be FK)." },
			],
			description: "One row per order line item.",
		});
	});

	it("tolerates a null/malformed time_columns blob (degrades to [])", () => {
		expect(
			projectTableEntity(entityRow({ timeColumns: null })).time_columns,
		).toEqual([]);
		expect(
			projectTableEntity(entityRow({ timeColumns: "nope" })).time_columns,
		).toEqual([]);
	});

	it("tolerates a null/malformed identity_columns blob (degrades to [])", () => {
		expect(
			projectTableEntity(entityRow({ identityColumns: null })).identity_columns,
		).toEqual([]);
		expect(
			projectTableEntity(entityRow({ identityColumns: "nope" }))
				.identity_columns,
		).toEqual([]);
	});

	it("tolerates a bare string[] grain (defensive fallback)", () => {
		expect(
			projectTableEntity(entityRow({ grainColumns: ["order_id"] })).grain,
		).toEqual(["order_id"]);
	});

	it("degrades a genuinely malformed/absent grain blob to an empty grain rather than throwing", () => {
		expect(projectTableEntity(entityRow({ grainColumns: null })).grain).toEqual(
			[],
		);
		// Neither the dict-with-`columns` nor the bare-array shape.
		expect(
			projectTableEntity(entityRow({ grainColumns: { not: "an array" } }))
				.grain,
		).toEqual([]);
		expect(
			projectTableEntity(entityRow({ grainColumns: { columns: "nope" } }))
				.grain,
		).toEqual([]);
	});

	it("renders narrow grain / time-axis columns / description (DAT-639)", () => {
		// Engine free-text/name fields are NARROW post-DAT-639; the projection
		// renders them as-is and the result stays digest-free.
		const out = projectTableEntity(
			entityRow({
				grainColumns: { columns: [`order_id`, "line_no"] },
				timeColumns: [
					{ column: `order_date`, aspect: "order", note: "Placed." },
				],
				identityColumns: [{ column: `order_id`, note: "Recurring identity." }],
				description: `One row per line in orders.`,
			}),
		);
		expect(out.grain).toEqual(["order_id", "line_no"]);
		expect(out.time_columns[0].column).toBe("order_date");
		expect(out.identity_columns[0].column).toBe("order_id");
		// The 40-hex digest is gone from every projected field.
		expect(JSON.stringify(out)).not.toMatch(/src_[0-9a-f]{40}/);
	});
});

// The drizzle SQL predicate is a self-referential object whose bound literals
// live in `Param` nodes (a `value` + `encoder` pair); collect just those via a
// cycle-safe walk. The column refs back-reference the whole table schema, so
// matching on column names would leak every sibling column — the bound VALUES
// are the unambiguous signal of what the predicate actually filters on.
function boundValues(predicate: unknown): string[] {
	const seen = new Set<unknown>();
	const out: string[] = [];
	const walk = (o: unknown, depth: number) => {
		if (depth > 8 || o === null || typeof o !== "object" || seen.has(o)) return;
		seen.add(o);
		const rec = o as Record<string, unknown>;
		if ("value" in rec && "encoder" in rec && typeof rec.value === "string") {
			out.push(rec.value);
		}
		for (const v of Object.values(rec)) walk(v, depth + 1);
	};
	walk(predicate, 0);
	return out.sort();
}

describe("tableEntityWhere (DAT-476, DAT-506 — workspace-catalog entity pick)", () => {
	it("filters on table_id alone (the view resolves at the workspace catalog head)", () => {
		// current_table_entities resolves at the workspace catalog head now (one row
		// per table_id, no session axis), so the predicate binds the table_id only;
		// the call-site `detected_at desc` order is the defensive tiebreak.
		expect(boundValues(tableEntityWhere("t_orders"))).toEqual(["t_orders"]);
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

	it("surfaces ALL top drivers (no cap) (DAT-649)", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectTableBand(tableRow({ topDrivers: many }));
		expect(out.top_drivers).toHaveLength(6);
		expect(out.top_drivers.map((d) => d.label)).toEqual([
			"L0",
			"L1",
			"L2",
			"L3",
			"L4",
			"L5",
		]);
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
	it("surfaces the narrow table_name + the physical_name round-trip key (DAT-639)", () => {
		const out = projectLookTable(
			"t_orders",
			`orders`,
			[projectColumnReadiness(row())],
			null,
			2,
			projectTableEntity(entityRow()),
		);
		expect(out.table_name).toBe("orders");
		expect(out.physical_name).toBe(`orders`);
		expect(out.analyzed).toBe(true);
		expect(out.pending_teaches).toBe(2);
		// The entity header carries straight through (DAT-476).
		expect(out.entity?.entity_type).toBe("transaction");
		expect(out.entity?.is_fact_table).toBe(true);
		expect(out.entity?.grain).toEqual(["order_id", "line_no"]);
		// The digest appears ONLY in the sanctioned physical_name (the run_sql
		// round-trip key) — nowhere else in the projection.
		const { physical_name: _pn, ...rest } = out;
		expect(JSON.stringify(rest)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("returns the empty not-found shell for a stale table id — entity null", () => {
		const out = projectLookTable("t_gone", null, [], null, 0);
		expect(out).toEqual({
			table_id: "t_gone",
			table_name: "",
			physical_name: "",
			analyzed: false,
			pending_teaches: 0,
			columns: [],
			table_readiness: null,
			entity: null,
		});
	});

	it("entity is null pre-session (no promoted detect run)", () => {
		// No entity arg — the default (null) models a table that hasn't been through
		// a begin_session detect run yet; the descriptive header is absent.
		const out = projectLookTable(
			"t_orders",
			"orders",
			[projectColumnReadiness(row())],
			null,
			0,
		);
		expect(out.entity).toBeNull();
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
