// Unit coverage for the query sub-agent's schema-block formatter (DAT-485). The
// Drizzle reads are integration/smoke-covered; here we pin the pure `formatSchema`
// projection: lake addressing, the per-column type + [concept] tag, deterministic
// ordering, and the empty-workspace note.

import { describe, expect, it, vi } from "vitest";

// query-context → ../db/metadata/client + ../duckdb/lake → #/config. The pure
// formatter touches none of it; stub the boundary so the import graph loads.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type { DriverRanking } from "./look-drivers";
import type { TableEntity } from "./look-table";
import {
	type CatalogAxisRow,
	type CatalogHierarchyRow,
	type EntityBlockRow,
	formatCatalog,
	formatDrivers,
	formatEntities,
	formatSchema,
	preferEnriched,
	type SchemaColumnRow,
	type SchemaConceptRow,
	type SchemaTableRow,
} from "./query-context";

const tables: SchemaTableRow[] = [
	{ tableId: "t1", physicalName: "journal_lines", layer: "typed" },
	{ tableId: "t2", physicalName: "chart_of_accounts", layer: "typed" },
];

const columnRows: SchemaColumnRow[] = [
	{ tableId: "t1", columnId: "c1", name: "Betrag", resolvedType: "DECIMAL" },
	{ tableId: "t1", columnId: "c2", name: "Datum", resolvedType: "DATE" },
	{
		tableId: "t2",
		columnId: "c3",
		name: "account_type",
		resolvedType: "VARCHAR",
	},
];

const concepts: SchemaConceptRow[] = [
	{
		columnId: "c1",
		businessConcept: "amount",
		temporalBehavior: null,
		temporalBehaviorContested: null,
	},
	// c2 has no concept; c3 maps to account_classification.
	{
		columnId: "c3",
		businessConcept: "account_classification",
		temporalBehavior: null,
		temporalBehaviorContested: null,
	},
];

describe("formatSchema", () => {
	it("addresses each table as lake.<layer>.<name>", () => {
		const block = formatSchema(tables, columnRows, concepts);
		expect(block).toContain("Table lake.typed.journal_lines:");
		expect(block).toContain("Table lake.typed.chart_of_accounts:");
	});

	it("addresses an enriched view in the typed schema (lake.typed.<view>)", () => {
		// enriched views live in the typed DuckDB schema (schema_for_layer), so the
		// address is lake.typed.<view> — NOT lake.enriched.<view>.
		const block = formatSchema(
			[
				{
					tableId: "e1",
					physicalName: "enriched_src__orders",
					layer: "enriched",
				},
			],
			[
				{
					tableId: "e1",
					columnId: "ec1",
					name: "region",
					resolvedType: "VARCHAR",
				},
			],
			[],
		);
		expect(block).toContain("Table lake.typed.enriched_src__orders:");
		expect(block).not.toContain("lake.enriched.");
	});

	it("shows each column's type and its [concept] tag when mapped", () => {
		const block = formatSchema(tables, columnRows, concepts);
		expect(block).toContain('- "Betrag" :: DECIMAL  [concept: amount]');
		expect(block).toContain(
			'- "account_type" :: VARCHAR  [concept: account_classification]',
		);
	});

	it("marks the resolved stock/flow behaviour and an open contest (DAT-509)", () => {
		const semantics: SchemaConceptRow[] = [
			{
				columnId: "c1",
				businessConcept: "account_balance",
				temporalBehavior: "point_in_time",
				temporalBehaviorContested: true,
			},
			// A resolved, uncontested flow renders the marker without the caveat —
			// even with no concept mapped.
			{
				columnId: "c3",
				businessConcept: null,
				temporalBehavior: "additive",
				temporalBehaviorContested: false,
			},
		];
		const block = formatSchema(tables, columnRows, semantics);
		expect(block).toContain(
			'- "Betrag" :: DECIMAL  [concept: account_balance] (point_in_time)  [stock/flow contested]',
		);
		const accountLine = block
			.split("\n")
			.find((l) => l.includes('"account_type"'));
		expect(accountLine).toBe('  - "account_type" :: VARCHAR (additive)');
		// The instruction header explains both markers to the sub-agent.
		expect(block).toContain("never SUM it across periods");
	});

	it("omits the concept tag for an unmapped column", () => {
		const block = formatSchema(tables, columnRows, concepts);
		expect(block).toContain('- "Datum" :: DATE');
		// The Datum line carries no [concept: …].
		const datumLine = block.split("\n").find((l) => l.includes('"Datum"'));
		expect(datumLine).toBeDefined();
		expect(datumLine).not.toContain("[concept:");
	});

	it("falls back to `unknown` for a null resolved type", () => {
		const block = formatSchema(
			[{ tableId: "t1", physicalName: "t", layer: "typed" }],
			[{ tableId: "t1", columnId: "c1", name: "x", resolvedType: null }],
			[],
		);
		expect(block).toContain('- "x" :: unknown');
	});

	it("orders tables and columns deterministically (by name)", () => {
		const block = formatSchema(tables, columnRows, concepts);
		// chart_of_accounts sorts before journal_lines.
		expect(block.indexOf("chart_of_accounts")).toBeLessThan(
			block.indexOf("journal_lines"),
		);
		// Within journal_lines, Betrag before Datum.
		expect(block.indexOf('"Betrag"')).toBeLessThan(block.indexOf('"Datum"'));
	});

	it("notes an empty workspace", () => {
		const block = formatSchema([], [], []);
		expect(block).toContain("No queryable tables in the workspace yet");
		expect(block).toContain("<schema>");
	});
});

describe("preferEnriched (mirror the engine's prefer-enriched rule)", () => {
	const t = (layer: string, physicalName: string) => ({ layer, physicalName });

	it("returns ONLY enriched rows when any exist (all-or-nothing)", () => {
		const rows = [
			t("typed", "orders"),
			t("enriched", "enriched_orders"),
			t("typed", "regions"),
		];
		expect(preferEnriched(rows)).toEqual([t("enriched", "enriched_orders")]);
	});

	it("falls back to the typed rows when no enriched view exists", () => {
		const rows = [t("typed", "orders"), t("typed", "regions")];
		expect(preferEnriched(rows)).toEqual(rows);
	});

	it("returns empty for an empty input", () => {
		expect(preferEnriched([])).toEqual([]);
	});
});

describe("formatCatalog (DAT-538 dimension catalog block)", () => {
	const addr = new Map<string, string>([
		["t1", "lake.typed.sales"],
		["t2", "lake.typed.orders"],
	]);
	const axes: CatalogAxisRow[] = [
		{ tableId: "t1", columnName: "region" },
		{ tableId: "t1", columnName: "channel" },
	];

	it("lists the natural dimensions per table (sorted)", () => {
		const block = formatCatalog(axes, [], addr);
		expect(block).toContain("Table lake.typed.sales:");
		expect(block).toContain('dimensions: "channel", "region"'); // sorted
		// No grain-safe / fan-out framing — this block is context, not a gate.
		expect(block).not.toContain("grain-safe");
		expect(block).not.toContain("GROUP BY");
	});

	it("renders an alias group as canonical ≡ others (group by canonical)", () => {
		const hierarchies: CatalogHierarchyRow[] = [
			{
				tableId: "t1",
				kind: "alias",
				canonicalLabel: "region",
				members: [{ column_name: "region" }, { column_name: "region_code" }],
			},
		];
		const block = formatCatalog(axes, hierarchies, addr);
		expect(block).toContain('alias: "region" ≡ "region_code"');
	});

	it("renders a non-alias hierarchy as an ordered drill-down chain", () => {
		const hierarchies: CatalogHierarchyRow[] = [
			{
				tableId: "t1",
				kind: "functional_dependency",
				canonicalLabel: null,
				members: [
					{ column_name: "city" },
					{ column_name: "region" },
					{ column_name: "country" },
				],
			},
		];
		const block = formatCatalog(axes, hierarchies, addr);
		expect(block).toContain('drill-down: "city" → "region" → "country"');
	});

	it("notes an empty catalog", () => {
		const block = formatCatalog([], [], addr);
		expect(block).toContain("No catalogued dimensions yet");
		expect(block).toContain("<dimensions>");
	});

	it("falls back to the raw table_id when no address is known", () => {
		const block = formatCatalog(
			[{ tableId: "orphan", columnName: "x" }],
			[],
			new Map(),
		);
		expect(block).toContain("Table orphan:");
	});
});

// --- formatDrivers (DAT-548): the <drivers> block projection ---------------------

const ranking = (over: Partial<DriverRanking> = {}): DriverRanking => ({
	measure: "revenue",
	target_type: "flow",
	grain: "row",
	entity: null,
	n_rows: 12000,
	ranked_dimensions: [
		{ dimension: "region", gain: 0.421 },
		{ dimension: "channel", gain: 0.18 },
	],
	driver_paths: [["region", "channel"], ["segment"]],
	interesting_slices: [
		{ dimension: "region", value: "EMEA", effect: 0.3, support: 1200 },
	],
	secondary_dimensions: [],
	...over,
});

describe("formatDrivers", () => {
	it("renders a row-grain stanza with top drivers (gain) + drill paths", () => {
		const block = formatDrivers([ranking()]);
		expect(block).toContain("<drivers>");
		expect(block).toContain('Measure "revenue" (flow, row-level, n=12000):');
		// Gain rounded to 2dp, strongest first.
		expect(block).toContain('top drivers: "region" (0.42), "channel" (0.18)');
		// Multi-col path joined coarse→fine; single-col path kept.
		expect(block).toContain('drill paths: "region" → "channel"; "segment"');
		expect(block).toContain(
			'notable slices: "region"=EMEA (effect 0.30, support 1200)',
		);
	});

	it("labels an entity grain as 'within <identity>' and keeps secondary drivers separate", () => {
		const block = formatDrivers([
			ranking({
				measure: "ltv",
				grain: "entity",
				entity: "customer_id",
				secondary_dimensions: [
					{
						dimension: "tenure",
						gain: 0.22,
						grain: "entity",
						entity: "customer_id",
					},
				],
			}),
		]);
		expect(block).toContain(
			'Measure "ltv" (flow, within customer_id, n=12000):',
		);
		expect(block).toContain(
			'other-grain drivers: "tenure" (within customer_id, 0.22)',
		);
	});

	it("caps the slice list to keep the block a hint, not a dump", () => {
		const slices = Array.from({ length: 6 }, (_, i) => ({
			dimension: "region",
			value: `R${i}`,
			effect: 0.1,
			support: 10,
		}));
		const block = formatDrivers([ranking({ interesting_slices: slices })]);
		expect(block).toContain('"region"=R0');
		expect(block).toContain('"region"=R2');
		// 4th+ slice dropped (MAX_SLICES_PER_MEASURE = 3).
		expect(block).not.toContain('"region"=R3');
	});

	it("drops a measure with no significant driver; all-empty → a note", () => {
		const block = formatDrivers([
			ranking({ measure: "kept" }),
			// barren keeps the base fixture's slice but has no ranked dims/paths —
			// still dropped (the filter gates on a driver, not on slices).
			ranking({
				measure: "barren",
				ranked_dimensions: [],
				driver_paths: [],
			}),
		]);
		expect(block).toContain('Measure "kept"');
		expect(block).not.toContain("barren");

		const empty = formatDrivers([
			ranking({ ranked_dimensions: [], driver_paths: [] }),
		]);
		expect(empty).toContain("No driver rankings yet");
		expect(empty).toContain("<drivers>");
	});

	it("carries the inform-don't-block usage guidance", () => {
		const block = formatDrivers([ranking()]);
		expect(block).toContain("you still author the SQL");
		expect(block).toContain("top-ranked dimension is the sensible default");
	});
});

// --- <entities> block (DAT-607) --------------------------------------------------

function entity(overrides: Partial<TableEntity> = {}): TableEntity {
	return {
		entity_type: "transaction",
		is_fact_table: true,
		is_dimension_table: false,
		grain: ["OrderID"],
		time_columns: [{ column: "OrderDate", aspect: "order", note: "Placed." }],
		identity_columns: [
			{ column: "CustomerID", note: "Recurring customer identity." },
		],
		description: "One row per order.",
		...overrides,
	};
}

const entAddr = (name: string) => `lake.typed.${name}`;

describe("formatEntities (DAT-607)", () => {
	it("renders grain, time, and identities per table with the entity head", () => {
		const out = formatEntities([
			{ address: entAddr("wwi_recent_orders"), entity: entity() },
		]);
		expect(out).toContain("<entities>");
		expect(out).toContain(
			"Table lake.typed.wwi_recent_orders — transaction (fact):",
		);
		expect(out).toContain("  grain: OrderID");
		expect(out).toContain("  time: OrderDate (order)");
		expect(out).toContain(
			"  identities: CustomerID — Recurring customer identity.",
		);
	});

	it("labels a dimension table and omits the kind when neither flag is set", () => {
		const dim = formatEntities([
			{
				address: entAddr("wwi_suppliers"),
				entity: entity({
					entity_type: "suppliers",
					is_fact_table: false,
					is_dimension_table: true,
					grain: ["SupplierID"],
					time_columns: [],
					identity_columns: [],
				}),
			},
		]);
		expect(dim).toContain(
			"Table lake.typed.wwi_suppliers — suppliers (dimension):",
		);
		expect(dim).toContain("  grain: SupplierID");
		expect(dim).not.toContain("  time:");
		expect(dim).not.toContain("  identities:");

		const noKind = formatEntities([
			{
				address: entAddr("t"),
				entity: entity({
					entity_type: "thing",
					is_fact_table: false,
					is_dimension_table: false,
					time_columns: [],
					identity_columns: [],
				}),
			},
		]);
		expect(noKind).toContain("Table lake.typed.t — thing:");
	});

	it("drops a table with no grain/time/identity signal", () => {
		const out = formatEntities([
			{
				address: entAddr("empty"),
				entity: entity({
					entity_type: "thing",
					grain: [],
					time_columns: [],
					identity_columns: [],
				}),
			},
		]);
		expect(out).toBe(
			"<entities>\n(No table entities detected yet.)\n</entities>",
		);
	});

	it("returns the one-line note for an empty entity set", () => {
		expect(formatEntities([])).toBe(
			"<entities>\n(No table entities detected yet.)\n</entities>",
		);
	});

	it("sorts stanzas by address (deterministic prompt)", () => {
		const out = formatEntities([
			{ address: entAddr("zebra"), entity: entity({ grain: ["z"] }) },
			{ address: entAddr("alpha"), entity: entity({ grain: ["a"] }) },
		]);
		expect(out.indexOf("lake.typed.alpha")).toBeLessThan(
			out.indexOf("lake.typed.zebra"),
		);
	});

	it("caps identities per table and clamps a long note", () => {
		const many: EntityBlockRow[] = [
			{
				address: entAddr("wide"),
				entity: entity({
					identity_columns: Array.from({ length: 12 }, (_, i) => ({
						column: `id_${i}`,
						note: "",
					})),
				}),
			},
		];
		const capped = formatEntities(many);
		expect(capped).toContain("id_7"); // 8th (index 7) kept
		expect(capped).not.toContain("id_8"); // 9th dropped by the cap

		const longNote = "x".repeat(300);
		const clamped = formatEntities([
			{
				address: entAddr("n"),
				entity: entity({
					identity_columns: [{ column: "CustomerID", note: longNote }],
				}),
			},
		]);
		expect(clamped).toContain("…");
		expect(clamped).not.toContain(longNote);
	});
});
