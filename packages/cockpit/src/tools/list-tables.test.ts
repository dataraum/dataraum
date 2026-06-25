// Unit tests for list_tables' pure inventory projection (DAT-349). No DB — the
// Drizzle joins are smoke-covered; here we pin the per-table band rollup, the
// worst-band precedence, the not-analyzed (left-join miss) case, and tables with
// no columns.
//
// Importing the tool transitively pulls config.ts + the Postgres metadata client.
// Mock both so this pure-helper test needs no env and opens no connection — and,
// per registry.test.ts, set NO process.env (which would leak across files in a
// reused worker and un-skip the gated integration tests).
import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	buildInventory,
	type ColumnBandRow,
	type EnrichedViewRow,
	type InventoryTableRow,
	type TableEntityRow,
} from "./list-tables";

// A 40-char sha-1 hex digest, as the content-keyed upload sources mint them.
const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";

function tableRow(
	overrides: Partial<InventoryTableRow> = {},
): InventoryTableRow {
	return {
		tableId: "t_orders",
		tableName: `orders`,
		layer: "typed",
		rowCount: 1000,
		sourceId: "s_sales",
		sourceName: `src_${DIGEST}`,
		sourceType: "csv",
		sourceBackend: null,
		sourceConnectionConfig: {
			file_uris: [`s3://lake/uploads/${DIGEST}/sales.csv`],
		},
		...overrides,
	};
}

describe("buildInventory (DAT-349)", () => {
	it("carries provenance + shape through, names display-mapped (DAT-433)", () => {
		const [out] = buildInventory([tableRow()], []);
		expect(out).toMatchObject({
			table_id: "t_orders",
			// Prose name + physical_name are both the narrow name now (DAT-639);
			// physical_name stays the run_sql round-trip key.
			table_name: "orders",
			physical_name: `orders`,
			layer: "typed",
			row_count: 1000,
			source_id: "s_sales",
			// Upload source: the FILE's name, never the content-keyed `src_<digest>`.
			source_name: "sales.csv",
			source_type: "csv",
			source_backend: null,
		});
	});

	it("keeps a db_recipe source's user-chosen name as source_name", () => {
		const [out] = buildInventory(
			[
				tableRow({
					tableName: "journal_lines",
					sourceName: "finance",
					sourceType: "db_recipe",
					sourceBackend: "postgres",
					sourceConnectionConfig: { tables: [] },
				}),
			],
			[],
		);
		expect(out.source_name).toBe("finance");
		expect(out.table_name).toBe("journal_lines");
		expect(out.physical_name).toBe("journal_lines");
	});

	it("degrades a malformed upload config to the neutral 'upload', never the digest", () => {
		const [out] = buildInventory(
			[tableRow({ sourceConnectionConfig: null })],
			[],
		);
		expect(out.source_name).toBe("upload");
		// The digest appears ONLY in the sanctioned physical_name (the run_sql
		// round-trip key) — nowhere else in the projection.
		const { physical_name: _pn, ...rest } = out;
		expect(JSON.stringify(rest)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("degrades an empty-string upload URI to 'upload', not a blank label", () => {
		const [out] = buildInventory(
			[tableRow({ sourceConnectionConfig: { file_uris: [""] } })],
			[],
		);
		expect(out.source_name).toBe("upload");
	});

	it("rolls a table's column bands up to a distribution + column count", () => {
		const cols: ColumnBandRow[] = [
			{ tableId: "t_orders", band: "ready" },
			{ tableId: "t_orders", band: "ready" },
			{ tableId: "t_orders", band: "investigate" },
			{ tableId: "t_orders", band: "blocked" },
			{ tableId: "t_orders", band: null }, // not analyzed
		];
		const [out] = buildInventory([tableRow()], cols);
		expect(out.readiness).toEqual({
			ready: 2,
			investigate: 1,
			blocked: 1,
			unanalyzed: 1,
		});
		expect(out.column_count).toBe(5);
		expect(out.analyzed).toBe(true);
	});

	it("worst_band is the most severe present (blocked > investigate > ready)", () => {
		const worst = (bands: (string | null)[]) =>
			buildInventory(
				[tableRow()],
				bands.map((band) => ({ tableId: "t_orders", band })),
			)[0].worst_band;
		expect(worst(["ready", "investigate", "blocked"])).toBe("blocked");
		expect(worst(["ready", "investigate"])).toBe("investigate");
		expect(worst(["ready", "ready"])).toBe("ready");
	});

	it("treats a table whose columns all lack a band as not analyzed", () => {
		const [out] = buildInventory(
			[tableRow()],
			[
				{ tableId: "t_orders", band: null },
				{ tableId: "t_orders", band: null },
			],
		);
		expect(out.analyzed).toBe(false);
		expect(out.worst_band).toBeNull();
		expect(out.readiness).toEqual({
			ready: 0,
			investigate: 0,
			blocked: 0,
			unanalyzed: 2,
		});
		expect(out.column_count).toBe(2);
	});

	it("gives a table with no columns a zeroed rollup", () => {
		const [out] = buildInventory([tableRow()], []);
		expect(out.column_count).toBe(0);
		expect(out.analyzed).toBe(false);
		expect(out.worst_band).toBeNull();
		expect(out.readiness).toEqual({
			ready: 0,
			investigate: 0,
			blocked: 0,
			unanalyzed: 0,
		});
	});

	it("scopes each rollup to its own table", () => {
		const out = buildInventory(
			[
				tableRow({ tableId: "t_orders", tableName: "orders" }),
				tableRow({ tableId: "t_items", tableName: "items" }),
			],
			[
				{ tableId: "t_orders", band: "blocked" },
				{ tableId: "t_items", band: "ready" },
			],
		);
		const byId = new Map(out.map((t) => [t.table_id, t]));
		expect(byId.get("t_orders")?.worst_band).toBe("blocked");
		expect(byId.get("t_items")?.worst_band).toBe("ready");
	});
});

// DAT-477: the session/detect-grain orientation buildInventory attaches per table
// — entity classification (entity_type / is_fact) + the enriched fact/dimension
// views built off the table. The two new row arrays default to empty, so the
// pre-session state (no entity rows, no view rows) yields null entity facts + an
// empty enriched_views summary on every table.
describe("buildInventory entity + enriched_views (DAT-477)", () => {
	it("leaves entity_type/is_fact null and enriched_views empty pre-session", () => {
		// Two-arg call (the legacy add_source path) — no session has run.
		const [out] = buildInventory([tableRow()], []);
		expect(out.entity_type).toBeNull();
		expect(out.is_fact).toBeNull();
		expect(out.enriched_views).toEqual({
			count: 0,
			view_names: [],
			any_grain_verified: null,
		});
	});

	it("treats an explicitly empty entity/view set as the pre-session state", () => {
		const [out] = buildInventory([tableRow()], [], [], []);
		expect(out.entity_type).toBeNull();
		expect(out.is_fact).toBeNull();
		expect(out.enriched_views.count).toBe(0);
	});

	it("attaches the table's entity classification by table_id", () => {
		const entities: TableEntityRow[] = [
			{
				tableId: "t_orders",
				detectedEntityType: "transaction",
				isFactTable: true,
				detectedAt: new Date("2026-06-01T00:00:00Z"),
			},
		];
		const [out] = buildInventory([tableRow()], [], entities, []);
		expect(out.entity_type).toBe("transaction");
		expect(out.is_fact).toBe(true);
	});

	it("scopes the entity classification to its own table", () => {
		const entities: TableEntityRow[] = [
			{
				tableId: "t_orders",
				detectedEntityType: "transaction",
				isFactTable: true,
				detectedAt: new Date("2026-06-01T00:00:00Z"),
			},
			{
				tableId: "t_items",
				detectedEntityType: "reference",
				isFactTable: false,
				detectedAt: new Date("2026-06-01T00:00:00Z"),
			},
		];
		const out = buildInventory(
			[
				tableRow({ tableId: "t_orders", tableName: "orders" }),
				tableRow({ tableId: "t_items", tableName: "items" }),
			],
			[],
			entities,
			[],
		);
		const byId = new Map(out.map((t) => [t.table_id, t]));
		expect(byId.get("t_orders")).toMatchObject({
			entity_type: "transaction",
			is_fact: true,
		});
		expect(byId.get("t_items")).toMatchObject({
			entity_type: "reference",
			is_fact: false,
		});
	});

	it("summarizes the enriched views grouped under their fact table", () => {
		// Engine builds enriched view names as `enriched_<source>__<table>`
		// (enriched_views_phase.py); displayTableName strips the content-keyed
		// `<source>__` segment so no digest reaches the agent (DAT-431).
		const views: EnrichedViewRow[] = [
			{
				factTableId: "t_orders",
				viewName: `enriched_orders`,
				isGrainVerified: true,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
			{
				factTableId: "t_orders",
				viewName: `enriched_orders_by_region`,
				isGrainVerified: false,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
			// A different fact table's view must not leak onto t_orders.
			{
				factTableId: "t_items",
				viewName: `enriched_items`,
				isGrainVerified: false,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
		];
		const out = buildInventory(
			[
				tableRow({ tableId: "t_orders", tableName: "orders" }),
				tableRow({ tableId: "t_items", tableName: "items" }),
			],
			[],
			[],
			views,
		);
		const byId = new Map(out.map((t) => [t.table_id, t]));
		expect(byId.get("t_orders")?.enriched_views).toEqual({
			count: 2,
			view_names: ["enriched_orders", "enriched_orders_by_region"],
			any_grain_verified: true,
		});
		expect(byId.get("t_items")?.enriched_views).toEqual({
			count: 1,
			view_names: ["enriched_items"],
			any_grain_verified: false,
		});
		// The digest appears nowhere in the projected names (DAT-431).
		expect(JSON.stringify(out)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("drops a null/blank enriched-view name and keeps count == view_names", () => {
		const views: EnrichedViewRow[] = [
			{
				factTableId: "t_orders",
				viewName: null,
				isGrainVerified: false,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
			{
				factTableId: "t_orders",
				viewName: "",
				isGrainVerified: false,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
			{
				factTableId: "t_orders",
				viewName: `enriched_orders`,
				isGrainVerified: false,
				createdAt: new Date("2026-06-01T00:00:00Z"),
			},
		];
		const [out] = buildInventory([tableRow()], [], [], views);
		// `count` tracks the EMITTED names, never the raw row count — a name-less
		// stale row must not inflate `count: 3` against `view_names: [one]`.
		expect(out.enriched_views.count).toBe(1);
		expect(out.enriched_views.view_names).toEqual(["enriched_orders"]);
		// But grain-verified still keys off the raw rows (here none verified), and
		// is non-null because the fact table does carry views.
		expect(out.enriched_views.any_grain_verified).toBe(false);
	});

	it("keeps the existing inventory fields unchanged when entity data is present", () => {
		const entities: TableEntityRow[] = [
			{
				tableId: "t_orders",
				detectedEntityType: "transaction",
				isFactTable: true,
				detectedAt: new Date("2026-06-01T00:00:00Z"),
			},
		];
		const [out] = buildInventory(
			[tableRow()],
			[
				{ tableId: "t_orders", band: "ready" },
				{ tableId: "t_orders", band: "blocked" },
			],
			entities,
			[],
		);
		// The DAT-349 projection is untouched — additive only.
		expect(out).toMatchObject({
			table_id: "t_orders",
			table_name: "orders",
			physical_name: `orders`,
			row_count: 1000,
			column_count: 2,
			worst_band: "blocked",
			analyzed: true,
		});
		expect(out.readiness).toEqual({
			ready: 1,
			investigate: 0,
			blocked: 1,
			unanalyzed: 0,
		});
	});

	// `current_table_entities` / `current_enriched_views` are `session:{id}`-head-
	// scoped: a multi-session workspace carries one row-set per session per table.
	// buildInventory must pick the LATEST session deterministically (by detected_at
	// / created_at), order-independent of the input array — never the
	// nondeterministic "whichever row was last in the list".
	describe("multi-session determinism (DAT-476 cross-lane guard)", () => {
		const t1 = new Date("2026-06-01T00:00:00Z");
		const t2 = new Date("2026-06-08T00:00:00Z");

		it("picks the latest-session entity by detected_at", () => {
			const entities: TableEntityRow[] = [
				{
					tableId: "t_orders",
					detectedEntityType: "reference",
					isFactTable: false,
					detectedAt: t1,
				},
				{
					tableId: "t_orders",
					detectedEntityType: "transaction",
					isFactTable: true,
					detectedAt: t2,
				},
			];
			// Newest-last in the array — the wrong row would win without the dedup.
			const [out] = buildInventory([tableRow()], [], entities, []);
			expect(out.entity_type).toBe("transaction");
			expect(out.is_fact).toBe(true);
		});

		it("is order-independent — the latest entity wins newest-first too", () => {
			const newestFirst: TableEntityRow[] = [
				{
					tableId: "t_orders",
					detectedEntityType: "transaction",
					isFactTable: true,
					detectedAt: t2,
				},
				{
					tableId: "t_orders",
					detectedEntityType: "reference",
					isFactTable: false,
					detectedAt: t1,
				},
			];
			const [out] = buildInventory([tableRow()], [], newestFirst, []);
			expect(out.entity_type).toBe("transaction");
			expect(out.is_fact).toBe(true);
		});

		it("keeps only the latest session's enriched views, not a cross-session pile", () => {
			const views: EnrichedViewRow[] = [
				// Older session: a single, unverified view.
				{
					factTableId: "t_orders",
					viewName: `enriched_orders_old`,
					isGrainVerified: false,
					createdAt: t1,
				},
				// Newer session: two views, one grain-verified — this set must win
				// whole, with no carry-over from the older session.
				{
					factTableId: "t_orders",
					viewName: `enriched_orders`,
					isGrainVerified: true,
					createdAt: t2,
				},
				{
					factTableId: "t_orders",
					viewName: `enriched_orders_by_region`,
					isGrainVerified: false,
					createdAt: t2,
				},
			];
			const [out] = buildInventory([tableRow()], [], [], views);
			expect(out.enriched_views).toEqual({
				count: 2,
				view_names: ["enriched_orders", "enriched_orders_by_region"],
				any_grain_verified: true,
			});
			// The older session's view must not leak in.
			expect(out.enriched_views.view_names).not.toContain(
				"enriched_orders_old",
			);
		});
	});
});
