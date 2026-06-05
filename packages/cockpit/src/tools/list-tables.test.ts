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
	type InventoryTableRow,
} from "./list-tables";

function tableRow(
	overrides: Partial<InventoryTableRow> = {},
): InventoryTableRow {
	return {
		tableId: "t_orders",
		tableName: "orders",
		layer: "typed",
		rowCount: 1000,
		sourceId: "s_sales",
		sourceName: "sales.csv",
		sourceType: "file",
		sourceBackend: "duckdb",
		...overrides,
	};
}

describe("buildInventory (DAT-349)", () => {
	it("carries provenance + shape through unchanged", () => {
		const [out] = buildInventory([tableRow()], []);
		expect(out).toMatchObject({
			table_id: "t_orders",
			table_name: "orders",
			layer: "typed",
			row_count: 1000,
			source_id: "s_sales",
			source_name: "sales.csv",
			source_type: "file",
			source_backend: "duckdb",
		});
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
