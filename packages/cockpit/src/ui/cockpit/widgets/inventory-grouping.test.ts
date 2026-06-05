import { describe, expect, it } from "vitest";

import type { InventoryTable } from "#/tools/list-tables";
import {
	groupLogicalTables,
	humanizeBand,
	sourceGroup,
	UPLOADS_GROUP_ID,
} from "#/ui/cockpit/widgets/inventory-grouping";

// Fixtures mirror the list_tables PROJECTION (DAT-433): `table_name` arrives in
// display form, the raw DuckDB name rides in `physical_name`, and an upload's
// `source_name` is the uploaded file's name.
function phys(over: Partial<InventoryTable> = {}): InventoryTable {
	return {
		table_id: "t",
		table_name: "orders",
		physical_name: "src__orders",
		layer: "typed",
		row_count: 100,
		column_count: 5,
		source_id: "s1",
		source_name: "orders.csv",
		source_type: "csv",
		source_backend: null,
		analyzed: true,
		worst_band: "ready",
		readiness: { ready: 5, investigate: 0, blocked: 0, unanalyzed: 0 },
		...over,
	};
}

describe("sourceGroup (DAT-424 — demote uploads)", () => {
	const DIGEST_A = "src_204bc8e118543a6c35654c1f68c43539a2e226f2";
	const DIGEST_B = "src_3cb4f3325aa757324f32b2dbe30b4ca5a55a8b50";

	it("collapses every content-keyed upload under ONE 'Uploads' umbrella", () => {
		const a = sourceGroup(DIGEST_A, "csv", DIGEST_A);
		const b = sourceGroup(DIGEST_B, "parquet", DIGEST_B);
		// Two distinct content-keyed sources → the SAME group (no per-file badge).
		expect(a.id).toBe(UPLOADS_GROUP_ID);
		expect(b.id).toBe(UPLOADS_GROUP_ID);
		expect(a.kind).toBe("uploads");
		expect(a.label).toBe("Uploads");
		// The digest name is NEVER surfaced as the label.
		expect(a.label).not.toContain("src_");
	});

	it("keeps a db_recipe connection as its own named origin", () => {
		const g = sourceGroup("warehouse", "db_recipe", "s-warehouse");
		expect(g.kind).toBe("connection");
		expect(g.id).toBe("s-warehouse");
		expect(g.label).toBe("warehouse");
	});

	it("treats a db_recipe source as a connection even if its name looks upload-y", () => {
		// db sources are db_recipe regardless of name — they can't masquerade as uploads.
		const g = sourceGroup("src_warehouse", "db_recipe", "s9");
		expect(g.kind).toBe("connection");
		expect(g.id).toBe("s9");
	});
});

describe("humanizeBand", () => {
	it("title-cases known bands and dashes the absent one", () => {
		expect(humanizeBand("ready")).toBe("Ready");
		expect(humanizeBand("investigate")).toBe("Investigate");
		expect(humanizeBand("blocked")).toBe("Blocked");
		expect(humanizeBand(null)).toBe("—");
	});
});

describe("groupLogicalTables", () => {
	it("collapses raw/typed/quarantine layers into one logical row (typed representative)", () => {
		const out = groupLogicalTables([
			phys({
				table_id: "raw",
				layer: "raw",
				row_count: 5502,
				worst_band: null,
			}),
			phys({
				table_id: "typed",
				layer: "typed",
				row_count: 5500,
				worst_band: "investigate",
			}),
			phys({ table_id: "q", layer: "quarantine", row_count: 2 }),
		]);
		expect(out).toHaveLength(1);
		expect(out[0]?.representative.table_id).toBe("typed");
		expect(out[0]?.displayName).toBe("orders");
		expect(out[0]?.quarantineRows).toBe(2);
		expect(out[0]?.layers).toHaveLength(3);
	});

	it("separates tables that differ by source or name", () => {
		const out = groupLogicalTables([
			phys({ table_id: "a", table_name: "orders", source_id: "s1" }),
			phys({ table_id: "b", table_name: "items", source_id: "s1" }),
			phys({
				table_id: "c",
				table_name: "orders",
				source_id: "s2",
				source_name: "orders_v2.csv",
			}),
		]);
		expect(out).toHaveLength(3);
	});

	it("reports zero quarantine when there is no quarantine layer", () => {
		const out = groupLogicalTables([phys()]);
		expect(out[0]?.quarantineRows).toBe(0);
	});

	it("falls back to a non-quarantine layer when no typed layer exists", () => {
		const out = groupLogicalTables([
			phys({ table_id: "raw", layer: "raw" }),
			phys({ table_id: "q", layer: "quarantine", row_count: 3 }),
		]);
		expect(out[0]?.representative.table_id).toBe("raw");
		expect(out[0]?.quarantineRows).toBe(3);
	});
});
