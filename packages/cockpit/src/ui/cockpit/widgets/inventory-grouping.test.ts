import { describe, expect, it } from "vitest";

import type { InventoryTable } from "#/tools/list-tables";
import {
	groupLogicalTables,
	humanizeBand,
	logicalTableName,
} from "#/ui/cockpit/widgets/inventory-grouping";

function phys(over: Partial<InventoryTable> = {}): InventoryTable {
	return {
		table_id: "t",
		table_name: "src__orders",
		layer: "typed",
		row_count: 100,
		column_count: 5,
		source_id: "s1",
		source_name: "src",
		source_type: "file",
		source_backend: null,
		source_status: null,
		analyzed: true,
		worst_band: "ready",
		readiness: { ready: 5, investigate: 0, blocked: 0, unanalyzed: 0 },
		...over,
	};
}

describe("logicalTableName", () => {
	it("strips the source prefix", () => {
		expect(
			logicalTableName("detection_v1__bank_transactions", "detection_v1"),
		).toBe("bank_transactions");
	});
	it("falls back to dropping up to the first __ when the prefix doesn't match", () => {
		expect(logicalTableName("foo__bar", "other")).toBe("bar");
	});
	it("returns the name unchanged when there is no prefix", () => {
		expect(logicalTableName("orders", "src")).toBe("orders");
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
			phys({ table_id: "a", table_name: "src__orders", source_id: "s1" }),
			phys({ table_id: "b", table_name: "src__items", source_id: "s1" }),
			phys({
				table_id: "c",
				table_name: "src__orders",
				source_id: "s2",
				source_name: "src2",
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
