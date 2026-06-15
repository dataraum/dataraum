// Unit tests for the relationship-target key grammar (DAT-409, DAT-506) — the TS
// mirror of the engine's relationship_target_key / parse_relationship_target /
// catalog_head_target. Pure functions; the contract that matters is round-trip
// stability + that the `::` separator survives UUIDs containing `-`.

import { describe, expect, it } from "vitest";

import {
	catalogHeadTarget,
	parseRelationshipTarget,
	relationshipTargetKey,
	tableTargetKey,
} from "./relationship-target";

describe("relationshipTargetKey / parseRelationshipTarget (DAT-409)", () => {
	it("builds the relationship:{from}::{to} key", () => {
		expect(relationshipTargetKey("col_a", "col_b")).toBe(
			"relationship:col_a::col_b",
		);
	});

	it("round-trips UUIDs (which contain '-') without ambiguity", () => {
		const from = "4dabf790-3243-4e9b-a6c1-2c443c89d223";
		const to = "9fed653d-0b37-4e59-8bd2-0ef59af0347f";
		const key = relationshipTargetKey(from, to);
		expect(parseRelationshipTarget(key)).toEqual({
			fromColumnId: from,
			toColumnId: to,
		});
	});

	it("returns null for a non-relationship target", () => {
		expect(parseRelationshipTarget("table:t1")).toBeNull();
		expect(parseRelationshipTarget("column:orders.amount")).toBeNull();
	});

	it("returns null for a malformed relationship target", () => {
		expect(parseRelationshipTarget("relationship:onlyone")).toBeNull();
		expect(parseRelationshipTarget("relationship:::")).toBeNull();
		expect(parseRelationshipTarget("relationship:a::")).toBeNull();
		expect(parseRelationshipTarget("relationship:::b")).toBeNull();
		// >2 parts (engine's `len(parts) != 2` branch) — unreachable with real
		// UUIDs (no `::`), but the guard must reject it rather than drop the tail.
		expect(parseRelationshipTarget("relationship:a::b::c")).toBeNull();
	});
});

describe("tableTargetKey (DAT-415)", () => {
	it("builds the table:{name} key keyed on the table NAME", () => {
		expect(tableTargetKey("smoke__payments")).toBe("table:smoke__payments");
	});

	it("is the inverse target a relationship parser rejects (distinct grain)", () => {
		expect(parseRelationshipTarget(tableTargetKey("orders"))).toBeNull();
	});
});

describe("catalogHeadTarget (DAT-506)", () => {
	it("is the constant workspace catalog head target", () => {
		expect(catalogHeadTarget()).toBe("catalog");
	});
});
