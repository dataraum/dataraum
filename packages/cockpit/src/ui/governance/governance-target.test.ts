// Unit coverage for the Governance deep-link seed builders (DAT-633) — pure.

import { describe, expect, it } from "vitest";

import {
	REPLAY_SEED,
	readinessDrillSeed,
	tableDrillSeed,
} from "./governance-target";

describe("readinessDrillSeed", () => {
	it("names why_column and the source for a column target", () => {
		const seed = readinessDrillSeed(
			"column:src_a__orders.amount",
			"src_a",
			"orders.amount",
		);
		expect(seed).toContain("why_column");
		expect(seed).toContain('"orders.amount"');
		expect(seed).toContain('source "src_a"');
	});

	it("names why_relationship for a relationship target", () => {
		expect(
			readinessDrillSeed("relationship:a::b", "", "relationship"),
		).toContain("why_relationship");
	});

	it("names why_table for a table target", () => {
		const seed = readinessDrillSeed("table:orders", "src_a", "orders");
		expect(seed).toContain("why_table");
		expect(seed).toContain('"orders"');
	});

	it("omits the source clause when source is unknown", () => {
		const seed = readinessDrillSeed(
			"column:orders.amount",
			"",
			"orders.amount",
		);
		expect(seed).not.toContain("source");
	});
});

describe("tableDrillSeed", () => {
	it("names why_table and the source", () => {
		const seed = tableDrillSeed("src_b", "customer_table");
		expect(seed).toContain("why_table");
		expect(seed).toContain('"customer_table"');
		expect(seed).toContain('source "src_b"');
	});
});

describe("REPLAY_SEED", () => {
	it("asks the agent to replay", () => {
		expect(REPLAY_SEED).toContain("replay");
	});
});
