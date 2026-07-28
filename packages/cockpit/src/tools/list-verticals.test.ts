// Unit tests for list-verticals' pure concept-count resolution (DAT-883). No DB —
// the fs scan + Drizzle count are smoke-covered; here we pin
// `resolvedConceptCount`'s precedence rule directly, the fix for the double-count
// this file shipped (summing the on-disk YAML count with the typed `concepts`
// count instead of picking one).

import { describe, expect, it, vi } from "vitest";

// Importing the module pulls config.ts + the metadata write client (a live
// postgres() connection at import time) — mock the env-dependent ones, same
// pattern as why-table.test.ts.
vi.mock("#/config", () => ({
	config: { dataraumConfigPath: "/opt/dataraum/config" },
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataWriteDb: {} }));
vi.mock("#/db/metadata/write-surface", () => ({ conceptsWrite: {} }));

import { resolvedConceptCount } from "./list-verticals";

describe("resolvedConceptCount (DAT-883)", () => {
	it("pre-frame: no typed rows yet — falls back to the on-disk YAML count", () => {
		// A vertical this workspace has never touched: typed=0, so the on-disk
		// count is the only available richness hint.
		expect(resolvedConceptCount(22, 0)).toBe(22);
	});

	it("typed wins once ANY typed rows exist — never summed with the on-disk count", () => {
		// The bug this fixes: a builtin whose pipeline already ran had typed rows
		// that are a RE-SEED of the same on-disk concepts (or a superset, with
		// frame edits) — summing them with the on-disk count roughly doubled the
		// reported richness. Typed must win outright, not add.
		expect(resolvedConceptCount(22, 22)).toBe(22);
		expect(resolvedConceptCount(22, 5)).toBe(5);
		// A framed vertical's typed rows can also exceed a builtin's on-disk seed
		// count (frame-added concepts beyond the shipped set) — still typed, not
		// typed + onto.
		expect(resolvedConceptCount(0, 30)).toBe(30);
	});

	it("both zero: the guard's `=== 0` contract stays a TRUE zero", () => {
		// verticalConceptCount's caller (the add_source pre-flight guard, and
		// use-vertical.ts's adopt guard) reject on exactly `=== 0` — this is the
		// only source-combination that must return 0.
		expect(resolvedConceptCount(0, 0)).toBe(0);
	});
});
