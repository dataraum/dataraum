// Unit coverage for the query sub-agent's schema-block formatter (DAT-485). The
// Drizzle reads are integration/smoke-covered; here we pin the pure `formatSchema`
// projection: lake addressing, the per-column type + [concept] tag, deterministic
// ordering, and the empty-workspace note.

import { describe, expect, it, vi } from "vitest";

// query-context → ../db/metadata/client + ../duckdb/lake → #/config. The pure
// formatter touches none of it; stub the boundary so the import graph loads.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
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
	{ columnId: "c1", businessConcept: "amount" },
	// c2 has no concept; c3 maps to account_classification.
	{ columnId: "c3", businessConcept: "account_classification" },
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
