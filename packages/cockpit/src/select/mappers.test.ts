// Unit tests for the select-stage pure mappers (DAT-398).
//
// These are pure functions (no driver, no DB, no bucket): the source_type
// derivation, duplicate-basename rejection, recipe-name sanitization, and
// schema-qualified SELECT synthesis. `mappers.ts` only TYPE-imports
// `../duckdb/connect` (erased), so no config boot is needed — but we mock
// `#/config` defensively, matching the suite's `#/`-alias mock convention.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

import type { ConnectSchema } from "#/duckdb/connect";
import {
	connectTablesToRecipeTables,
	duplicateBasenames,
	recipeSqlForDisplayName,
	sanitizedStem,
	sanitizeRecipeName,
	sourceTypeForUri,
	sourceTypeForUris,
	uriStem,
} from "./mappers";

describe("sourceTypeForUri", () => {
	it("derives the engine source_type from the URI suffix (not the literal 'file')", () => {
		expect(sourceTypeForUri("s3://b/data/orders.csv")).toBe("csv");
		expect(sourceTypeForUri("s3://b/x.tsv")).toBe("csv");
		expect(sourceTypeForUri("s3://b/x.txt")).toBe("csv");
		expect(sourceTypeForUri("s3://b/x.parquet")).toBe("parquet");
		expect(sourceTypeForUri("s3://b/x.pq")).toBe("parquet");
		expect(sourceTypeForUri("s3://b/x.json")).toBe("json");
		expect(sourceTypeForUri("s3://b/x.jsonl")).toBe("json");
		expect(sourceTypeForUri("s3://b/x.ndjson")).toBe("json");
	});

	it("is case-insensitive on the extension", () => {
		expect(sourceTypeForUri("s3://b/X.CSV")).toBe("csv");
		expect(sourceTypeForUri("s3://b/X.Parquet")).toBe("parquet");
	});

	it("throws on an unsupported or missing extension", () => {
		expect(() => sourceTypeForUri("s3://b/notes.md")).toThrow(/source_type/);
		expect(() => sourceTypeForUri("s3://b/readme")).toThrow(/source_type/);
	});
});

describe("sourceTypeForUris", () => {
	it("returns the single homogeneous type for a multi-file selection", () => {
		expect(
			sourceTypeForUris(["s3://b/a.csv", "s3://b/c/d.csv", "s3://b/e.tsv"]),
		).toBe("csv");
	});

	it("rejects a mixed-type selection (one source row carries one source_type)", () => {
		expect(() =>
			sourceTypeForUris(["s3://b/a.csv", "s3://b/b.parquet"]),
		).toThrow(/mixes incompatible source types/);
	});

	it("rejects an empty list", () => {
		expect(() => sourceTypeForUris([])).toThrow(/empty/);
	});
});

describe("uriStem", () => {
	it("returns the extensionless basename of an s3 URI", () => {
		expect(uriStem("s3://bucket/uploads/abc/orders.csv")).toBe("orders");
		expect(uriStem("s3://bucket/data.parquet")).toBe("data");
		expect(uriStem("s3://bucket/noext")).toBe("noext");
	});
});

describe("duplicateBasenames", () => {
	it("returns nothing when every file maps to a distinct raw table", () => {
		expect(
			duplicateBasenames(["s3://b/orders.csv", "s3://b/customers.csv"]),
		).toEqual([]);
	});

	it("flags two files that collide on the same <source>__<stem> raw table", () => {
		// Same stem across folders → both name <source>__data → engine fails loud.
		expect(
			duplicateBasenames(["s3://b/a/data.csv", "s3://b/b/data.csv"]),
		).toEqual(["data"]);
	});

	it("flags files that differ only by extension (same stem)", () => {
		expect(
			duplicateBasenames(["s3://b/data.csv", "s3://b/data.parquet"]),
		).toEqual(["data"]);
	});

	it("returns colliding stems sorted", () => {
		expect(
			duplicateBasenames([
				"s3://b/zeta.csv",
				"s3://b/x/zeta.csv",
				"s3://b/alpha.csv",
				"s3://b/y/alpha.csv",
			]),
		).toEqual(["alpha", "zeta"]);
	});

	it("flags stems that differ only by CASE (engine lowercases the raw table)", () => {
		// Orders.csv + orders.csv both sanitize to `orders` → one raw table.
		expect(
			duplicateBasenames(["s3://b/Orders.csv", "s3://b/orders.csv"]),
		).toEqual(["Orders", "orders"]);
	});

	it("flags stems that differ only by PUNCTUATION (engine collapses to `_`)", () => {
		// q1-data + q1_data both sanitize to `q1_data` → one raw table.
		expect(
			duplicateBasenames(["s3://b/q1-data.csv", "s3://b/q1_data.csv"]),
		).toEqual(["q1-data", "q1_data"]);
	});

	it("does NOT flag stems that only look similar but sanitize distinctly", () => {
		expect(
			duplicateBasenames(["s3://b/orders.csv", "s3://b/orders_2024.csv"]),
		).toEqual([]);
	});
});

describe("sanitizedStem", () => {
	it("mirrors the engine sanitize_identifier collision domain", () => {
		expect(sanitizedStem("Orders")).toBe("orders");
		expect(sanitizedStem("q1-data")).toBe("q1_data");
		expect(sanitizedStem("q1_data")).toBe("q1_data");
		expect(sanitizedStem("  weird--name  ")).toBe("weird_name");
		expect(sanitizedStem("2024_orders")).toBe("x_2024_orders");
	});
});

describe("sanitizeRecipeName", () => {
	it("lowercases and collapses non-identifier runs to underscores", () => {
		expect(sanitizeRecipeName("SalesLT.Customer")).toBe("saleslt_customer");
		expect(sanitizeRecipeName("  weird--name  ")).toBe("weird_name");
		expect(sanitizeRecipeName("Invoices")).toBe("invoices");
	});

	it("ensures a leading letter (recipe pattern requires [a-z] first)", () => {
		expect(sanitizeRecipeName("2024_orders")).toBe("t_2024_orders");
		expect(sanitizeRecipeName("___")).toBe("t");
	});

	it("produces a valid recipe identifier matching ^[a-z][a-z0-9_]*$", () => {
		for (const display of ["dbo.Invoices", "2024_x", "A B C", "tbl"]) {
			expect(sanitizeRecipeName(display)).toMatch(/^[a-z][a-z0-9_]*$/);
		}
	});
});

describe("recipeSqlForDisplayName", () => {
	it("quotes a schema-qualified display name as schema.table", () => {
		expect(recipeSqlForDisplayName("dbo.Invoices")).toBe(
			'SELECT * FROM "dbo"."Invoices"',
		);
	});

	it("quotes an unqualified (default-schema) display name as a single ident", () => {
		expect(recipeSqlForDisplayName("Invoices")).toBe(
			'SELECT * FROM "Invoices"',
		);
	});

	it("escapes embedded double-quotes in each identifier segment (injection surface)", () => {
		expect(recipeSqlForDisplayName('dbo.we"ird')).toBe(
			'SELECT * FROM "dbo"."we""ird"',
		);
		expect(recipeSqlForDisplayName('a"; DROP TABLE x; --')).toBe(
			'SELECT * FROM "a""; DROP TABLE x; --"',
		);
	});

	it("only splits on the FIRST dot (a dotted table in a schema stays intact)", () => {
		expect(recipeSqlForDisplayName("sch.a.b")).toBe(
			'SELECT * FROM "sch"."a.b"',
		);
	});
});

describe("connectTablesToRecipeTables", () => {
	const table = (name: string): ConnectSchema["tables"][number] => ({
		name,
		rowCountEstimate: null,
		columns: [],
	});

	it("synthesizes one {name, sql} recipe entry per picked table", () => {
		expect(
			connectTablesToRecipeTables([table("dbo.Invoices"), table("Customers")]),
		).toEqual([
			{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
			{ name: "customers", sql: 'SELECT * FROM "Customers"' },
		]);
	});

	it("de-duplicates recipe names that sanitize to the same identifier", () => {
		// `dbo.Orders` and `staging.Orders` both sanitize the table part to
		// `orders` after schema-prefixing → distinct identifiers, but two display
		// names that sanitize identically must not collide on one raw table.
		const out = connectTablesToRecipeTables([
			table("Orders"),
			table("orders"),
			table("ORDERS"),
		]);
		expect(out.map((t) => t.name)).toEqual(["orders", "orders_2", "orders_3"]);
		// Each still selects the original display name verbatim.
		expect(out[1].sql).toBe('SELECT * FROM "orders"');
	});

	it("throws on an empty selection", () => {
		expect(() => connectTablesToRecipeTables([])).toThrow(/at least one/);
	});
});
