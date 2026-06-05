// Unit tests for the select-stage pure mappers (DAT-398, DAT-422).
//
// These are pure functions (no driver, no DB, no bucket): the per-file
// source_type derivation, the content-keyed source name, recipe-name
// sanitization, and schema-qualified SELECT synthesis. `mappers.ts` only
// TYPE-imports `../duckdb/connect` (erased), so no config boot is needed — but we
// mock `#/config` defensively, matching the suite's `#/`-alias mock convention.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

import type { ConnectSchema } from "#/duckdb/connect";
import {
	connectTablesToRecipeTables,
	contentKeyedSourceName,
	recipeSqlForDisplayName,
	sanitizeRecipeName,
	sourceTypeForUri,
} from "./mappers";

// A real 40-char sha-1 hex digest (the digest of the empty string) — the shape
// upload/digest.ts produces; `src_` + 40 = 44 chars, a valid source name.
const DIGEST = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

describe("contentKeyedSourceName", () => {
	it("derives src_<digest> from a staged upload URI", () => {
		expect(
			contentKeyedSourceName(`s3://dataraum-lake/uploads/${DIGEST}/orders.csv`),
		).toBe(`src_${DIGEST}`);
	});

	it("keys on the digest only — the filename and extension don't change it", () => {
		const a = contentKeyedSourceName(
			`s3://dataraum-lake/uploads/${DIGEST}/orders.csv`,
		);
		const b = contentKeyedSourceName(
			`s3://dataraum-lake/uploads/${DIGEST}/orders.parquet`,
		);
		// Same digest → same source name (the upload dedup already collapses
		// identical bytes to one key, so this is the re-select idempotency path).
		expect(a).toBe(b);
	});

	it("produces an engine-valid source name (lowercase, letter-led, ≤49)", () => {
		const name = contentKeyedSourceName(
			`s3://dataraum-lake/uploads/${DIGEST}/orders.csv`,
		);
		expect(name).toMatch(/^[a-z][a-z0-9_]{1,48}$/);
		expect(name.length).toBeLessThanOrEqual(49);
	});

	it("fails loud on a non-upload URI (a bucket/prefix object is not content-addressed)", () => {
		expect(() =>
			contentKeyedSourceName("s3://dataraum-lake/data/2024/sales.csv"),
		).toThrow(/must be a staged upload/);
	});

	it("fails loud on a nested or shallow key (not exactly uploads/<digest>/<file>)", () => {
		expect(() =>
			contentKeyedSourceName(`s3://dataraum-lake/uploads/${DIGEST}/a/b.csv`),
		).toThrow(/must be a staged upload/);
		expect(() =>
			contentKeyedSourceName(`s3://dataraum-lake/uploads/${DIGEST}`),
		).toThrow(/must be a staged upload/);
	});
});

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
