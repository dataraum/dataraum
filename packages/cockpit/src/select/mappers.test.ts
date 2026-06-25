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
	recipeSqlForDisplayName,
	reservedSourceNamePrefix,
	sanitizeRecipeName,
	sourceTypeForUri,
	uploadTableName,
} from "./mappers";
import {
	contentKeyedSourceName,
	recipeContentHash,
} from "./source-content-hash";

// A real 40-char sha-1 hex digest (the digest of the empty string) — the shape
// upload/digest.ts produces; `src_` + 40 = 44 chars, a valid source name.
const DIGEST = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
// DAT-505: staged uploads live under the workspace's `<ws>/uploads/` prefix.
const WS = "00000000-0000-0000-0000-000000000001";

describe("contentKeyedSourceName", () => {
	it("derives src_<digest> from a staged upload URI", () => {
		expect(
			contentKeyedSourceName(
				`s3://dataraum-lake/${WS}/uploads/${DIGEST}/orders.csv`,
			),
		).toBe(`src_${DIGEST}`);
	});

	it("keys on the digest only — the filename and extension don't change it", () => {
		const a = contentKeyedSourceName(
			`s3://dataraum-lake/${WS}/uploads/${DIGEST}/orders.csv`,
		);
		const b = contentKeyedSourceName(
			`s3://dataraum-lake/${WS}/uploads/${DIGEST}/orders.parquet`,
		);
		// Same digest → same source name (the upload dedup already collapses
		// identical bytes to one key, so this is the re-select idempotency path).
		expect(a).toBe(b);
	});

	it("produces an engine-valid source name (lowercase, letter-led, ≤49)", () => {
		const name = contentKeyedSourceName(
			`s3://dataraum-lake/${WS}/uploads/${DIGEST}/orders.csv`,
		);
		expect(name).toMatch(/^[a-z][a-z0-9_]{1,48}$/);
		expect(name.length).toBeLessThanOrEqual(49);
	});

	it("fails loud on a non-upload URI (a bucket/prefix object is not content-addressed)", () => {
		expect(() =>
			contentKeyedSourceName("s3://dataraum-lake/data/2024/sales.csv"),
		).toThrow(/must be a staged upload/);
	});

	it("fails loud on a key missing the workspace prefix (bare uploads/<digest>/<file>)", () => {
		expect(() =>
			contentKeyedSourceName(`s3://dataraum-lake/uploads/${DIGEST}/orders.csv`),
		).toThrow(/must be a staged upload/);
	});

	it("fails loud on a nested or shallow key (not exactly <ws>/uploads/<digest>/<file>)", () => {
		expect(() =>
			contentKeyedSourceName(
				`s3://dataraum-lake/${WS}/uploads/${DIGEST}/a/b.csv`,
			),
		).toThrow(/must be a staged upload/);
		expect(() =>
			contentKeyedSourceName(`s3://dataraum-lake/${WS}/uploads/${DIGEST}`),
		).toThrow(/must be a staged upload/);
	});
});

describe("reservedSourceNamePrefix (DAT-433)", () => {
	it("flags each derived-table family prefix", () => {
		expect(reservedSourceNamePrefix("src_mydata")).toBe("src_");
		expect(reservedSourceNamePrefix("enriched_data")).toBe("enriched_");
	});

	it("passes bare words and near-misses (only the prefixed forms collide)", () => {
		expect(reservedSourceNamePrefix("src")).toBeNull();
		expect(reservedSourceNamePrefix("srcdata")).toBeNull();
		expect(reservedSourceNamePrefix("enriched")).toBeNull();
		expect(reservedSourceNamePrefix("slice")).toBeNull();
		expect(reservedSourceNamePrefix("finance_data")).toBeNull();
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

describe("uploadTableName (DAT-639)", () => {
	it("names a file's raw table after the sanitized stem (mirrors raw_table_name_for_uri)", () => {
		// The engine drops the `src_<digest>__` source prefix post-DAT-639: a CSV at
		// uploads/<digest>/Orders.CSV loads into the NARROW raw table `orders`.
		expect(
			uploadTableName(`s3://dataraum-lake/${WS}/uploads/${DIGEST}/Orders.CSV`),
		).toBe("orders");
	});

	it("sanitizes a dashed/odd filename sensibly (lowercase, underscores, letter-led)", () => {
		expect(
			uploadTableName(
				`s3://dataraum-lake/${WS}/uploads/${DIGEST}/Master TXN-sample.csv`,
			),
		).toBe("master_txn_sample");
		// A digit-led stem gets the `t_` lead-letter fix (sanitizeRecipeName rule).
		expect(
			uploadTableName(
				`s3://dataraum-lake/${WS}/uploads/${DIGEST}/2024_orders.parquet`,
			),
		).toBe("t_2024_orders");
	});

	it("keys on the filename, not the digest — two files with the same stem collide", () => {
		// The narrow-name guard's whole point: two distinct uploads whose stems match
		// resolve to ONE raw table name (the duplication DAT-639 catches).
		const a = uploadTableName(
			`s3://dataraum-lake/${WS}/uploads/${DIGEST}/orders.csv`,
		);
		const b = uploadTableName(
			"s3://dataraum-lake/ws2/uploads/0000000000000000000000000000000000000000/orders.csv",
		);
		expect(a).toBe("orders");
		expect(b).toBe("orders");
	});

	it("handles a stem with no extension (a dotless basename stays whole)", () => {
		expect(
			uploadTableName(`s3://dataraum-lake/${WS}/uploads/${DIGEST}/README`),
		).toBe("readme");
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

describe("recipeContentHash (DAT-430)", () => {
	const tables = [
		{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
		{ name: "customers", sql: 'SELECT * FROM "Customers"' },
	];

	it("is a deterministic sha256 hex over the canonical {backend, tables} JSON", () => {
		const h = recipeContentHash("mssql", tables);
		expect(h).toMatch(/^[0-9a-f]{64}$/);
		// Same backend + same pick → same hash: the property the engine's
		// idempotent-re-select skip relies on (current recipe_hash ==
		// imported_recipe_hash witness).
		expect(
			recipeContentHash(
				"mssql",
				tables.map((t) => ({ ...t })),
			),
		).toBe(h);
	});

	it("changes when the pick, the SQL, or the order changes", () => {
		const h = recipeContentHash("mssql", tables);
		expect(recipeContentHash("mssql", [tables[0]])).not.toBe(h); // dropped table
		expect(
			recipeContentHash("mssql", [
				tables[0],
				{ ...tables[1], sql: 'SELECT * FROM "Archive"."Customers"' },
			]),
		).not.toBe(h); // re-pointed SQL
		expect(recipeContentHash("mssql", [tables[1], tables[0]])).not.toBe(h); // order
	});

	it("changes when the SAME tables are picked against a DIFFERENT backend", () => {
		// The backend is part of the recipe identity: a re-select of the same
		// source name against another DBMS with identical table names must NOT
		// match the import witness — otherwise the engine would silently skip
		// over raw tables extracted from the old backend.
		const h = recipeContentHash("mssql", tables);
		expect(recipeContentHash("postgres", tables)).not.toBe(h);
	});

	it("folds credentialSource into the identity when present, leaving the table-pick hash byte-identical (DAT-592)", () => {
		// A query-source reads through a named connection: same SQL, different
		// connection = a different recipe, so re-pointing it must NOT match the
		// witness (else a presence-skip over stale raw tables).
		const withWwi = recipeContentHash("mssql", tables, "wwi");
		expect(withWwi).toMatch(/^[0-9a-f]{64}$/);
		expect(recipeContentHash("mssql", tables, "staging")).not.toBe(withWwi);
		// The table-pick path (no credentialSource arg) keeps the OLD canonical
		// {backend, tables} hash — existing import witnesses stay valid, and it is
		// distinct from any credential-qualified hash.
		expect(recipeContentHash("mssql", tables)).not.toBe(withWwi);
	});
});
