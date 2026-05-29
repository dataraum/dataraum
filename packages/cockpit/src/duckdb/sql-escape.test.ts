import { describe, expect, it } from "vitest";

import {
	buildDucklakeAttachSql,
	escapeSqlLiteral,
	pgUrlToLibpq,
} from "./sql-escape";

// These two helpers are the load-bearing string surgery for the DuckLake
// ATTACH; they mirror the engine's `_pg_url_to_libpq` / `_escape_sql_literal`
// and must agree with it (both sides attach the same catalog).

describe("pgUrlToLibpq (DAT-367)", () => {
	it("converts a full postgresql URL to libpq keyword-value form", () => {
		expect(
			pgUrlToLibpq("postgresql://dataraum:secret@postgres:5432/lake_catalog"),
		).toBe(
			"dbname=lake_catalog host=postgres port=5432 user=dataraum password=secret",
		);
	});

	it("percent-decodes credentials (no quoting needed for @ or /)", () => {
		// libpq only requires single-quoting for whitespace / quote / backslash;
		// `@` and `/` pass through bare.
		expect(pgUrlToLibpq("postgresql://u%40corp:p%2Fw@h:5432/db")).toBe(
			"dbname=db host=h port=5432 user=u@corp password=p/w",
		);
	});

	it("single-quotes and escapes a password containing whitespace", () => {
		const out = pgUrlToLibpq("postgresql://u:pa%20ss@h:5432/db");
		expect(out).toContain("password='pa ss'");
	});

	it("omits absent parts (no port, no password)", () => {
		expect(pgUrlToLibpq("postgresql://u@h/db")).toBe("dbname=db host=h user=u");
	});
});

describe("escapeSqlLiteral (DAT-367)", () => {
	it("escapes single quotes and backslashes", () => {
		expect(escapeSqlLiteral("/var/li'b\\lake")).toBe("/var/li\\'b\\\\lake");
	});

	it("leaves a clean path untouched", () => {
		expect(escapeSqlLiteral("/var/lib/dataraum/lake")).toBe(
			"/var/lib/dataraum/lake",
		);
	});
});

describe("buildDucklakeAttachSql (DAT-367 escaping fix)", () => {
	it("builds a well-formed ATTACH for clean credentials", () => {
		expect(
			buildDucklakeAttachSql(
				"lake",
				"postgresql://dataraum:dataraum@postgres:5432/lake_catalog",
				"/var/lib/dataraum/lake",
			),
		).toBe(
			"ATTACH 'ducklake:postgres:dbname=lake_catalog host=postgres port=5432 " +
				"user=dataraum password=dataraum' AS lake " +
				"(DATA_PATH '/var/lib/dataraum/lake', READ_ONLY)",
		);
	});

	it("escapes the libpq single-quotes a spaced password introduces", () => {
		// pgUrlToLibpq wraps a spaced password as password='pa ss'; those inner
		// quotes must be backslash-escaped so they don't close the outer SQL
		// literal. The previous code interpolated the libpq string raw → broken.
		const sql = buildDucklakeAttachSql(
			"lake",
			"postgresql://u:pa%20ss@h:5432/db",
			"/data",
		);
		expect(sql).toContain("password=\\'pa ss\\'");
		// The outer literal stays balanced: exactly one unescaped opening quote
		// after ATTACH and the only unescaped quotes wrap the two literals.
		expect(sql.startsWith("ATTACH 'ducklake:postgres:")).toBe(true);
		expect(sql).toContain("' AS lake (DATA_PATH '/data', READ_ONLY)");
	});

	it("escapes a quote/backslash in the data path", () => {
		const sql = buildDucklakeAttachSql(
			"lake",
			"postgresql://u:p@h:5432/db",
			"/da'ta\\x",
		);
		expect(sql).toContain("(DATA_PATH '/da\\'ta\\\\x', READ_ONLY)");
	});
});
