import { describe, expect, it } from "vitest";

import { escapeSqlLiteral, pgUrlToLibpq } from "./sql-escape";

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
