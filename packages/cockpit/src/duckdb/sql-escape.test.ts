import { describe, expect, it } from "vitest";

import {
	buildDucklakeAttachSql,
	ducklakeMetadataSchemaFor,
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

describe("escapeSqlLiteral (DAT-386 — DuckDB doubles quotes, backslash is literal)", () => {
	it("doubles single quotes and leaves backslashes untouched", () => {
		// DuckDB does not honour `\'`; the only escape is `''`. A backslash is an
		// ordinary character, so it must NOT be doubled (over-escaping would change
		// the value the reader/ATTACH sees).
		expect(escapeSqlLiteral("/var/li'b\\lake")).toBe("/var/li''b\\lake");
	});

	it("neutralizes a quote-break injection by doubling the quote", () => {
		// The Finding-1 attacker: a `'` that would close the literal and let the
		// trailing SQL execute. Doubling turns it into data, not a delimiter.
		expect(
			escapeSqlLiteral(
				"s3://dataraum-lake/a.csv') UNION ALL SELECT version() -- ",
			),
		).toBe("s3://dataraum-lake/a.csv'') UNION ALL SELECT version() -- ");
	});

	it("leaves a clean path untouched", () => {
		expect(escapeSqlLiteral("/var/lib/dataraum/lake")).toBe(
			"/var/lib/dataraum/lake",
		);
	});
});

describe("ducklakeMetadataSchemaFor (DAT-815)", () => {
	it("derives ws_<id> with dashes as underscores, mirroring the engine", () => {
		// Must agree with the engine's `schema_name_for` (server/workspace.py):
		// the engine's writer ATTACH names this exact schema, and a divergent
		// derivation would point the cockpit reader at a different catalog.
		expect(
			ducklakeMetadataSchemaFor("00000000-0000-0000-0000-000000000001"),
		).toBe("ws_00000000_0000_0000_0000_000000000001");
	});

	it("keeps a dash-free id verbatim under the ws_ prefix", () => {
		expect(ducklakeMetadataSchemaFor("test")).toBe("ws_test");
	});
});

describe("buildDucklakeAttachSql (DAT-367 escaping fix)", () => {
	it("builds a well-formed ATTACH for clean credentials", () => {
		expect(
			buildDucklakeAttachSql(
				"lake",
				"postgresql://dataraum:dataraum@postgres:5432/lake_catalog",
				"s3://dataraum-lake/lake",
				"ws_00000000_0000_0000_0000_000000000001",
			),
		).toBe(
			"ATTACH 'ducklake:postgres:dbname=lake_catalog host=postgres port=5432 " +
				"user=dataraum password=dataraum' AS lake " +
				"(DATA_PATH 's3://dataraum-lake/lake', " +
				"METADATA_SCHEMA 'ws_00000000_0000_0000_0000_000000000001', READ_ONLY)",
		);
	});

	it("escapes the libpq single-quotes a spaced password introduces", () => {
		// pgUrlToLibpq wraps a spaced password as password='pa ss'; those inner
		// quotes must be DOUBLED so they don't close the outer SQL literal. DuckDB
		// collapses each `''` back to one `'` before the postgres connector sees
		// it. (The previous code interpolated the libpq string raw → broken.)
		const sql = buildDucklakeAttachSql(
			"lake",
			"postgresql://u:pa%20ss@h:5432/db",
			"/data",
			"ws_test",
		);
		expect(sql).toContain("password=''pa ss''");
		// The outer literal stays balanced: exactly one unescaped opening quote
		// after ATTACH and the only unescaped quotes wrap the literals.
		expect(sql.startsWith("ATTACH 'ducklake:postgres:")).toBe(true);
		expect(sql).toContain(
			"' AS lake (DATA_PATH '/data', METADATA_SCHEMA 'ws_test', READ_ONLY)",
		);
	});

	it("escapes a quote in the data path by doubling it; backslash stays literal", () => {
		const sql = buildDucklakeAttachSql(
			"lake",
			"postgresql://u:p@h:5432/db",
			"/da'ta\\x",
			"ws_test",
		);
		expect(sql).toContain(
			"(DATA_PATH '/da''ta\\x', METADATA_SCHEMA 'ws_test', READ_ONLY)",
		);
	});

	it("escapes a quote in the metadata schema by doubling it", () => {
		// The schema comes from a derivation over the workspace id, but the
		// builder must not rely on that: it is the escaping seam.
		const sql = buildDucklakeAttachSql(
			"lake",
			"postgresql://u:p@h:5432/db",
			"/data",
			"ws'x",
		);
		expect(sql).toContain("METADATA_SCHEMA 'ws''x'");
	});
});
