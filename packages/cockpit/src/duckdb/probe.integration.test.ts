// Real in-process DuckDB integration for probe (DAT-367).
//
// Exercises the full probe path against a real sqlite source: credential
// resolution (DATARAUM_<NAME>_URL) → INSTALL/LOAD sqlite → ATTACH READ_ONLY →
// USE → wrapped SELECT → DETACH → JSON result. sqlite is the cheapest of the
// supported backends to stand up hermetically (a file, no server); the ATTACH
// machinery is shared across all four backends.
//
// Importing probe boots config.ts (the extension-cache contract), so we stub
// the required env before the dynamic import — same approach as
// connect.integration. The values are placeholders; nothing here touches them.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

const REQUIRED_DEFAULTS: Record<string, string> = {
	COCKPIT_DATABASE_URL:
		process.env.COCKPIT_DATABASE_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/cockpit_db",
	METADATA_DATABASE_URL:
		process.env.METADATA_DATABASE_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/dataraum",
	// Config-parse placeholder only — these suites never touch the metadata
	// write surface (DAT-816 role split).
	METADATA_WRITER_DATABASE_URL:
		process.env.METADATA_WRITER_DATABASE_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/dataraum",
	DATARAUM_WORKSPACE_ID:
		process.env.DATARAUM_WORKSPACE_ID ?? "00000000-0000-0000-0000-000000000001",
	DATARAUM_CONFIG_PATH:
		process.env.DATARAUM_CONFIG_PATH ?? "/opt/dataraum/config",
	DATARAUM_LAKE_PATH:
		process.env.DATARAUM_LAKE_PATH ?? "s3://dataraum-lake/lake",
	DUCKLAKE_CATALOG_URL:
		process.env.DUCKLAKE_CATALOG_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
	ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
	S3_ENDPOINT: process.env.S3_ENDPOINT ?? "127.0.0.1:8333",
	S3_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID ?? "dataraum",
	S3_SECRET_ACCESS_KEY:
		process.env.S3_SECRET_ACCESS_KEY ?? "dataraum-s3-secret",
	S3_BUCKET: process.env.S3_BUCKET ?? "dataraum-lake",
};
for (const [k, v] of Object.entries(REQUIRED_DEFAULTS)) {
	if (!process.env[k]) process.env[k] = v;
}

// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shape
let probe: any;

let dir: string;
let dbfile: string;

beforeAll(async () => {
	// Dynamic import so the env stub above is in place before config.ts loads.
	({ probe } = await import("./probe"));

	dir = mkdtempSync(join(tmpdir(), "probe-it-"));
	dbfile = join(dir, "src.sqlite");

	// Build a real sqlite source file with the sqlite extension.
	const inst = await DuckDBInstance.create(":memory:");
	const c = await inst.connect();
	await c.run("INSTALL sqlite");
	await c.run("LOAD sqlite");
	await c.run(`ATTACH '${dbfile}' AS s (TYPE SQLITE)`);
	await c.run("CREATE TABLE s.main.widgets(id INTEGER, name VARCHAR)");
	await c.run(
		"INSERT INTO s.main.widgets VALUES (1,'gear'),(2,'cog'),(3,'bolt')",
	);
	await c.run("DETACH s");
	c.closeSync();
	inst.closeSync();

	vi.stubEnv("DATARAUM_WIDGETS_URL", dbfile);
});

afterAll(() => {
	vi.unstubAllEnvs();
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("probe against a real sqlite source (DAT-367)", () => {
	it("resolves credentials by name, attaches READ_ONLY, returns rows", async () => {
		const result = await probe({
			source_name: "widgets",
			backend: "sqlite",
			sql: "SELECT id, name FROM widgets ORDER BY id",
		});
		expect(result.columns).toEqual(["id", "name"]);
		// The sqlite extension surfaces INTEGER columns as BIGINT; getRowObjectsJson
		// serializes BIGINT losslessly as a string ("1"), not a JS number.
		expect(result.rows).toEqual([
			{ id: "1", name: "gear" },
			{ id: "2", name: "cog" },
			{ id: "3", name: "bolt" },
		]);
	});

	it("honors the limit cap", async () => {
		const result = await probe({
			source_name: "widgets",
			backend: "sqlite",
			sql: "SELECT id FROM widgets ORDER BY id",
			limit: 1,
		});
		expect(result.rowCount).toBe(1);
	});

	it("fails loud when no credential is set for the source", async () => {
		await expect(
			probe({ source_name: "unknown_src", backend: "sqlite", sql: "SELECT 1" }),
		).rejects.toThrow(/DATARAUM_UNKNOWN_SRC_URL/);
	});

	it("rejects an unsupported backend", async () => {
		await expect(
			probe({ source_name: "widgets", backend: "oracle", sql: "SELECT 1" }),
		).rejects.toThrow(/Unsupported backend/);
	});
});
