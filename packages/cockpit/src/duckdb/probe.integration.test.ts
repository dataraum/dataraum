// Real in-process DuckDB integration for probe (DAT-367).
//
// Exercises the full probe path against a real sqlite source: credential
// resolution (DATARAUM_<NAME>_URL) → INSTALL/LOAD sqlite → ATTACH READ_ONLY →
// USE → wrapped SELECT → DETACH → JSON result. sqlite is the cheapest of the
// supported backends to stand up hermetically (a file, no server); the ATTACH
// machinery is shared across all four backends.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { probe } from "./probe";

let dir: string;
let dbfile: string;

beforeAll(async () => {
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
