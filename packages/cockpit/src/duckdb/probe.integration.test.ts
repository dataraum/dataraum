// Real in-process DuckDB integration for the probe ATTACH path (DAT-367).
//
// Exercises the full path against a real sqlite source: credential resolution
// (DATARAUM_<NAME>_URL) → INSTALL/LOAD sqlite → ATTACH READ_ONLY → USE →
// DESCRIBE + bounded sample → DETACH → JSON result. sqlite is the cheapest of
// the supported backends to stand up hermetically (a file, no server); the
// ATTACH machinery below `probeDescribe` is shared by `openProbeConnection`,
// which `/api/probe-sql` streams from, and is the same machinery the deleted
// `probe()` used (DAT-671 R6).
//
// Importing the module boots config.ts (the extension-cache contract), so we
// stub the required env before the dynamic import — same approach as
// connect.integration. The values are placeholders; nothing here touches them.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { applyIntegrationEnv } from "#/test/integration-env";

applyIntegrationEnv();

// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shape
let probeDescribe: any;

let dir: string;
let dbfile: string;

beforeAll(async () => {
	// Dynamic import so the env stub above is in place before config.ts loads.
	({ probeDescribe } = await import("./probe"));

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

describe("probeDescribe against a real sqlite source (DAT-367)", () => {
	it("resolves credentials by name, attaches READ_ONLY, returns columns + rows", async () => {
		const result = await probeDescribe({
			source_name: "widgets",
			backend: "sqlite",
			sql: "SELECT id, name FROM widgets ORDER BY id",
		});
		// The sqlite extension surfaces INTEGER columns as BIGINT.
		expect(result.columns).toEqual([
			{ name: "id", type: "BIGINT" },
			{ name: "name", type: "VARCHAR" },
		]);
		// getRowObjectsJson serializes BIGINT losslessly as a string ("1"), not a
		// JS number.
		expect(result.sampleRows).toEqual([
			{ id: "1", name: "gear" },
			{ id: "2", name: "cog" },
			{ id: "3", name: "bolt" },
		]);
	});

	it("honors the limit cap on the sample", async () => {
		const result = await probeDescribe({
			source_name: "widgets",
			backend: "sqlite",
			sql: "SELECT id FROM widgets ORDER BY id",
			limit: 1,
		});
		expect(result.sampleRows).toHaveLength(1);
	});

	it("fails loud when no credential is set for the source", async () => {
		await expect(
			probeDescribe({
				source_name: "unknown_src",
				backend: "sqlite",
				sql: "SELECT 1",
			}),
		).rejects.toThrow(/DATARAUM_UNKNOWN_SRC_URL/);
	});

	it("rejects an unsupported backend", async () => {
		await expect(
			probeDescribe({
				source_name: "widgets",
				backend: "oracle",
				sql: "SELECT 1",
			}),
		).rejects.toThrow(/Unsupported backend/);
	});
});
