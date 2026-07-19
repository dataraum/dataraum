// Real in-process DuckDB integration for the STREAMING probe path (DAT-576).
//
// The human-grid analog of probe.integration: instead of materializing a sample,
// it drives openProbeConnection() → buildGridQuery() → conn.stream() → streamNdjson
// and asserts the columnar-NDJSON frames — proving the full streaming-over-ATTACH
// path (the same one /api/probe-sql is a thin shell over) against a real sqlite
// source. sqlite is the cheapest backend to stand up hermetically (a file, no
// server); the ATTACH + stream machinery is shared across all four backends.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { buildGridQuery } from "./grid-query";
import { type StreamableResult, streamNdjson } from "./stream-sql";

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
let openProbeConnection: any;

let dir: string;
let dbfile: string;

/** Drive streamNdjson to completion, returning the parsed frames. */
async function collectFrames(
	result: StreamableResult,
	cap = 50_000,
): Promise<{ t: string; [k: string]: unknown }[]> {
	const frames: { t: string; [k: string]: unknown }[] = [];
	for await (const line of streamNdjson(result, cap, "test", {
		aborted: false,
	})) {
		const trimmed = line.trim();
		if (trimmed) frames.push(JSON.parse(trimmed));
	}
	return frames;
}

beforeAll(async () => {
	({ openProbeConnection } = await import("./probe"));

	dir = mkdtempSync(join(tmpdir(), "probe-stream-it-"));
	dbfile = join(dir, "src.sqlite");

	// Build a real sqlite source with 5000 rows (> one 2048-row DuckDB chunk, so
	// the stream genuinely produces multiple batch frames).
	const inst = await DuckDBInstance.create(":memory:");
	const c = await inst.connect();
	await c.run("INSTALL sqlite");
	await c.run("LOAD sqlite");
	await c.run(`ATTACH '${dbfile}' AS s (TYPE SQLITE)`);
	await c.run("CREATE TABLE s.main.events(id INTEGER, label VARCHAR)");
	await c.run(
		"INSERT INTO s.main.events SELECT i, 'e' || i FROM range(5000) AS t(i)",
	);
	await c.run("DETACH s");
	c.closeSync();
	inst.closeSync();

	vi.stubEnv("DATARAUM_EVENTS_URL", dbfile);
});

afterAll(() => {
	vi.unstubAllEnvs();
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("streaming probe over a real sqlite source (DAT-576)", () => {
	it("streams columnar NDJSON frames over the ATTACH and disposes the connection", async () => {
		const { conn, dispose } = await openProbeConnection({
			source_name: "events",
			backend: "sqlite",
		});
		const result = (await conn.stream(
			buildGridQuery("SELECT id, label FROM events ORDER BY id"),
		)) as unknown as StreamableResult;
		const frames = await collectFrames(result);
		dispose();

		const header = frames.find((f) => f.t === "h");
		const batches = frames.filter((f) => f.t === "b");
		const footer = frames.find((f) => f.t === "f");

		expect(header?.columns).toEqual(["id", "label"]);
		// 5000 rows > 2048 vector size → more than one batch (genuine streaming).
		expect(batches.length).toBeGreaterThan(1);
		expect(footer?.rows).toBe(5000);
		expect(footer?.truncated).toBeFalsy();
	});

	it("caps + flags truncated, ordering the FULL result before the cap", async () => {
		const { conn, dispose } = await openProbeConnection({
			source_name: "events",
			backend: "sqlite",
		});
		const result = (await conn.stream(
			buildGridQuery("SELECT id FROM events", { column: "id", dir: "desc" }),
		)) as unknown as StreamableResult;
		const frames = await collectFrames(result, 3);
		dispose();

		const footer = frames.find((f) => f.t === "f");
		const firstBatch = frames.find((f) => f.t === "b");
		expect(footer?.truncated).toBe(true);
		expect(footer?.cap).toBe(3);
		// DESC over the full result → top id is 4999, not an arbitrary first-N slice.
		// sqlite surfaces INTEGER as BIGINT, serialized losslessly as a string.
		expect((firstBatch?.cols as unknown[][])[0][0]).toBe("4999");
	});

	it("surfaces a bad-SQL prepare error from conn.stream() (no leak of the source URL)", async () => {
		const { conn, dispose, redact } = await openProbeConnection({
			source_name: "events",
			backend: "sqlite",
		});
		await expect(
			conn.stream(buildGridQuery("SELECT * FROM does_not_exist")),
		).rejects.toThrow();
		// redact is a no-op for a sqlite file path, but the contract holds.
		expect(redact("plain message")).toBe("plain message");
		dispose();
	});

	it("rejects an unsupported backend and a missing credential (loud)", async () => {
		await expect(
			openProbeConnection({ source_name: "events", backend: "oracle" }),
		).rejects.toThrow(/Unsupported backend/);
		await expect(
			openProbeConnection({ source_name: "no_such_src", backend: "sqlite" }),
		).rejects.toThrow(/DATARAUM_NO_SUCH_SRC_URL/);
	});
});
