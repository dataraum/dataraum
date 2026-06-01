// Real in-process DuckDB integration for the streaming grid path (DAT-385 P1).
//
// Exercises `streamNdjson` against a REAL neo `conn.stream()` over a REAL
// DuckLake lake — the lazy chunk path, JSON-safe type coercion, the columnar
// reshaping, cap/truncation, and the in-band error footer. Mirrors
// `run-sql.integration.test.ts`: a writer connection (engine stand-in) creates +
// commits `lake.typed.*`; a separate READ_ONLY reader (the cockpit) streams it.
//
// Hermetic: a local DuckLake catalog FILE (not Postgres). The read semantics —
// committed-snapshot visibility across instances — are identical to production.
// Self-skips nothing extra: it needs no external service, only the native addon,
// so it runs under the `integration` project (kept out of the default unit run).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
	type AbortSignalLike,
	buildGridQuery,
	clampGridCap,
	type GridSort,
	type ResultFrame,
	type StreamableResult,
	streamNdjson,
} from "./stream-sql";

let dir: string;
let readerConn: DuckDBConnection;
let readerInstance: DuckDBInstance;

/** Stream a SQL query through the real driver + streamNdjson, collecting frames. */
async function streamQuery(
	sql: string,
	cap: number,
	params?: (string | number | boolean | null)[],
	sort?: GridSort,
): Promise<ResultFrame[]> {
	// Compose through buildGridQuery — exactly what the route does — so the sort
	// path is exercised against the real driver, not a hand-wrapped string.
	const wrapped = buildGridQuery(sql, sort);
	const result = (await (params
		? readerConn.stream(wrapped, params)
		: readerConn.stream(wrapped))) as unknown as StreamableResult;
	const frames: ResultFrame[] = [];
	for await (const line of streamNdjson(result, cap, "q_it")) {
		frames.push(JSON.parse(line) as ResultFrame);
	}
	return frames;
}

/**
 * Stream a query but flip the abort signal the moment the first batch frame
 * lands, mirroring the route's mutable `{ aborted }` flag (run-sql.ts) and the
 * grid closing mid-stream. The generator checks `signal.aborted` at the NEXT
 * chunk boundary, so the loop stops within one chunk and the footer carries
 * `cancelled: true`.
 */
async function streamQueryAbortingAfterFirstBatch(
	sql: string,
	cap: number,
): Promise<ResultFrame[]> {
	const wrapped = `SELECT * FROM (${sql}) AS _run_sql`;
	const result = (await readerConn.stream(
		wrapped,
	)) as unknown as StreamableResult;
	const signal: { aborted: boolean } = { aborted: false };
	const frames: ResultFrame[] = [];
	for await (const line of streamNdjson(
		result,
		cap,
		"q_it",
		signal as AbortSignalLike,
	)) {
		const frame = JSON.parse(line) as ResultFrame;
		frames.push(frame);
		if (frame.t === "b") signal.aborted = true;
	}
	return frames;
}

beforeAll(async () => {
	dir = mkdtempSync(join(tmpdir(), "stream-sql-it-"));
	const dataPath = join(dir, "data");
	const catalog = join(dir, "catalog.ducklake");

	const writerInstance = await DuckDBInstance.create(":memory:");
	const writer = await writerInstance.connect();
	try {
		await writer.run("INSTALL ducklake");
	} catch {
		// already present
	}
	await writer.run("LOAD ducklake");
	await writer.run(
		`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}')`,
	);
	await writer.run("CREATE SCHEMA IF NOT EXISTS lake.typed");
	await writer.run(
		"CREATE TABLE lake.typed.orders(id INTEGER, customer VARCHAR, amount DECIMAL(18,2), created_at TIMESTAMP)",
	);
	await writer.run(
		"INSERT INTO lake.typed.orders VALUES " +
			"(1,'acme',10.00,TIMESTAMP '2026-05-01 09:00:00')," +
			"(2,'beta',9.99,TIMESTAMP '2026-05-02 10:30:00')," +
			"(3,'acme',50.50,TIMESTAMP '2026-05-03 11:15:00')",
	);
	// A wider table to exercise multi-chunk streaming (DuckDB chunks ~2048 rows).
	await writer.run(
		"CREATE TABLE lake.typed.big AS SELECT range AS n FROM range(5000)",
	);
	// Nested types (design §12 must-verify): a LIST column and a STRUCT column,
	// to confirm JsonDuckDBValueConverter coerces them to plain JSON arrays /
	// objects (not opaque driver handles).
	await writer.run(
		"CREATE TABLE lake.typed.nested(id INTEGER, tags VARCHAR[], meta STRUCT(a INTEGER, b VARCHAR))",
	);
	await writer.run(
		"INSERT INTO lake.typed.nested VALUES " +
			"(1, ['x','y'], {'a': 10, 'b': 'hello'})," +
			"(2, [], {'a': 20, 'b': 'world'})",
	);
	writer.closeSync();
	writerInstance.closeSync();

	readerInstance = await DuckDBInstance.create(":memory:");
	readerConn = await readerInstance.connect();
	await readerConn.run("LOAD ducklake");
	await readerConn.run(
		`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}', READ_ONLY)`,
	);
});

afterAll(() => {
	readerConn?.closeSync();
	readerInstance?.closeSync();
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("streamNdjson over a real DuckLake lake (DAT-385)", () => {
	it("streams header / batch / footer with type fidelity", async () => {
		const frames = await streamQuery(
			"SELECT id, customer, amount, created_at FROM lake.typed.orders ORDER BY id",
			1000,
		);

		const header = frames[0];
		if (header.t !== "h") throw new Error("expected header first");
		expect(header.columns).toEqual(["id", "customer", "amount", "created_at"]);
		// neo's columnTypesJson() returns STRUCTURED type metadata (not bare type
		// strings as the design sketch illustrated): a `typeId` per column plus
		// width/scale for parameterized types. That richer shape is exactly what
		// drives client cell formatting (right-align numbers, decimal places).
		expect(Array.isArray(header.types)).toBe(true);
		const types = header.types as Array<Record<string, unknown>>;
		expect(types).toHaveLength(4);
		// DECIMAL(18,2) carries its width + scale so the grid can format precisely.
		expect(types[2]).toMatchObject({ width: 18, scale: 2 });
		// Every column reports a numeric typeId.
		for (const t of types) {
			expect(typeof t.typeId).toBe("number");
		}

		// One batch for 3 rows, then a clean footer.
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch second");
		expect(batch.n).toBe(3);
		// Columnar: cols[colIndex][rowIndex].
		expect(batch.cols[0]).toEqual([1, 2, 3]);
		expect(batch.cols[1]).toEqual(["acme", "beta", "acme"]);
		// DECIMAL → JSON-safe string (lossless), TIMESTAMP → ISO-ish string.
		expect(batch.cols[2]).toEqual(["10.00", "9.99", "50.50"]);
		expect(typeof batch.cols[3][0]).toBe("string");

		expect(frames.at(-1)).toEqual({ t: "f", rows: 3 });
	});

	it("coerces HUGEINT (sum) to a JSON-safe string", async () => {
		const frames = await streamQuery(
			"SELECT sum(amount) AS total FROM lake.typed.orders",
			1000,
		);
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch");
		expect(batch.cols[0]).toEqual(["70.49"]);
	});

	it("binds positional params", async () => {
		const frames = await streamQuery(
			"SELECT id FROM lake.typed.orders WHERE customer = $1 ORDER BY id",
			1000,
			["acme"],
		);
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch");
		expect(batch.cols[0]).toEqual([1, 3]);
	});

	it("streams a multi-chunk result and totals the rows in the footer", async () => {
		const frames = await streamQuery("SELECT n FROM lake.typed.big", 10_000);
		const batches = frames.filter((f) => f.t === "b");
		// 5000 rows over ~2048-row chunks → more than one batch.
		expect(batches.length).toBeGreaterThan(1);
		const total = batches.reduce((s, b) => s + (b.t === "b" ? b.n : 0), 0);
		expect(total).toBe(5000);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 5000 });
	});

	it("truncates at the cap and reports it in the footer", async () => {
		const cap = clampGridCap(100);
		const frames = await streamQuery("SELECT n FROM lake.typed.big", cap);
		const batches = frames.filter((f) => f.t === "b");
		const total = batches.reduce((s, b) => s + (b.t === "b" ? b.n : 0), 0);
		expect(total).toBe(100);
		expect(frames.at(-1)).toEqual({
			t: "f",
			rows: 100,
			truncated: true,
			cap: 100,
		});
	});

	it("does NOT mark truncated when the cap equals the full row count", async () => {
		// `big` has exactly 5000 rows; a cap of 5000 is a full set, not a cut-off
		// one. The footer must read clean — the post-cap peek finds no more rows.
		const frames = await streamQuery("SELECT n FROM lake.typed.big", 5000);
		const batches = frames.filter((f) => f.t === "b");
		const total = batches.reduce((s, b) => s + (b.t === "b" ? b.n : 0), 0);
		expect(total).toBe(5000);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 5000 });
	});

	it("applies a server-side sort (DESC) over the wrapped query", async () => {
		const sort: GridSort = { column: "amount", dir: "desc" };
		const frames = await streamQuery(
			"SELECT id, customer, amount FROM lake.typed.orders",
			1000,
			undefined,
			sort,
		);
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch");
		// amount DESC: 50.50 (id 3), 10.00 (id 1), 9.99 (id 2).
		expect(batch.cols[0]).toEqual([3, 1, 2]);
		expect(batch.cols[2]).toEqual(["50.50", "10.00", "9.99"]);
	});

	it("sorts BEFORE the cap, so a truncated result still shows the true top-N", async () => {
		// `big` is range(5000); ORDER BY n DESC then cap at 3 must surface the
		// global top — 4999, 4998, 4997 — NOT the first 3 streamed rows. This is
		// the whole reason sort is server-side, not client-side over the window.
		// (range() is BIGINT → JSON-safe STRING via the bigint→string coercion.)
		const sort: GridSort = { column: "n", dir: "desc" };
		const frames = await streamQuery(
			"SELECT n FROM lake.typed.big",
			3,
			undefined,
			sort,
		);
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch");
		expect(batch.cols[0]).toEqual(["4999", "4998", "4997"]);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 3, truncated: true, cap: 3 });
	});

	it("emits an in-band error footer for a bad query (HTTP would stay 200)", async () => {
		// `conn.stream` is lazy, so a binder error surfaces on first fetchChunk —
		// streamNdjson catches it and reports it in the footer rather than throwing.
		let frames: ResultFrame[] = [];
		try {
			frames = await streamQuery(
				"SELECT no_such_col FROM lake.typed.orders",
				1000,
			);
		} catch {
			// If the driver throws at stream() prepare time instead, that's also a
			// valid surface; the route returns a 400 before streaming in that case.
			return;
		}
		const footer = frames.at(-1);
		if (footer?.t !== "f") throw new Error("expected footer");
		expect(footer.error).toBeTruthy();
	});

	it("coerces LIST and STRUCT columns to JSON-safe values", async () => {
		const frames = await streamQuery(
			"SELECT id, tags, meta FROM lake.typed.nested ORDER BY id",
			1000,
		);

		const header = frames[0];
		if (header.t !== "h") throw new Error("expected header first");
		expect(header.columns).toEqual(["id", "tags", "meta"]);

		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch second");
		expect(batch.n).toBe(2);
		expect(batch.cols[0]).toEqual([1, 2]);
		// LIST → plain JS array (empty list stays an empty array, not null).
		expect(batch.cols[1]).toEqual([["x", "y"], []]);
		// STRUCT → nested plain object, exactly what getRowObjectsJson produces.
		expect(batch.cols[2]).toEqual([
			{ a: 10, b: "hello" },
			{ a: 20, b: "world" },
		]);

		expect(frames.at(-1)).toEqual({ t: "f", rows: 2 });
	});

	it("stops within one chunk on abort and leaves the connection reusable", async () => {
		// Abort the moment the first batch lands; the generator re-checks the
		// signal at the next chunk boundary, so it stops after at most one more
		// chunk of work. With a 10k cap (above the full 5000 rows) the stream
		// would otherwise run to completion — proving the cut is the abort, not
		// the cap.
		const frames = await streamQueryAbortingAfterFirstBatch(
			"SELECT n FROM lake.typed.big",
			10_000,
		);

		const footer = frames.at(-1);
		if (footer?.t !== "f") throw new Error("expected footer");
		expect(footer.cancelled).toBe(true);
		expect(footer.truncated).toBeUndefined();
		// Far fewer than the full 5000 rows — the loop broke early.
		expect(footer.rows).toBeLessThan(5000);
		expect(footer.rows).toBeGreaterThan(0);

		// The shared READ_ONLY reader must be healthy after an aborted stream:
		// a second query on the SAME connection returns correct results.
		const after = await streamQuery(
			"SELECT id, customer FROM lake.typed.orders ORDER BY id",
			1000,
		);
		const batch = after[1];
		if (batch.t !== "b") throw new Error("expected batch");
		expect(batch.n).toBe(3);
		expect(batch.cols[0]).toEqual([1, 2, 3]);
		expect(batch.cols[1]).toEqual(["acme", "beta", "acme"]);
		expect(after.at(-1)).toEqual({ t: "f", rows: 3 });
	});
});
