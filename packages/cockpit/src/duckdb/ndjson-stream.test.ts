// Unit tests for the client NDJSON reader + columnar store (DAT-385 P2).
//
// Pure: no DOM, no DB, no native driver — feed hand-built ReadableStreams of
// byte chunks (deliberately split across frame and line boundaries) and assert
// the frame sequence + the folded ColumnStore. The server contract these mirror
// is unit-tested separately in stream-sql.test.ts.

import { describe, expect, it } from "vitest";

import type { ResultFrame } from "#/duckdb/stream-sql";
import {
	ColumnStore,
	readNdjsonIntoStore,
	readNdjsonStream,
} from "./ndjson-stream";

/** A ReadableStream<Uint8Array> emitting exactly these UTF-8 chunks, in order. */
function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
	const enc = new TextEncoder();
	return new ReadableStream({
		start(controller) {
			for (const c of chunks) controller.enqueue(enc.encode(c));
			controller.close();
		},
	});
}

const HEADER: ResultFrame = {
	t: "h",
	columns: ["id", "name"],
	types: [{ typeId: 1 }, { typeId: 2 }],
	queryId: "q_1",
};
const BATCH1: ResultFrame = {
	t: "b",
	n: 2,
	cols: [
		[1, 2],
		["a", "b"],
	],
};
const BATCH2: ResultFrame = { t: "b", n: 1, cols: [[3], ["c"]] };

describe("readNdjsonStream", () => {
	it("emits frames in order from whole lines", async () => {
		const frames: ResultFrame[] = [];
		await readNdjsonStream(
			streamOf([
				`${JSON.stringify(HEADER)}\n${JSON.stringify(BATCH1)}\n${JSON.stringify({ t: "f", rows: 2 })}\n`,
			]),
			(f) => frames.push(f),
		);
		expect(frames.map((f) => f.t)).toEqual(["h", "b", "f"]);
	});

	it("reassembles a frame split across reads (mid-line boundary)", async () => {
		const line = JSON.stringify(BATCH1);
		const mid = Math.floor(line.length / 2);
		const frames: ResultFrame[] = [];
		await readNdjsonStream(
			streamOf([
				`${JSON.stringify(HEADER)}\n${line.slice(0, mid)}`,
				`${line.slice(mid)}\n${JSON.stringify({ t: "f", rows: 2 })}\n`,
			]),
			(f) => frames.push(f),
		);
		expect(frames).toEqual([HEADER, BATCH1, { t: "f", rows: 2 }]);
	});

	it("handles several frames arriving in one read", async () => {
		const frames: ResultFrame[] = [];
		await readNdjsonStream(
			streamOf([
				`${JSON.stringify(HEADER)}\n${JSON.stringify(BATCH1)}\n${JSON.stringify(BATCH2)}\n${JSON.stringify({ t: "f", rows: 3 })}\n`,
			]),
			(f) => frames.push(f),
		);
		expect(frames.map((f) => f.t)).toEqual(["h", "b", "b", "f"]);
	});

	it("flushes a trailing frame with no final newline", async () => {
		const frames: ResultFrame[] = [];
		await readNdjsonStream(
			streamOf([
				`${JSON.stringify(HEADER)}\n`,
				JSON.stringify({ t: "f", rows: 0 }),
			]),
			(f) => frames.push(f),
		);
		expect(frames.map((f) => f.t)).toEqual(["h", "f"]);
	});
});

describe("ColumnStore", () => {
	it("seeds columns from the header and stays streaming", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		expect(s.columns).toEqual(["id", "name"]);
		expect(s.queryId).toBe("q_1");
		expect(s.cols).toEqual([[], []]);
		expect(s.status).toBe("streaming");
	});

	it("accumulates batches column-major across multiple chunks", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		s.apply(BATCH1);
		s.apply(BATCH2);
		expect(s.cols).toEqual([
			[1, 2, 3],
			["a", "b", "c"],
		]);
		expect(s.rowCount).toBe(3);
		// Columnar read — the accessorFn path the grid uses.
		expect(s.cols[1][2]).toBe("c");
	});

	it("marks done on a clean footer", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		s.apply(BATCH1);
		s.apply({ t: "f", rows: 2 });
		expect(s.status).toBe("done");
		expect(s.truncated).toBe(false);
	});

	it("marks truncated and records the cap", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		s.apply({ t: "f", rows: 50000, truncated: true, cap: 50000 });
		expect(s.status).toBe("truncated");
		expect(s.truncated).toBe(true);
		expect(s.cap).toBe(50000);
	});

	it("marks cancelled when the footer flags an abort", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		s.apply({ t: "f", rows: 1, cancelled: true });
		expect(s.status).toBe("cancelled");
	});

	it("marks error and carries the message", () => {
		const s = new ColumnStore();
		s.apply(HEADER);
		s.apply({ t: "f", rows: 0, error: "boom" });
		expect(s.status).toBe("error");
		expect(s.error).toBe("boom");
	});
});

describe("readNdjsonIntoStore", () => {
	it("folds a full stream into a done store", async () => {
		const store = await readNdjsonIntoStore(
			streamOf([
				`${JSON.stringify(HEADER)}\n${JSON.stringify(BATCH1)}\n${JSON.stringify(BATCH2)}\n${JSON.stringify({ t: "f", rows: 3 })}\n`,
			]),
		);
		expect(store.status).toBe("done");
		expect(store.rowCount).toBe(3);
		expect(store.cols).toEqual([
			[1, 2, 3],
			["a", "b", "c"],
		]);
	});
});
