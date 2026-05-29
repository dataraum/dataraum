// Unit tests for the pure streaming core (DAT-385 P1). No real DuckDB, no
// native addon — we drive `streamNdjson` with a fake StreamableResult and assert
// the cap clamp, the chunk→columnar reshaping, and header/batch/footer framing.

import { describe, expect, it } from "vitest";

import {
	clampGridCap,
	encodeFrame,
	GRID_DEFAULT_CAP,
	GRID_HARD_CEILING,
	type ResultFrame,
	type StreamableChunk,
	type StreamableResult,
	streamNdjson,
} from "./stream-sql";

// --- A fake chunk/result that mirrors neo's surface ------------------------

class FakeChunk implements StreamableChunk {
	constructor(private readonly cols: (string | number | null)[][]) {}
	get rowCount(): number {
		return this.cols[0]?.length ?? 0;
	}
	// The real chunk applies the converter per value; our fake values are already
	// JSON-safe, so we pass them through unchanged (the converter is only invoked
	// for type coercion, which the integration test exercises for real).
	convertColumns<T>(): (T | null)[][] {
		return this.cols as unknown as (T | null)[][];
	}
}

/** A result that hands out `chunks` in order, then `null`. */
function fakeResult(
	columns: string[],
	types: string[],
	chunks: (string | number | null)[][][],
): StreamableResult {
	let i = 0;
	return {
		columnNames: () => columns,
		columnTypesJson: () => types,
		fetchChunk: async () => {
			if (i >= chunks.length) return null;
			const c = chunks[i];
			i += 1;
			return new FakeChunk(c);
		},
	};
}

/** Collect a streamNdjson run into parsed frames. */
async function collect(
	result: StreamableResult,
	cap: number,
	queryId = "q_test",
	signal?: { aborted: boolean },
): Promise<ResultFrame[]> {
	const frames: ResultFrame[] = [];
	for await (const line of streamNdjson(result, cap, queryId, signal)) {
		expect(line.endsWith("\n")).toBe(true);
		frames.push(JSON.parse(line) as ResultFrame);
	}
	return frames;
}

describe("clampGridCap (design §5.5)", () => {
	it("defaults an absent cap to the grid default (50k, not the agent's 1000)", () => {
		expect(clampGridCap()).toBe(GRID_DEFAULT_CAP);
		expect(clampGridCap(undefined)).toBe(50_000);
	});

	it("passes through a value within range", () => {
		expect(clampGridCap(1234)).toBe(1234);
	});

	it("clamps above the hard ceiling to 200k", () => {
		expect(clampGridCap(999_999)).toBe(GRID_HARD_CEILING);
		expect(clampGridCap(GRID_HARD_CEILING + 1)).toBe(200_000);
	});

	it("floors a non-positive or non-finite cap to at least 1", () => {
		expect(clampGridCap(0)).toBe(1);
		expect(clampGridCap(-5)).toBe(1);
		expect(clampGridCap(Number.NaN)).toBe(GRID_DEFAULT_CAP);
		expect(clampGridCap(Number.POSITIVE_INFINITY)).toBe(GRID_DEFAULT_CAP);
	});

	it("floors a fractional cap", () => {
		expect(clampGridCap(10.9)).toBe(10);
	});
});

describe("encodeFrame", () => {
	it("serializes one frame per line with a trailing newline", () => {
		expect(encodeFrame({ t: "f", rows: 3 })).toBe('{"t":"f","rows":3}\n');
	});
});

describe("streamNdjson framing", () => {
	it("emits header, one batch per chunk, then a clean footer", async () => {
		const result = fakeResult(
			["id", "name"],
			["INTEGER", "VARCHAR"],
			[
				[
					[1, 2],
					["a", "b"],
				],
				[[3], ["c"]],
			],
		);
		const frames = await collect(result, 1000);

		expect(frames[0]).toEqual({
			t: "h",
			columns: ["id", "name"],
			types: ["INTEGER", "VARCHAR"],
			queryId: "q_test",
		});
		expect(frames[1]).toEqual({
			t: "b",
			n: 2,
			cols: [
				[1, 2],
				["a", "b"],
			],
		});
		expect(frames[2]).toEqual({ t: "b", n: 1, cols: [[3], ["c"]] });
		expect(frames[3]).toEqual({ t: "f", rows: 3 });
		expect(frames).toHaveLength(4);
	});

	it("treats columnar batches as cols[colIndex][rowIndex]", async () => {
		const result = fakeResult(
			["a", "b", "c"],
			["INTEGER", "INTEGER", "INTEGER"],
			[
				[
					[1, 4],
					[2, 5],
					[3, 6],
				],
			],
		);
		const frames = await collect(result, 1000);
		const batch = frames[1];
		if (batch.t !== "b") throw new Error("expected batch");
		// Two rows (1,2,3) and (4,5,6) stored column-major.
		expect(batch.cols).toEqual([
			[1, 4],
			[2, 5],
			[3, 6],
		]);
		expect(batch.n).toBe(2);
	});

	it("always emits a footer even with zero chunks", async () => {
		const result = fakeResult(["id"], ["INTEGER"], []);
		const frames = await collect(result, 1000);
		expect(frames).toEqual([
			{ t: "h", columns: ["id"], types: ["INTEGER"], queryId: "q_test" },
			{ t: "f", rows: 0 },
		]);
	});

	it("stops on a zero-row chunk (neo's end sentinel) without emitting a batch", async () => {
		const result = fakeResult(["id"], ["INTEGER"], [[[]]]);
		const frames = await collect(result, 1000);
		expect(frames).toEqual([
			{ t: "h", columns: ["id"], types: ["INTEGER"], queryId: "q_test" },
			{ t: "f", rows: 0 },
		]);
	});
});

describe("streamNdjson cap + truncation", () => {
	it("slices the chunk that crosses the cap and marks truncated", async () => {
		const result = fakeResult(["id"], ["INTEGER"], [[[1, 2, 3, 4, 5]]]);
		const frames = await collect(result, 3);
		expect(frames[1]).toEqual({ t: "b", n: 3, cols: [[1, 2, 3]] });
		expect(frames[2]).toEqual({ t: "f", rows: 3, truncated: true, cap: 3 });
	});

	it("stops pulling further chunks once the cap is hit", async () => {
		let pulls = 0;
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				pulls += 1;
				return new FakeChunk([[1, 2]]);
			},
		};
		const frames = await collect(result, 2);
		// Exactly one chunk consumed (it filled the cap); no extra pull.
		expect(pulls).toBe(1);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 2, truncated: true, cap: 2 });
	});

	it("emits an in-band error footer when fetchChunk throws (still HTTP 200)", async () => {
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				throw new Error("Binder Error: no such column");
			},
		};
		const frames = await collect(result, 1000);
		expect(frames[0].t).toBe("h");
		expect(frames[1]).toEqual({
			t: "f",
			rows: 0,
			error: "Binder Error: no such column",
		});
	});

	it("reports rows emitted before a mid-stream failure in the error footer", async () => {
		let i = 0;
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				i += 1;
				if (i === 1) return new FakeChunk([[1, 2]]);
				throw new Error("IO Error mid-stream");
			},
		};
		const frames = await collect(result, 1000);
		expect(frames[1]).toEqual({ t: "b", n: 2, cols: [[1, 2]] });
		expect(frames[2]).toEqual({
			t: "f",
			rows: 2,
			error: "IO Error mid-stream",
		});
	});
});

describe("streamNdjson cancellation", () => {
	it("breaks at the next chunk boundary when the signal is aborted", async () => {
		const signal = { aborted: false };
		let pulls = 0;
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				pulls += 1;
				if (pulls === 1) {
					// Simulate the client going away after the first chunk.
					signal.aborted = true;
					return new FakeChunk([[1]]);
				}
				return new FakeChunk([[99]]);
			},
		};
		const frames = await collect(result, 1000, "q_test", signal);
		// First chunk emitted; loop checks aborted before pulling a second.
		expect(pulls).toBe(1);
		expect(frames.map((f) => f.t)).toEqual(["h", "b", "f"]);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 1 });
	});
});
