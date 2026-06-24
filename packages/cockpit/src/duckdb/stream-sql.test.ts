// Unit tests for the pure streaming core (DAT-385 P1). No real DuckDB, no
// native addon — we drive `streamNdjson` with a fake StreamableResult and assert
// the cap clamp, the chunk→columnar reshaping, and header/batch/footer framing.

import { describe, expect, it } from "vitest";

import { HARD_ROW_CEILING } from "#/duckdb/limit";
import {
	buildGridQuery,
	clampGridCap,
	clampOffset,
	clampPageLimit,
	encodeFrame,
	GRID_DEFAULT_CAP,
	GRID_MAX_PAGE,
	GRID_PAGE_SIZE,
	quoteIdentifier,
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

	it("clamps above the shared hard ceiling (HARD_ROW_CEILING, DAT-384)", () => {
		expect(clampGridCap(999_999)).toBe(HARD_ROW_CEILING);
		expect(clampGridCap(HARD_ROW_CEILING + 1)).toBe(200_000);
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

describe("quoteIdentifier", () => {
	it("wraps a plain name in double quotes", () => {
		expect(quoteIdentifier("amount")).toBe('"amount"');
	});

	it("doubles embedded quotes so a name can never break out of the literal", () => {
		// A column literally named `weird"name` (or an injection attempt) is quoted,
		// not escaped-then-concatenated: the embedded `"` is doubled.
		expect(quoteIdentifier('weird"name')).toBe('"weird""name"');
		expect(quoteIdentifier('x" ; DROP TABLE t --')).toBe(
			'"x"" ; DROP TABLE t --"',
		);
	});
});

describe("buildGridQuery (design §7.3 — server-side sort)", () => {
	it("wraps the query unchanged when there is no sort", () => {
		expect(buildGridQuery("SELECT * FROM lake.typed.orders")).toBe(
			"SELECT * FROM (SELECT * FROM lake.typed.orders) AS _run_sql",
		);
		expect(buildGridQuery("SELECT 1", null)).toBe(
			"SELECT * FROM (SELECT 1) AS _run_sql",
		);
	});

	it("appends an ORDER BY on the quoted column for asc/desc", () => {
		expect(
			buildGridQuery("SELECT id, amount FROM t", {
				column: "amount",
				dir: "asc",
			}),
		).toBe(
			'SELECT * FROM (SELECT id, amount FROM t) AS _run_sql ORDER BY "amount" ASC',
		);
		expect(
			buildGridQuery("SELECT id FROM t", { column: "id", dir: "desc" }),
		).toBe('SELECT * FROM (SELECT id FROM t) AS _run_sql ORDER BY "id" DESC');
	});

	it("quotes the sort column so a hostile name can't inject", () => {
		const out = buildGridQuery("SELECT * FROM t", {
			column: 'x" ; DROP TABLE t --',
			dir: "asc",
		});
		expect(out).toBe(
			'SELECT * FROM (SELECT * FROM t) AS _run_sql ORDER BY "x"" ; DROP TABLE t --" ASC',
		);
	});
});

describe("buildGridQuery windowing (DAT-613)", () => {
	it("imposes ORDER BY ALL + LIMIT(limit+1)/OFFSET for an unsorted window", () => {
		// A windowed grid MUST impose a deterministic total order or separate
		// LIMIT/OFFSET pages can reorder rows across requests. Unsorted → ORDER BY
		// ALL (every output column, column-agnostic). The +1 over-fetch is the
		// has-more probe (route streams with cap = limit).
		expect(
			buildGridQuery("SELECT id FROM t", null, { limit: 500, offset: 0 }),
		).toBe(
			"SELECT * FROM (SELECT id FROM t) AS _run_sql ORDER BY ALL LIMIT 501 OFFSET 0",
		);
		expect(
			buildGridQuery("SELECT id FROM t", null, { limit: 100, offset: 200 }),
		).toBe(
			"SELECT * FROM (SELECT id FROM t) AS _run_sql ORDER BY ALL LIMIT 101 OFFSET 200",
		);
	});

	it("orders by the sort column then COLUMNS(*) as a stable tiebreaker when windowing", () => {
		// Sorted window: the user column leads, then COLUMNS(*) breaks ties with a
		// column-agnostic total order so rows tied on the sort column keep a stable
		// order across page boundaries.
		expect(
			buildGridQuery(
				"SELECT id, amount FROM t",
				{ column: "amount", dir: "desc" },
				{ limit: 50, offset: 50 },
			),
		).toBe(
			'SELECT * FROM (SELECT id, amount FROM t) AS _run_sql ORDER BY "amount" DESC, COLUMNS(*) LIMIT 51 OFFSET 50',
		);
	});

	it("still quotes the sort column under windowing", () => {
		const out = buildGridQuery(
			"SELECT * FROM t",
			{ column: 'x" --', dir: "asc" },
			{ limit: 10, offset: 0 },
		);
		expect(out).toBe(
			'SELECT * FROM (SELECT * FROM t) AS _run_sql ORDER BY "x"" --" ASC, COLUMNS(*) LIMIT 11 OFFSET 0',
		);
	});

	it("floors fractional window bounds into the inlined integers", () => {
		expect(buildGridQuery("SELECT 1", null, { limit: 10.9, offset: 5.9 })).toBe(
			"SELECT * FROM (SELECT 1) AS _run_sql ORDER BY ALL LIMIT 11 OFFSET 5",
		);
	});

	it("imposes no ORDER BY and no LIMIT without a window (probe path unchanged)", () => {
		// The non-windowed probe grid keeps its exact prior shape — no imposed total
		// order, so the natural scan order is preserved.
		expect(buildGridQuery("SELECT 1")).toBe(
			"SELECT * FROM (SELECT 1) AS _run_sql",
		);
		expect(buildGridQuery("SELECT 1", { column: "x", dir: "asc" })).toBe(
			'SELECT * FROM (SELECT 1) AS _run_sql ORDER BY "x" ASC',
		);
	});
});

describe("clampPageLimit (DAT-613)", () => {
	it("defaults an absent/non-finite limit to the grid page size", () => {
		expect(clampPageLimit()).toBe(GRID_PAGE_SIZE);
		expect(clampPageLimit(Number.NaN)).toBe(GRID_PAGE_SIZE);
		expect(clampPageLimit(Number.POSITIVE_INFINITY)).toBe(GRID_PAGE_SIZE);
	});

	it("passes a value within range and floors a fractional one", () => {
		expect(clampPageLimit(250)).toBe(250);
		expect(clampPageLimit(250.9)).toBe(250);
	});

	it("clamps above the max page and floors a non-positive to 1", () => {
		expect(clampPageLimit(GRID_MAX_PAGE + 1)).toBe(GRID_MAX_PAGE);
		expect(clampPageLimit(0)).toBe(1);
		expect(clampPageLimit(-9)).toBe(1);
	});
});

describe("clampOffset (DAT-613)", () => {
	it("defaults an absent/negative/non-finite offset to 0", () => {
		expect(clampOffset()).toBe(0);
		expect(clampOffset(-5)).toBe(0);
		expect(clampOffset(Number.NaN)).toBe(0);
	});

	it("passes a non-negative value and floors a fractional one", () => {
		expect(clampOffset(1000)).toBe(1000);
		expect(clampOffset(1000.7)).toBe(1000);
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

	it("peeks exactly one chunk past the cap to confirm truncation, then stops", async () => {
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
		// One chunk filled the cap; one peek confirmed there's genuinely more.
		// No third pull — peak memory stays ≈ one chunk.
		expect(pulls).toBe(2);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 2, truncated: true, cap: 2 });
	});

	it("does NOT mark truncated when the result is exactly the cap (full set)", async () => {
		// First chunk fills the cap exactly; the peek returns the end sentinel.
		const result = fakeResult(["id"], ["INTEGER"], [[[1, 2, 3]]]);
		const frames = await collect(result, 3);
		expect(frames[1]).toEqual({ t: "b", n: 3, cols: [[1, 2, 3]] });
		// Clean footer — no `truncated`, no `cap`.
		expect(frames.at(-1)).toEqual({ t: "f", rows: 3 });
	});

	it("does NOT mark truncated when a zero-row sentinel follows an exact-cap chunk", async () => {
		// The peek returns a 0-row chunk (neo's historical end sentinel), not null.
		const result = fakeResult(["id"], ["INTEGER"], [[[1, 2, 3]], [[]]]);
		const frames = await collect(result, 3);
		expect(frames.at(-1)).toEqual({ t: "f", rows: 3 });
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

	it("redacts the source URL from a mid-stream error footer (probe path, DAT-576)", async () => {
		// An external-ATTACH driver error can echo the credential-bearing DSN; the
		// probe route passes its `redact` so the secret never lands in the footer.
		const secret = "Server=db,1433;UID=sa;PWD=hunter2";
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				throw new Error(`ATTACH driver error for '${secret}'`);
			},
		};
		const redact = (m: string) => m.split(secret).join("<source url redacted>");
		const lines: string[] = [];
		for await (const line of streamNdjson(
			result,
			1000,
			"q",
			{ aborted: false },
			redact,
		)) {
			const trimmed = line.trim();
			if (trimmed) lines.push(trimmed);
		}
		const footer = lines
			.map((l) => JSON.parse(l) as { t: string; error?: string })
			.find((f) => f.t === "f");
		expect(footer?.error).toBe(
			"ATTACH driver error for '<source url redacted>'",
		);
		expect(footer?.error).not.toContain(secret);
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
		// Footer flags the early stop so a consumer can tell a partial body from a
		// clean finish.
		expect(frames.at(-1)).toEqual({ t: "f", rows: 1, cancelled: true });
	});

	it("flags cancelled before any chunk when aborted up front", async () => {
		const signal = { aborted: true };
		let pulls = 0;
		const result: StreamableResult = {
			columnNames: () => ["id"],
			columnTypesJson: () => ["INTEGER"],
			fetchChunk: async () => {
				pulls += 1;
				return new FakeChunk([[1]]);
			},
		};
		const frames = await collect(result, 1000, "q_test", signal);
		// Aborted before the loop pulled anything: header, then a cancelled footer.
		expect(pulls).toBe(0);
		expect(frames).toEqual([
			{ t: "h", columns: ["id"], types: ["INTEGER"], queryId: "q_test" },
			{ t: "f", rows: 0, cancelled: true },
		]);
	});
});
