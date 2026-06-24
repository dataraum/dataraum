// Unit tests for the pure (neo-free) grid SQL composition + request parsing:
// sort (DAT-385), windowing + push-down filter (DAT-613), and the clamps. No
// DuckDB, no driver — every function here is a pure string/shape transform.

import { describe, expect, it } from "vitest";

import {
	buildFilterClause,
	buildGridQuery,
	clampGridCap,
	clampOffset,
	clampPageLimit,
	GRID_DEFAULT_CAP,
	GRID_MAX_PAGE,
	GRID_PAGE_SIZE,
	type GridFilter,
	parseColumnFilterInput,
	parseFilters,
	parseSort,
	quoteIdentifier,
} from "#/duckdb/grid-query";
import { HARD_ROW_CEILING } from "#/duckdb/limit";

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

describe("parseSort", () => {
	it("returns null for an absent sort", () => {
		expect(parseSort(undefined)).toEqual({ sort: null });
		expect(parseSort(null)).toEqual({ sort: null });
	});

	it("accepts a valid {column,dir}", () => {
		expect(parseSort({ column: "amount", dir: "desc" })).toEqual({
			sort: { column: "amount", dir: "desc" },
		});
	});

	it("rejects an array, a bad dir, and an empty/oversized column", () => {
		expect(parseSort([])).toEqual({ error: expect.stringContaining("object") });
		expect(parseSort({ column: "a", dir: "up" })).toEqual({
			error: expect.stringContaining("asc"),
		});
		expect(parseSort({ column: "", dir: "asc" })).toEqual({
			error: expect.stringContaining("non-empty"),
		});
		expect(parseSort({ column: "x".repeat(257), dir: "asc" })).toEqual({
			error: expect.stringContaining("256"),
		});
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

	it("inserts WHERE before ORDER BY/LIMIT and after the wrap", () => {
		// The filter clause is opaque to buildGridQuery — it just splices it in the
		// SQL-correct position (WHERE → ORDER BY → LIMIT).
		expect(
			buildGridQuery(
				"SELECT id FROM t",
				{ column: "id", dir: "asc" },
				{ limit: 10, offset: 0 },
				"CAST(\"id\" AS VARCHAR) ILIKE ('%' || $1 || '%')",
			),
		).toBe(
			"SELECT * FROM (SELECT id FROM t) AS _run_sql WHERE CAST(\"id\" AS VARCHAR) ILIKE ('%' || $1 || '%') ORDER BY \"id\" ASC, COLUMNS(*) LIMIT 11 OFFSET 0",
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

describe("buildFilterClause (DAT-613 push-down filter)", () => {
	it("returns no WHERE and no params for an empty filter set", () => {
		expect(buildFilterClause([], 0)).toEqual({ where: null, params: [] });
	});

	it("numbers filter params AFTER the user's own params and ANDs predicates", () => {
		// User query already used $1,$2 → filter binds start at $3.
		const filters: GridFilter[] = [
			{ column: "name", op: "contains", value: "ac" },
			{ column: "amount", op: "gt", value: "1000" },
		];
		const { where, params } = buildFilterClause(filters, 2);
		expect(where).toBe(
			"CAST(\"name\" AS VARCHAR) ILIKE ('%' || $3 || '%') AND \"amount\" > $4",
		);
		expect(params).toEqual(["ac", "1000"]);
	});

	it("maps every comparison operator to its SQL", () => {
		const ops = [
			["eq", "="],
			["neq", "<>"],
			["gt", ">"],
			["gte", ">="],
			["lt", "<"],
			["lte", "<="],
		] as const;
		for (const [op, sql] of ops) {
			const { where } = buildFilterClause([{ column: "n", op, value: "1" }], 0);
			expect(where).toBe(`"n" ${sql} $1`);
		}
	});

	it("quotes a hostile filter column", () => {
		const { where } = buildFilterClause(
			[{ column: 'x" --', op: "eq", value: "1" }],
			0,
		);
		expect(where).toBe('"x"" --" = $1');
	});
});

describe("parseFilters (DAT-613 request validation)", () => {
	it("defaults absent filters to an empty list", () => {
		expect(parseFilters(undefined)).toEqual({ filters: [] });
		expect(parseFilters(null)).toEqual({ filters: [] });
	});

	it("accepts a well-formed filter array", () => {
		expect(
			parseFilters([{ column: "amount", op: "gte", value: "10" }]),
		).toEqual({ filters: [{ column: "amount", op: "gte", value: "10" }] });
	});

	it("rejects a non-array, an unknown op, a bad column, and a non-string value", () => {
		expect(parseFilters({})).toEqual({
			error: expect.stringContaining("array"),
		});
		expect(parseFilters([{ column: "a", op: "like", value: "x" }])).toEqual({
			error: expect.stringContaining("filter.op"),
		});
		expect(parseFilters([{ column: "", op: "eq", value: "x" }])).toEqual({
			error: expect.stringContaining("filter.column"),
		});
		expect(parseFilters([{ column: "a", op: "eq", value: 5 }])).toEqual({
			error: expect.stringContaining("filter.value"),
		});
	});

	it("rejects too many filters", () => {
		const many = Array.from({ length: 65 }, () => ({
			column: "a",
			op: "eq" as const,
			value: "1",
		}));
		expect(parseFilters(many)).toEqual({
			error: expect.stringContaining("max 64"),
		});
	});
});

describe("parseColumnFilterInput (DAT-613 filter-row input)", () => {
	it("clears the filter on empty/whitespace input", () => {
		expect(parseColumnFilterInput("amount", "", "numeric")).toBeNull();
		expect(parseColumnFilterInput("name", "   ", "text")).toBeNull();
		expect(parseColumnFilterInput("amount", ">  ", "numeric")).toBeNull();
	});

	it("text columns always match a substring (contains)", () => {
		expect(parseColumnFilterInput("name", " ac ", "text")).toEqual({
			column: "name",
			op: "contains",
			value: "ac",
		});
		// Even a leading '>' is literal text for a text column.
		expect(parseColumnFilterInput("name", ">x", "text")).toEqual({
			column: "name",
			op: "contains",
			value: ">x",
		});
	});

	it("numeric/temporal columns parse a leading comparison operator", () => {
		const cases = [
			[">1000", "gt", "1000"],
			[">= 10", "gte", "10"],
			["<5", "lt", "5"],
			["<=  9", "lte", "9"],
			["!=3", "neq", "3"],
			["<>3", "neq", "3"],
			["=42", "eq", "42"],
		] as const;
		for (const [raw, op, value] of cases) {
			expect(parseColumnFilterInput("amount", raw, "numeric")).toEqual({
				column: "amount",
				op,
				value,
			});
		}
	});

	it("treats a bare numeric/temporal value as equals", () => {
		expect(parseColumnFilterInput("amount", "42", "numeric")).toEqual({
			column: "amount",
			op: "eq",
			value: "42",
		});
		expect(parseColumnFilterInput("created", "2024-01-01", "temporal")).toEqual(
			{ column: "created", op: "eq", value: "2024-01-01" },
		);
	});
});
