import { describe, expect, it } from "vitest";

import {
	CANVAS_TOOLS,
	isCanvasTool,
	teachChipSummary,
	toolChipSummary,
	toolLabel,
} from "./tool-chip-summary";

describe("toolLabel", () => {
	it("maps known tools to plain-language titles (never the raw verb)", () => {
		expect(toolLabel("list_tables")).toBe("Workspace tables");
		expect(toolLabel("run_sql")).toBe("Query");
		expect(toolLabel("why_column")).toBe("Column detail");
		expect(toolLabel("look_table")).toBe("Table readiness");
	});

	it("humanizes an unmapped tool instead of leaking snake_case", () => {
		// No raw underscores / lowercase verb reaches the user for a future tool.
		expect(toolLabel("some_new_tool")).toBe("Some new tool");
		expect(toolLabel("")).toBe("Working");
	});
});

describe("isCanvasTool", () => {
	it("marks the 9 canvas-producing tools clickable", () => {
		expect(CANVAS_TOOLS.size).toBe(9);
		for (const name of [
			"list_sources",
			"list_tables",
			"look_table",
			"why_column",
			"connect",
			"frame",
			"select",
			"run_sql",
			"replay",
		]) {
			expect(isCanvasTool(name)).toBe(true);
		}
	});

	it("marks probe / teach display-only", () => {
		for (const name of ["probe", "teach", "unknown"]) {
			expect(isCanvasTool(name)).toBe(false);
		}
	});
});

describe("toolChipSummary — completed canvas tools (no JSON, readable)", () => {
	it("list_sources breaks available inputs down by kind", () => {
		expect(toolChipSummary("list_sources", {}, [{ kind: "file" }])).toBe(
			"1 file",
		);
		expect(
			toolChipSummary("list_sources", {}, [
				{ kind: "database" },
				{ kind: "database" },
				{ kind: "file" },
			]),
		).toBe("2 databases, 1 file");
		expect(toolChipSummary("list_sources", {}, [])).toBe("no available inputs");
	});

	it("list_tables counts tables and notes the source filter", () => {
		expect(toolChipSummary("list_tables", {}, [{}, {}])).toBe("2 tables");
		expect(toolChipSummary("list_tables", { source_id: "s9" }, [{}])).toBe(
			"1 table in s9",
		);
	});

	it("look_table names the table + column count + analyzed state", () => {
		expect(
			toolChipSummary(
				"look_table",
				{},
				{
					table_name: "orders",
					analyzed: true,
					columns: [{}, {}],
				},
			),
		).toBe("orders — 2 columns");
		expect(
			toolChipSummary(
				"look_table",
				{},
				{
					table_name: "orders",
					analyzed: false,
					columns: [{}],
				},
			),
		).toBe("orders — 1 column, not yet analyzed");
	});

	it("why_column names the column + table + band", () => {
		expect(
			toolChipSummary(
				"why_column",
				{},
				{
					column_name: "amount",
					table_name: "orders",
					found: true,
					band: "investigate",
				},
			),
		).toBe("amount (orders) — investigate");
		expect(toolChipSummary("why_column", {}, { found: false })).toBe(
			"column not found",
		);
	});

	it("connect names the source + table count", () => {
		expect(
			toolChipSummary("connect", {}, { source: "people.csv", tables: [{}] }),
		).toBe("people.csv — 1 table");
	});

	it("frame names the vertical + concept count", () => {
		expect(
			toolChipSummary(
				"frame",
				{},
				{ vertical: "ecommerce", concepts: [{}, {}] },
			),
		).toBe("ecommerce — 2 concepts");
	});

	it("select names the source + type", () => {
		expect(
			toolChipSummary("select", {}, { name: "orders", source_type: "file" }),
		).toBe("orders (file)");
	});

	it("run_sql shows the SQL from the call input (truncated, flattened)", () => {
		expect(
			toolChipSummary("run_sql", { sql: "SELECT 1" }, { rowCount: 0 }),
		).toBe("SELECT 1");
		const long = `SELECT ${"x".repeat(100)}`;
		const summary = toolChipSummary("run_sql", { sql: long }, { rowCount: 0 });
		expect(summary.length).toBeLessThanOrEqual(60);
		expect(summary.endsWith("…")).toBe(true);
	});

	it("never includes raw JSON braces from the output", () => {
		const summary = toolChipSummary(
			"connect",
			{},
			{
				source: "x",
				tables: [{ name: "t" }],
			},
		);
		expect(summary).not.toContain("{");
		expect(summary).not.toContain('"');
	});
});

describe("toolChipSummary — streaming / pre-result states", () => {
	it("renders a neutral running label before the result arrives", () => {
		expect(toolChipSummary("list_sources", {}, undefined)).toBe(
			"listing available inputs…",
		);
		expect(toolChipSummary("connect", {}, undefined)).toBe("connecting…");
		expect(toolChipSummary("run_sql", {}, undefined)).toBe("running query…");
	});

	it("treats a truthy-but-PARTIAL result as still running (no .length crash)", () => {
		// The SDK can surface a partial/streaming or errored tool output: truthy but
		// missing its array. Accessing `.tables.length` / `.concepts.length` /
		// `.columns.length` on it crashed the chat rail (the multi-file drag-drop
		// crash). These must degrade to the running label, not throw.
		expect(toolChipSummary("connect", {}, { source: "people.csv" })).toBe(
			"connecting…",
		);
		expect(toolChipSummary("frame", {}, { vertical: "finance" })).toBe(
			"framing concepts…",
		);
		expect(toolChipSummary("look_table", {}, { table_name: "orders" })).toBe(
			"reading table readiness…",
		);
	});

	it("treats a truthy NON-array list output as empty (no .filter crash)", () => {
		// list_* outputs are arrays; a partial/errored truthy non-array reached
		// `.filter`/`.length` → "e.filter is not a function" crashed the rail.
		expect(toolChipSummary("list_sources", {}, {})).toBe("no available inputs");
		expect(toolChipSummary("list_verticals", {}, {})).toBe("no verticals");
		expect(toolChipSummary("list_tables", {}, {})).toBe("0 tables");
	});
});

describe("teachChipSummary (display-only, readable at every state)", () => {
	it("reads {type, payload} from arguments at approval time", () => {
		expect(
			teachChipSummary(
				{ type: "null_value", payload: { sentinel: "N/A" } },
				undefined,
			),
		).toBe("teach null_value {sentinel}");
	});

	it("reads {overlay_id, type} once complete", () => {
		expect(
			teachChipSummary(
				{ type: "null_value", payload: {} },
				{ overlay_id: "ov-7", type: "null_value" },
			),
		).toBe("taught null_value → ov-7");
	});

	it("surfaces a structured validation error", () => {
		expect(
			teachChipSummary(
				{ type: "null_value", payload: {} },
				{ error: "payload.sentinel is required" },
			),
		).toContain("teach rejected");
	});

	it("degrades to a neutral label with no arguments yet", () => {
		expect(teachChipSummary(undefined, undefined)).toBe("teach…");
	});
});

describe("toolChipSummary — replay / probe (display-only)", () => {
	it("replay shows the source before run, run id after", () => {
		expect(toolChipSummary("replay", { source_id: "src1" }, undefined)).toBe(
			"replay src1",
		);
		expect(
			toolChipSummary(
				"replay",
				{ source_id: "src1" },
				{
					run_id: "r9",
				},
			),
		).toBe("replay — run r9");
	});

	it("probe shows the source name + row count", () => {
		expect(
			toolChipSummary("probe", { source_name: "pg" }, { rowCount: 3 }),
		).toBe("probe on pg — 3 rows");
	});
});
