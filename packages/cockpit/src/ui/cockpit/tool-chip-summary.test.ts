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

	it("flips a progressive verb to its settled form once the call is done", () => {
		// In-progress title (done=false) stays present-tense…
		expect(toolLabel("select")).toBe("Registering source");
		expect(toolLabel("connect")).toBe("Reading source");
		// …and flips to a settled form when the call completes.
		expect(toolLabel("select", true)).toBe("Registered source");
		expect(toolLabel("connect", true)).toBe("Source schema");
		expect(toolLabel("teach", true)).toBe("Taught");
		expect(toolLabel("replay", true)).toBe("Re-ran");
	});

	it("leaves already-settled noun titles unchanged when done", () => {
		// Tools whose label is already a noun read fine in both states — no flip.
		expect(toolLabel("list_tables", true)).toBe("Workspace tables");
		expect(toolLabel("run_sql", true)).toBe("Query");
	});
});

describe("isCanvasTool", () => {
	it("marks the 13 canvas-producing tools clickable", () => {
		expect(CANVAS_TOOLS.size).toBe(13);
		for (const name of [
			"list_sources",
			"list_tables",
			"look_table",
			"why_column",
			"why_table",
			"why_relationship",
			"look_relationships",
			"connect",
			"frame",
			"select",
			"run_sql",
			"replay",
			"upload",
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

	// DAT-437: the engine emits one row per physical layer (raw/typed/quarantine),
	// so the chip must count LOGICAL tables — 8 logical tables must never read as
	// "24 tables".
	it("list_tables counts LOGICAL tables (physical layers collapse)", () => {
		const layers = (source_id: string, table_name: string) =>
			["raw", "typed", "quarantine"].map((layer) => ({
				source_id,
				table_name,
				source_name: `${table_name}.csv`,
				layer,
			}));
		// 8 logical tables × 3 physical layers = 24 rows → the chip says 8.
		const inventory = Array.from({ length: 8 }, (_, i) =>
			layers("s1", `tbl_${i}`),
		).flat();
		expect(toolChipSummary("list_tables", {}, inventory)).toBe("8 tables");
		// Same-named tables from DIFFERENT sources stay distinct logical tables.
		expect(
			toolChipSummary("list_tables", {}, [
				...layers("s1", "orders"),
				...layers("s2", "orders"),
			]),
		).toBe("2 tables");
	});

	// DAT-437 review: an upload's source_id is the content-keyed `src_<40hex>`
	// digest — the filter suffix must use the rows' HUMAN source_name (post-DAT-433
	// the filename for uploads, the connection name for db), and the digest must
	// never reach the chip text in ANY state.
	it("list_tables names the source filter by the rows' source_name, never the digest source_id", () => {
		const digestId = `src_${"deadbeef".repeat(5)}`; // a REAL content-keyed upload id
		const rows = ["raw", "typed", "quarantine"].map((layer) => ({
			source_id: digestId,
			source_name: "trial_balance.csv",
			table_name: "trial_balance",
			layer,
		}));
		const summary = toolChipSummary(
			"list_tables",
			{ source_id: digestId },
			rows,
		);
		expect(summary).toBe("1 table in trial_balance.csv");
		expect(summary).not.toContain("src_");
		expect(summary).not.toContain("deadbeef");
		// An empty filtered result has no human label — the suffix drops entirely.
		expect(toolChipSummary("list_tables", { source_id: digestId }, [])).toBe(
			"0 tables",
		);
		// The running state can't know the human label yet — no id echo either.
		expect(
			toolChipSummary("list_tables", { source_id: digestId }, undefined),
		).toBe("listing tables…");
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

	it("why_table names the table + band (DAT-434)", () => {
		expect(
			toolChipSummary(
				"why_table",
				{},
				{ table_name: "orders", found: true, band: "ready" },
			),
		).toBe("orders — ready");
		expect(toolChipSummary("why_table", {}, { found: false })).toBe(
			"table not found",
		);
		// A null display name degrades to a placeholder, never the table_id.
		expect(
			toolChipSummary(
				"why_table",
				{},
				{ table_id: "t_1", table_name: null, found: true, band: "ready" },
			),
		).toBe("table — ready");
	});

	it("why_relationship names the endpoints + band (DAT-434)", () => {
		expect(
			toolChipSummary(
				"why_relationship",
				{},
				{
					from_table_name: "orders",
					to_table_name: "customers",
					found: true,
					band: "ready",
				},
			),
		).toBe("orders → customers — ready");
		expect(toolChipSummary("why_relationship", {}, { found: false })).toBe(
			"relationship not found",
		);
	});

	it("look_relationships counts the relationships (DAT-434)", () => {
		expect(
			toolChipSummary(
				"look_relationships",
				{},
				{ analyzed: true, relationships: [{}, {}] },
			),
		).toBe("2 relationships");
		expect(
			toolChipSummary(
				"look_relationships",
				{},
				{ analyzed: false, relationships: [] },
			),
		).toBe("not yet analyzed");
	});

	it("connect names the source + table count", () => {
		expect(
			toolChipSummary("connect", {}, { source: "people.csv", tables: [{}] }),
		).toBe("people.csv — 1 table");
	});

	it("connect shows a file source's filename, not the full s3:// URI", () => {
		expect(
			toolChipSummary(
				"connect",
				{},
				{
					sourceKind: "file",
					source: "s3://dataraum-lake/uploads/da833c2e/trial_balance.csv",
					tables: [{}],
				},
			),
		).toBe("trial_balance.csv — 1 table");
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
