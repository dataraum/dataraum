import type { UIMessage } from "@tanstack/ai-react";
import { describe, expect, it } from "vitest";

import {
	canvasFromCallId,
	canvasFromMessages,
	toolResultToCanvas,
} from "./tool-result-to-canvas";

function msg(parts: unknown[]): UIMessage {
	return { id: "m", role: "assistant", parts } as unknown as UIMessage;
}

describe("toolResultToCanvas", () => {
	it("maps list_sources to a source-list canvas", () => {
		const state = toolResultToCanvas("list_sources", [
			{
				kind: "file",
				name: "orders.csv",
				backend: null,
				uri: "s3://dataraum-lake/uploads/abc/orders.csv",
				size_bytes: 1234,
			},
		]);
		expect(state).toEqual({
			kind: "source-list",
			sources: [expect.objectContaining({ name: "orders.csv", kind: "file" })],
		});
	});

	it("maps list_tables to a workspace-inventory canvas", () => {
		const state = toolResultToCanvas("list_tables", [
			{
				table_id: "t1",
				table_name: "orders",
				layer: "typed",
				row_count: 10,
				column_count: 3,
				source_id: "s1",
				source_name: "orders.csv",
				source_type: "file",
				source_backend: null,
				analyzed: true,
				worst_band: "ready",
				readiness: { ready: 3, investigate: 0, blocked: 0, unanalyzed: 0 },
			},
		]);
		expect(state?.kind).toBe("workspace-inventory");
	});

	it("maps look_table to a table-readiness canvas", () => {
		const state = toolResultToCanvas("look_table", {
			table_id: "t1",
			table_name: "orders",
			analyzed: true,
			pending_teaches: 0,
			columns: [],
		});
		expect(state?.kind).toBe("table-readiness");
	});

	it("leaves the canvas unchanged when look_table has no result", () => {
		expect(toolResultToCanvas("look_table", null)).toBeNull();
	});

	it("maps why_column to a column-why canvas", () => {
		const state = toolResultToCanvas("why_column", {
			column_id: "c1",
			column_name: "amount",
			table_name: "orders",
			found: true,
			band: "investigate",
			worst_intent_risk: 0.4,
			analyzed: true,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "…",
			pending_teaches: 0,
		});
		expect(state?.kind).toBe("column-why");
	});

	it("leaves the canvas unchanged when why_column has no result", () => {
		expect(toolResultToCanvas("why_column", null)).toBeNull();
	});

	it("the why_* projectors reject the SDK's errored-call output shape (DAT-434)", () => {
		// An errored server tool's output is the truthy `{ error }` object — it
		// must NOT project (it would render as a not-found state, masking the
		// failure); the error surfaces in the chat rail instead.
		for (const tool of [
			"why_column",
			"why_table",
			"why_relationship",
			"why_validation",
		]) {
			expect(
				toolResultToCanvas(tool, { error: "Temporal query failed" }),
			).toBeNull();
		}
	});

	it("maps why_table to a table-why canvas (DAT-434)", () => {
		const state = toolResultToCanvas("why_table", {
			table_id: "t1",
			table_name: "orders",
			found: true,
			band: "ready",
			worst_intent_risk: 0.1,
			analyzed: true,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "…",
			pending_teaches: 0,
		});
		expect(state?.kind).toBe("table-why");
	});

	it("leaves the canvas unchanged when why_table has no result", () => {
		expect(toolResultToCanvas("why_table", null)).toBeNull();
	});

	it("maps why_relationship to a relationship-why canvas (DAT-434)", () => {
		const state = toolResultToCanvas("why_relationship", {
			from_column_id: "c1",
			to_column_id: "c2",
			from_table_name: "orders",
			from_column_name: "customer_id",
			to_table_name: "customers",
			to_column_name: "id",
			found: true,
			band: "ready",
			worst_intent_risk: 0.1,
			analyzed: true,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "…",
			pending_teaches: 0,
		});
		expect(state?.kind).toBe("relationship-why");
	});

	it("leaves the canvas unchanged when why_relationship has no result", () => {
		expect(toolResultToCanvas("why_relationship", null)).toBeNull();
	});

	it("maps look_relationships to a relationship-list canvas (DAT-434)", () => {
		const state = toolResultToCanvas("look_relationships", {
			session_id: "s1",
			analyzed: true,
			pending_teaches: 0,
			relationships: [],
		});
		expect(state?.kind).toBe("relationship-list");
	});

	it("leaves the canvas unchanged for a partial look_relationships output (no relationships array)", () => {
		// Same partial/streaming guard as connect/frame: a truthy object without
		// the array must not project (the list widget maps over it).
		expect(
			toolResultToCanvas("look_relationships", { analyzed: true }),
		).toBeNull();
		expect(toolResultToCanvas("look_relationships", null)).toBeNull();
	});

	it("maps why_validation to a validation-why canvas (DAT-440)", () => {
		const state = toolResultToCanvas("why_validation", {
			validation_id: "gl_invoice_match",
			found: true,
			state: "executed",
			state_reason: null,
			strictness: 0.8,
			grounded_against: "",
			status: "executed",
			severity: "error",
			passed: false,
			message: "12 invoices unmatched",
			sql_used: "SELECT 1",
			executed_at: "2026-06-07T12:00:00.000Z",
			details: "",
			pending_teaches: 0,
		});
		expect(state?.kind).toBe("validation-why");
	});

	it("leaves the canvas unchanged when why_validation has no result", () => {
		expect(toolResultToCanvas("why_validation", null)).toBeNull();
	});

	it("maps look_validation to a validation-list canvas (DAT-440)", () => {
		const state = toolResultToCanvas("look_validation", {
			session_id: "s1",
			analyzed: true,
			pending_teaches: 0,
			validations: [],
		});
		expect(state?.kind).toBe("validation-list");
	});

	it("leaves the canvas unchanged for a partial look_validation output (no validations array)", () => {
		expect(
			toolResultToCanvas("look_validation", { analyzed: true }),
		).toBeNull();
		expect(toolResultToCanvas("look_validation", null)).toBeNull();
	});

	it("maps connect to a schema-preview canvas", () => {
		const schema = {
			sourceKind: "file" as const,
			source: "/data/people.csv",
			tables: [
				{
					name: "people.csv",
					rowCountEstimate: 3,
					columns: [
						{
							name: "id",
							position: 1,
							sourceType: "BIGINT",
							nullable: false,
							sampleValues: [1, 2, 3],
						},
					],
				},
			],
		};
		expect(toolResultToCanvas("connect", schema)).toEqual({
			kind: "schema-preview",
			schema,
		});
	});

	it("returns null for a missing connect result (canvas unchanged)", () => {
		expect(toolResultToCanvas("connect", null)).toBeNull();
	});

	it("returns null for a PARTIAL connect result (no tables array — no SchemaPreview crash)", () => {
		// A truthy-but-partial/streaming or errored connect output has no `tables`
		// array; projecting it crashed SchemaPreview on `schema.tables.length` (the
		// multi-file drag-drop crash). Leave the canvas unchanged until complete.
		expect(toolResultToCanvas("connect", { source: "people.csv" })).toBeNull();
		expect(toolResultToCanvas("connect", {})).toBeNull();
	});

	it("maps frame to a model-frame canvas (DAT-382, DAT-469)", () => {
		const frame = {
			vertical: "_adhoc",
			concepts: [
				{
					name: "revenue",
					description: "Total income",
					indicators: ["revenue", "sales"],
					typical_role: "measure",
					overlay_id: "o1",
				},
			],
			validations: [
				{
					validation_id: "non_negative_amounts",
					name: "Non-negative amounts",
					description: "Every amount must be >= 0.",
					category: "data_quality",
					severity: "error",
					check_type: "constraint",
					overlay_id: "v1",
				},
			],
		};
		expect(toolResultToCanvas("frame", frame)).toEqual({
			kind: "model-frame",
			frame,
		});
	});

	it("returns null for a missing frame result (canvas unchanged)", () => {
		expect(toolResultToCanvas("frame", null)).toBeNull();
	});

	it("returns null for a PARTIAL frame result (no concepts array)", () => {
		expect(toolResultToCanvas("frame", { vertical: "finance" })).toBeNull();
	});

	it("maps run_sql to a result-grid from the CALL INPUT (sql + params)", () => {
		// The grid re-issues the query, so it reads the input, not the result.
		const result = { columns: ["n"], rows: [{ n: 1 }], rowCount: 1 };
		const input = { sql: "SELECT n FROM t WHERE n > $1", params: [0] };
		expect(toolResultToCanvas("run_sql", result, input)).toEqual({
			kind: "result-grid",
			sql: "SELECT n FROM t WHERE n > $1",
			params: [0],
		});
	});

	it("omits params for run_sql with no bind values (absent or empty)", () => {
		// Absent params and an empty array are the same query — both collapse to
		// no params so the grid's queryKey can't flip between them mid-stream.
		expect(toolResultToCanvas("run_sql", {}, { sql: "SELECT 1" })).toEqual({
			kind: "result-grid",
			sql: "SELECT 1",
		});
		expect(
			toolResultToCanvas("run_sql", {}, { sql: "SELECT 1", params: [] }),
		).toEqual({ kind: "result-grid", sql: "SELECT 1" });
	});

	it("returns null for run_sql with no sql on the wire (canvas unchanged)", () => {
		expect(toolResultToCanvas("run_sql", {}, {})).toBeNull();
		expect(toolResultToCanvas("run_sql", {}, undefined)).toBeNull();
	});

	it("maps answer to an answer-result, lifting the confidence onto the canvas (DAT-500)", () => {
		const result = {
			answer: "Total revenue is 42.",
			grid: { sql: "SELECT SUM(revenue) AS value FROM t" },
			assumptions: ["Treated 2024 as the fiscal year."],
			concepts_used: ["revenue"],
			tables_touched: ["t"],
			data_quality: { band: "investigate", note: "one table not analyzed" },
			components: [],
			reliability: {
				grounded_ratio: 0.5,
				exact_reuse: 1,
				adapted: 0,
				fresh: 1,
			},
		};
		expect(toolResultToCanvas("answer", result)).toEqual({
			kind: "answer-result",
			sql: "SELECT SUM(revenue) AS value FROM t",
			confidence: {
				band: "investigate",
				note: "one table not analyzed",
				groundedRatio: 0.5,
				reuse: { exactReuse: 1, adapted: 0, fresh: 1 },
				assumptions: ["Treated 2024 as the fiscal year."],
				conceptsUsed: ["revenue"],
			},
		});
	});

	it("answer with no analyzed table yields a null band + zeroed reuse (no throw)", () => {
		expect(
			toolResultToCanvas("answer", {
				grid: { sql: "SELECT 1" },
				data_quality: null,
				assumptions: [],
				concepts_used: [],
			}),
		).toEqual({
			kind: "answer-result",
			sql: "SELECT 1",
			confidence: {
				band: null,
				note: undefined,
				groundedRatio: 0,
				reuse: { exactReuse: 0, adapted: 0, fresh: 0 },
				assumptions: [],
				conceptsUsed: [],
			},
		});
	});

	it("returns null for answer with no grid or an agent error (canvas unchanged)", () => {
		expect(toolResultToCanvas("answer", { grid: null })).toBeNull();
		expect(toolResultToCanvas("answer", {})).toBeNull();
		expect(toolResultToCanvas("answer", { error: "boom" })).toBeNull();
	});

	it("tolerates a drifted answer result without throwing (non-object fields, bad types)", () => {
		// data_quality a bare string, reliability a string, concepts_used with a
		// non-string member — all must degrade to defaults, never throw.
		const state = toolResultToCanvas("answer", {
			grid: { sql: "SELECT 1" },
			data_quality: "ready",
			reliability: "n/a",
			assumptions: ["ok", 42, null],
			concepts_used: ["revenue", 7],
		});
		expect(state).toEqual({
			kind: "answer-result",
			sql: "SELECT 1",
			confidence: {
				band: null,
				note: undefined,
				groundedRatio: 0,
				reuse: { exactReuse: 0, adapted: 0, fresh: 0 },
				assumptions: ["ok"],
				conceptsUsed: ["revenue"],
			},
		});
	});

	it("maps a teach_metric OVERRIDE to the metric-shadow key (DAT-482)", () => {
		expect(
			toolResultToCanvas("teach_metric", {
				overlay_id: "ov-1",
				graph_id: "ebitda",
				vertical: "finance",
				override: true,
				shadowed_spec: { graph_id: "ebitda", name: "EBITDA" },
			}),
		).toEqual({
			kind: "metric-shadow",
			vertical: "finance",
			graphId: "ebitda",
		});
	});

	it("returns null for a fresh teach_metric declaration (nothing to replace)", () => {
		expect(
			toolResultToCanvas("teach_metric", {
				overlay_id: "ov-2",
				graph_id: "win_rate",
				vertical: "sales",
				override: false,
				shadowed_spec: null,
			}),
		).toBeNull();
		// An errored teach result ({error}) carries no override → no canvas.
		expect(
			toolResultToCanvas("teach_metric", { error: "DB write failed" }),
		).toBeNull();
	});

	it("maps upload to the upload-area canvas (the UI tool just opens it)", () => {
		expect(toolResultToCanvas("upload", { ready: true })).toEqual({
			kind: "upload-area",
		});
	});

	it("maps select to the add-source-progress canvas (DAT-436: calling select starts the import)", () => {
		const selection = {
			sources: ["s1"],
			name: "orders.csv",
			source_type: "csv",
			backend: null,
			stage: "add_source",
			vertical: "finance",
			file_uris: ["s3://dataraum-lake/uploads/aaa111/orders.csv"],
			recipe_tables: null,
			workflow_id: "addsource-ws-sess",
			run_id: "run-1",
			session_id: "sess-1",
		};
		expect(toolResultToCanvas("select", selection)).toEqual({
			kind: "add-source-progress",
			workflowId: "addsource-ws-sess",
			runId: "run-1",
		});
	});

	it("returns null for a missing/refused select result (canvas unchanged)", () => {
		expect(toolResultToCanvas("select", null)).toBeNull();
		// A refused select (NoConceptsError — no run ids) projects nothing; the
		// last good canvas stays and the error surfaces in the chat rail.
		expect(toolResultToCanvas("select", { error: "no concepts" })).toBeNull();
	});

	it("returns null for write/compute tools (canvas unchanged)", () => {
		expect(toolResultToCanvas("teach", { overlay_id: "o1" })).toBeNull();
	});

	it("maps replay to the add-source-progress canvas (DAT-352)", () => {
		expect(
			toolResultToCanvas("replay", {
				workflow_id: "wf-1",
				run_id: "run-1",
				source_id: "s1",
				scope: {},
			}),
		).toEqual({
			kind: "add-source-progress",
			workflowId: "wf-1",
			runId: "run-1",
		});
		// A rejected/failed replay (no ids) leaves the canvas unchanged.
		expect(toolResultToCanvas("replay", {})).toBeNull();
	});

	it("maps begin_session to the session-progress canvas (DAT-435)", () => {
		expect(
			toolResultToCanvas("begin_session", {
				workflow_id: "beginsession-ws-sess",
				run_id: "run-1",
				session_id: "sess-1",
				table_ids: ["t1", "t2"],
			}),
		).toEqual({
			kind: "session-progress",
			workflowId: "beginsession-ws-sess",
			runId: "run-1",
		});
		// A failed start (no ids — e.g. the SDK's `{error}` output) leaves the
		// canvas unchanged; the failure surfaces in the chat rail.
		expect(toolResultToCanvas("begin_session", null)).toBeNull();
		expect(
			toolResultToCanvas("begin_session", { error: "session row missing" }),
		).toBeNull();
	});

	it("maps operating_model to the operating-model-progress canvas (DAT-440)", () => {
		expect(
			toolResultToCanvas("operating_model", {
				workflow_id: "operatingmodel-ws-sess",
				run_id: "run-om-1",
				session_id: "sess-1",
			}),
		).toEqual({
			kind: "operating-model-progress",
			workflowId: "operatingmodel-ws-sess",
			runId: "run-om-1",
		});
		// A failed start leaves the canvas unchanged (chat rail carries the error).
		expect(toolResultToCanvas("operating_model", null)).toBeNull();
		expect(
			toolResultToCanvas("operating_model", { error: "workflow start failed" }),
		).toBeNull();
	});

	it("returns null for a missing or NON-array list result (no .filter/.reduce crash)", () => {
		// A partial/streaming or errored output can be undefined or a truthy
		// NON-array; projecting it crashed SourceList/Inventory on .filter/.reduce/
		// .length ("e.filter is not a function"). Leave the canvas unchanged.
		expect(toolResultToCanvas("list_sources", undefined)).toBeNull();
		expect(toolResultToCanvas("list_sources", {})).toBeNull();
		expect(toolResultToCanvas("list_tables", { error: "x" })).toBeNull();
		// A genuine empty array still projects (correctly an empty source-list).
		expect(toolResultToCanvas("list_sources", [])).toEqual({
			kind: "source-list",
			sources: [],
		});
	});
});

describe("canvasFromMessages", () => {
	it("reads a result from a tool-call part's output", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "list_sources",
					arguments: "{}",
					state: "complete",
					output: [],
				},
			]),
		];
		expect(canvasFromMessages(messages)).toEqual({
			kind: "source-list",
			sources: [],
		});
	});

	it("reads a result from a correlated tool-result part (JSON content)", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "list_tables",
					arguments: "{}",
					state: "complete",
				},
				{
					type: "tool-result",
					toolCallId: "c1",
					content: JSON.stringify([
						{
							table_id: "t1",
							source_id: "s1",
							table_name: "x",
							layer: "raw",
							row_count: null,
						},
					]),
					state: "complete",
				},
			]),
		];
		expect(canvasFromMessages(messages)?.kind).toBe("workspace-inventory");
	});

	it("maps run_sql to a result-grid using the call arguments (not the result)", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "run_sql",
					arguments: JSON.stringify({
						sql: "SELECT * FROM orders",
						params: [],
					}),
					state: "complete",
					output: { columns: ["id"], rows: [{ id: 1 }], rowCount: 1 },
				},
			]),
		];
		expect(canvasFromMessages(messages)).toEqual({
			kind: "result-grid",
			sql: "SELECT * FROM orders",
		});
	});

	it("returns null when there are no tool parts", () => {
		expect(
			canvasFromMessages([msg([{ type: "text", content: "hi" }])]),
		).toBeNull();
	});

	it("uses the latest tool result when several are present", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "list_sources",
					arguments: "{}",
					state: "complete",
					output: [],
				},
				{
					type: "tool-call",
					id: "c2",
					name: "list_tables",
					arguments: "{}",
					state: "complete",
					output: [],
				},
			]),
		];
		expect(canvasFromMessages(messages)?.kind).toBe("workspace-inventory");
	});

	it("does not let a trailing non-canvas tool shadow the last canvas result", () => {
		// connect (maps to schema-preview) then list_verticals (no canvas projector).
		// The canvas must STAY on the connect result, not collapse to null because a
		// non-canvas tool completed last. This is the substrate of the refused-select
		// stuck-spinner fix: that turn ends on a non-canvas list_verticals, yet the
		// canvas must still reconcile to a real result instead of spinning forever.
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "connect",
					arguments: "{}",
					state: "complete",
					output: { tables: [{ name: "t", columns: [] }] },
				},
				{
					type: "tool-call",
					id: "c2",
					name: "list_verticals",
					arguments: "{}",
					state: "complete",
					output: [{ name: "finance" }],
				},
			]),
		];
		expect(canvasFromMessages(messages)?.kind).toBe("schema-preview");
	});
});

describe("canvasFromCallId (DAT-354 rehydration)", () => {
	const twoCalls = [
		msg([
			{
				type: "tool-call",
				id: "c1",
				name: "list_sources",
				arguments: "{}",
				state: "complete",
				output: [{ source_id: "s1" }],
			},
			{
				type: "tool-call",
				id: "c2",
				name: "list_tables",
				arguments: "{}",
				state: "complete",
				output: [{ table_id: "t1" }],
			},
		]),
	];

	it("resolves an EARLIER call's result, not just the latest", () => {
		// The latest is list_tables (c2); addressing c1 by id rehydrates the
		// earlier source-list.
		expect(canvasFromCallId(twoCalls, "c1")?.kind).toBe("source-list");
		expect(canvasFromCallId(twoCalls, "c2")?.kind).toBe("workspace-inventory");
	});

	it("reads run_sql's call arguments for the result-grid, like canvasFromMessages", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "q1",
					name: "run_sql",
					arguments: JSON.stringify({ sql: "SELECT 1" }),
					state: "complete",
					output: { columns: [], rows: [], rowCount: 0 },
				},
			]),
		];
		expect(canvasFromCallId(messages, "q1")).toEqual({
			kind: "result-grid",
			sql: "SELECT 1",
		});
	});

	it("reads a result from a correlated tool-result part", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "list_tables",
					arguments: "{}",
					state: "complete",
				},
				{
					type: "tool-result",
					toolCallId: "c1",
					content: JSON.stringify([{ table_id: "t1" }]),
				},
			]),
		];
		expect(canvasFromCallId(messages, "c1")?.kind).toBe("workspace-inventory");
	});

	it("returns null for an unknown call id", () => {
		expect(canvasFromCallId(twoCalls, "nope")).toBeNull();
	});

	it("returns null for a not-yet-complete call (no output)", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "list_sources",
					arguments: "{}",
					state: "partial-call",
				},
			]),
		];
		expect(canvasFromCallId(messages, "c1")).toBeNull();
	});

	it("returns null for a display-only tool (teach maps to no canvas member)", () => {
		const messages = [
			msg([
				{
					type: "tool-call",
					id: "c1",
					name: "teach",
					arguments: JSON.stringify({ type: "null_value", payload: {} }),
					state: "complete",
					output: { overlay_id: "ov1", type: "null_value" },
				},
			]),
		];
		expect(canvasFromCallId(messages, "c1")).toBeNull();
	});
});
