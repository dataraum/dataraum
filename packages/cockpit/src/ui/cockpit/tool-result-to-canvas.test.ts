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
				source_status: null,
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

	it("maps frame to a concept-frame canvas (DAT-382)", () => {
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
		};
		expect(toolResultToCanvas("frame", frame)).toEqual({
			kind: "concept-frame",
			frame,
		});
	});

	it("returns null for a missing frame result (canvas unchanged)", () => {
		expect(toolResultToCanvas("frame", null)).toBeNull();
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

	it("maps select to a selected-source canvas (DAT-398)", () => {
		const selection = {
			source_id: "s1",
			name: "orders",
			source_type: "csv",
			backend: null,
			stage: "add_source",
			file_uris: ["s3://dataraum-lake/orders.csv"],
			recipe_tables: null,
		};
		expect(toolResultToCanvas("select", selection)).toEqual({
			kind: "selected-source",
			selection,
		});
	});

	it("returns null for a missing select result (canvas unchanged)", () => {
		expect(toolResultToCanvas("select", null)).toBeNull();
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

	it("tolerates a missing result array", () => {
		expect(toolResultToCanvas("list_sources", undefined)).toEqual({
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
		// non-canvas tool completed last. This is the substrate of the denied-select
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
