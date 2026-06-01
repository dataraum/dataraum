import type { UIMessage } from "@tanstack/ai-react";
import { describe, expect, it } from "vitest";

import {
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
				source_id: "s1",
				name: "orders",
				source_type: "file",
				status: null,
				backend: null,
				created_at: "2026-01-01T00:00:00.000Z",
			},
		]);
		expect(state).toEqual({
			kind: "source-list",
			sources: [expect.objectContaining({ source_id: "s1" })],
		});
	});

	it("maps list_tables to a table-list canvas", () => {
		const state = toolResultToCanvas("list_tables", [
			{
				table_id: "t1",
				source_id: "s1",
				table_name: "orders",
				layer: "raw",
				row_count: 10,
			},
		]);
		expect(state?.kind).toBe("table-list");
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
		expect(canvasFromMessages(messages)?.kind).toBe("table-list");
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
		expect(canvasFromMessages(messages)?.kind).toBe("table-list");
	});
});
