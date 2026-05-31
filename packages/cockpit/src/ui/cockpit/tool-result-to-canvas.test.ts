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
