// Tool-result → canvas bridge (DAT-347 seam, extended for DAT-353).
//
// Two pure functions, no React:
//   toolResultToCanvas(name, result)  — maps ONE tool's result to a CanvasState,
//     or null to leave the canvas unchanged (write/compute tools like teach/
//     replay render in the chat rail, not the canvas). A new canvas tool adds
//     one case here + its widget + one register() line — never edits the rail.
//   canvasFromMessages(messages)      — adapts the useChat message list to the
//     bridge: finds the latest completed tool result and maps it. Returns null
//     when there's nothing new to show.
//
// Row types are type-only imports (erased — no server code in the client bundle).

import type { UIMessage } from "@tanstack/ai-react";
import type { ConnectSchema } from "#/duckdb/connect";
import type { FrameResult } from "#/tools/frame";
import type { SourceSummary } from "#/tools/list-sources";
import type { TableSummary } from "#/tools/list-tables";
import type { LookTableResult } from "#/tools/look-table";
import type { SelectResult } from "#/tools/select";
import type { WhyColumnResult } from "#/tools/why-column";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/**
 * Map a single tool result to the canvas. `null` = leave the canvas as-is
 * (the result still shows in the chat rail's tool card).
 */
export function toolResultToCanvas(
	toolName: string,
	result: unknown,
	input?: unknown,
): CanvasState | null {
	switch (toolName) {
		case "list_sources":
			return {
				kind: "source-list",
				sources: (result as SourceSummary[]) ?? [],
			};
		case "list_tables":
			return { kind: "table-list", tables: (result as TableSummary[]) ?? [] };
		case "look_table":
			// The per-table readiness grid; a missing result (e.g. an errored read)
			// leaves the canvas unchanged.
			return result
				? { kind: "table-readiness", readiness: result as LookTableResult }
				: null;
		case "why_column":
			// The per-column explanation; a missing result leaves the canvas as-is.
			return result
				? { kind: "column-why", why: result as WhyColumnResult }
				: null;
		case "connect":
			// null/undefined → leave the canvas unchanged (e.g. a failed connect
			// surfaces its error in the chat rail, not as an empty preview).
			return result
				? { kind: "schema-preview", schema: result as ConnectSchema }
				: null;
		case "frame":
			// The frame result (declared concepts) renders as the ConceptFrame
			// widget; a missing result leaves the canvas unchanged.
			return result
				? { kind: "concept-frame", frame: result as FrameResult }
				: null;
		case "select":
			// The persisted Source descriptor (file_uris / recipe tables + the
			// advanced stage) renders as the SelectedSource widget; a missing result
			// (e.g. a rejected duplicate-basename select surfacing its error in the
			// chat rail) leaves the canvas unchanged.
			return result
				? { kind: "selected-source", selection: result as SelectResult }
				: null;
		case "run_sql": {
			// The agent's run_sql returns a small materialized sample for the LLM;
			// the human grid re-issues the query against the stateless streaming
			// endpoint, so it maps from the CALL INPUT (sql + bind params), not the
			// result. No sql on the wire → leave the canvas unchanged.
			const args = (input ?? {}) as { sql?: unknown; params?: unknown };
			if (typeof args.sql !== "string" || args.sql.length === 0) return null;
			// Carry params ONLY when there are bind values: an empty array and
			// "absent" are the same query, so collapse them. Otherwise the grid's
			// queryKey flips between `[]` and `undefined` as the streamed tool args
			// settle, re-issuing the whole stream for no reason.
			const params =
				Array.isArray(args.params) && args.params.length > 0
					? (args.params as (string | number | boolean | null)[])
					: undefined;
			return params
				? { kind: "result-grid", sql: args.sql, params }
				: { kind: "result-grid", sql: args.sql };
		}
		default:
			// teach / replay / unknown: no canvas projection.
			return null;
	}
}

/**
 * Adapt the useChat message list to the canvas. Walks every message part and
 * tracks the latest completed tool result, tolerating both shapes the SDK can
 * emit: an `output` on the `tool-call` part, or a correlated `tool-result` part
 * (content is JSON; fall back to the raw string if it isn't). Returns the mapped
 * CanvasState, or null when nothing maps (caller leaves the canvas unchanged).
 */
export function canvasFromMessages(
	messages: ReadonlyArray<UIMessage>,
): CanvasState | null {
	let latest: { name: string; output: unknown; input: unknown } | null = null;
	const callById = new Map<string, { name: string; input: unknown }>();

	for (const message of messages) {
		for (const part of message.parts) {
			if (part.type === "tool-call") {
				const input = parseToolArguments(part);
				callById.set(part.id, { name: part.name, input });
				if (part.output !== undefined) {
					latest = { name: part.name, output: part.output, input };
				}
			} else if (part.type === "tool-result") {
				const call = callById.get(part.toolCallId);
				if (call) {
					let output: unknown = part.content;
					try {
						output = JSON.parse(part.content);
					} catch {
						// content wasn't JSON — keep the raw string.
					}
					latest = { name: call.name, output, input: call.input };
				}
			}
		}
	}

	return latest
		? toolResultToCanvas(latest.name, latest.output, latest.input)
		: null;
}

/**
 * Lift a tool-call's input off the part's JSON `arguments` string (the SDK
 * carries the call input there). Tolerates a missing or non-JSON value →
 * undefined. The `run_sql` → result-grid mapping needs this: the grid re-issues
 * the agent's query, so it reads the SQL from the call input, not the result.
 */
function parseToolArguments(part: { arguments?: unknown }): unknown {
	const raw = part.arguments;
	if (typeof raw !== "string") return raw ?? undefined;
	try {
		return JSON.parse(raw);
	} catch {
		return undefined;
	}
}
