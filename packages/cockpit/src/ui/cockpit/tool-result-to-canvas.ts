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

/** Projects one tool's result (+ call input) to a CanvasState, or null to leave
 * the canvas unchanged. */
type CanvasProjector = (result: unknown, input: unknown) => CanvasState | null;

/**
 * The canonical tool → canvas projector map — the SINGLE source of truth for
 * which tools produce a canvas member. A new canvas tool adds ONE entry here
 * (plus its widget + one register() line); `CANVAS_TOOLS` / `isCanvasTool`
 * derive from these keys, so the chat rail's chip clickability never needs a
 * second, hand-maintained list (DAT-354 de-dup). Tools absent from the map
 * (teach / replay / probe) project nothing → their chips are display-only.
 */
const PROJECTORS: Record<string, CanvasProjector> = {
	list_sources: (result) => ({
		kind: "source-list",
		sources: (result as SourceSummary[]) ?? [],
	}),
	list_tables: (result) => ({
		kind: "table-list",
		tables: (result as TableSummary[]) ?? [],
	}),
	// The per-table readiness grid; a missing result (e.g. an errored read)
	// leaves the canvas unchanged.
	look_table: (result) =>
		result
			? { kind: "table-readiness", readiness: result as LookTableResult }
			: null,
	// The per-column explanation; a missing result leaves the canvas as-is.
	why_column: (result) =>
		result ? { kind: "column-why", why: result as WhyColumnResult } : null,
	// null/undefined → leave the canvas unchanged (a failed connect surfaces its
	// error in the chat rail, not as an empty preview).
	connect: (result) =>
		result ? { kind: "schema-preview", schema: result as ConnectSchema } : null,
	// Declared concepts render as the ConceptFrame widget; missing → unchanged.
	frame: (result) =>
		result ? { kind: "concept-frame", frame: result as FrameResult } : null,
	// The persisted Source descriptor renders as SelectedSource; a missing result
	// (e.g. a rejected duplicate-basename select) leaves the canvas unchanged.
	select: (result) =>
		result
			? { kind: "selected-source", selection: result as SelectResult }
			: null,
	// The agent's run_sql returns a small sample for the LLM; the human grid
	// re-issues the query against the stateless streaming endpoint, so it maps
	// from the CALL INPUT (sql + bind params), not the result. No sql → unchanged.
	run_sql: (_result, input) => {
		const args = (input ?? {}) as { sql?: unknown; params?: unknown };
		if (typeof args.sql !== "string" || args.sql.length === 0) return null;
		// Carry params ONLY when there are bind values: an empty array and
		// "absent" are the same query, so collapse them — else the grid's queryKey
		// flips between `[]` and `undefined` as the streamed args settle, re-issuing
		// the whole stream for no reason.
		const params =
			Array.isArray(args.params) && args.params.length > 0
				? (args.params as (string | number | boolean | null)[])
				: undefined;
		return params
			? { kind: "result-grid", sql: args.sql, params }
			: { kind: "result-grid", sql: args.sql };
	},
};

/**
 * The tool names whose result rehydrates the focus canvas — DERIVED from the
 * projector map, so there is no second hand-maintained list (DAT-354 de-dup).
 */
export const CANVAS_TOOLS: ReadonlySet<string> = new Set(
	Object.keys(PROJECTORS),
);

/** A tool whose result maps to a canvas member → its chip is clickable. */
export function isCanvasTool(toolName: string): boolean {
	return toolName in PROJECTORS;
}

/**
 * Map a single tool result to the canvas. `null` = leave the canvas as-is
 * (the result still shows in the chat rail's tool card).
 */
export function toolResultToCanvas(
	toolName: string,
	result: unknown,
	input?: unknown,
): CanvasState | null {
	return PROJECTORS[toolName]?.(result, input) ?? null;
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
 * Resolve ONE specific tool-call by its id and map it to the canvas (DAT-354
 * rehydration). Walks the message list for the call's name + input and its
 * completed output (tolerating both the `output`-on-the-call shape and a
 * correlated `tool-result` part, mirroring `canvasFromMessages`), then reuses
 * the same `toolResultToCanvas` mapper. Returns null when the call id isn't
 * found, hasn't completed, or maps to no canvas member (teach/replay/probe) —
 * the caller then leaves the canvas unchanged (those chips are display-only).
 */
export function canvasFromCallId(
	messages: ReadonlyArray<UIMessage>,
	callId: string,
): CanvasState | null {
	let name: string | undefined;
	let input: unknown;
	let output: unknown;
	let hasOutput = false;

	for (const message of messages) {
		for (const part of message.parts) {
			if (part.type === "tool-call" && part.id === callId) {
				name = part.name;
				input = parseToolArguments(part);
				if (part.output !== undefined) {
					output = part.output;
					hasOutput = true;
				}
			} else if (part.type === "tool-result" && part.toolCallId === callId) {
				let parsed: unknown = part.content;
				try {
					parsed = JSON.parse(part.content);
				} catch {
					// content wasn't JSON — keep the raw string.
				}
				output = parsed;
				hasOutput = true;
			}
		}
	}

	if (name === undefined || !hasOutput) return null;
	return toolResultToCanvas(name, output, input);
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
