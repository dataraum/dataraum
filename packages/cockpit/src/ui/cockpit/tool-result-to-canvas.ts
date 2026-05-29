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
import type { SourceSummary } from "#/tools/list-sources";
import type { TableSummary } from "#/tools/list-tables";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/**
 * Map a single tool result to the canvas. `null` = leave the canvas as-is
 * (the result still shows in the chat rail's tool card).
 */
export function toolResultToCanvas(
	toolName: string,
	result: unknown,
): CanvasState | null {
	switch (toolName) {
		case "list_sources":
			return {
				kind: "source-list",
				sources: (result as SourceSummary[]) ?? [],
			};
		case "list_tables":
			return { kind: "table-list", tables: (result as TableSummary[]) ?? [] };
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
	let latest: { name: string; output: unknown } | null = null;
	const nameByCallId = new Map<string, string>();

	for (const message of messages) {
		for (const part of message.parts) {
			if (part.type === "tool-call") {
				nameByCallId.set(part.id, part.name);
				if (part.output !== undefined) {
					latest = { name: part.name, output: part.output };
				}
			} else if (part.type === "tool-result") {
				const name = nameByCallId.get(part.toolCallId);
				if (name) {
					let output: unknown = part.content;
					try {
						output = JSON.parse(part.content);
					} catch {
						// content wasn't JSON — keep the raw string.
					}
					latest = { name, output };
				}
			}
		}
	}

	return latest ? toolResultToCanvas(latest.name, latest.output) : null;
}
