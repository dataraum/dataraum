// Tool-result → canvas mapper (DAT-347, C1).
//
// Pure function: given a tool name and its result, decide what the focus canvas
// should show next. This is the OTHER half of the register-don't-replace
// contract — a C2-C6 column adds ONE case here (mapping its tool's result to its
// new CanvasState member) plus its widget + register() line. It never edits the
// chat rail or the canvas.
//
// C1 has no rich widgets, so every tool result maps to `empty` (the chat rail
// still renders the textual result in its tool-call card). The signature is the
// extension seam.

import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function toolResultToCanvas(
	_toolName: string,
	_result: unknown,
): CanvasState {
	// C2-C6: switch on _toolName and project _result into a richer member, e.g.
	//   case "preview_table": return { kind: "table-preview", rows: ... };
	return { kind: "empty" };
}
