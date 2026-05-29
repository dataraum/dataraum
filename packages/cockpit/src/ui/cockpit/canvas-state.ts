// What the focus canvas is currently showing (DAT-347, C1).
//
// A discriminated union over `kind`. C1 ships ONLY the baseline members below;
// C2-C6 each add one member here, one widget file, one register() line, and one
// tool→canvas mapper case — they never touch the canvas/stream/shell plumbing.
// Keep the union sorted baseline-first so the extension point is obvious.

export type CanvasState =
	| { kind: "empty" }
	| { kind: "loading" }
	| { kind: "error"; message: string };

/** Every `kind` a canvas member can have — handy for registry/test exhaustion. */
export type CanvasKind = CanvasState["kind"];
