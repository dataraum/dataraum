// Focus canvas (DAT-347, C1).
//
// Renders whatever the cockpit's canvasState points at, by resolving the widget
// for its `kind` from the shared registry. If a kind has no registered widget —
// e.g. a C2-C6 member whose widget hasn't landed yet — it degrades to the error
// widget rather than crashing the whole view. This is the registry's payoff:
// partial landings stay safe.

import { canvasRegistry } from "#/ui/cockpit/canvas-registry";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function FocusCanvas({ state }: { state: CanvasState }) {
	const contract = canvasRegistry.resolve(state.kind);

	if (contract) {
		// Resolved by `state.kind`, so the widget's narrowed state IS this state.
		const Widget = contract.component;
		return (
			<div data-testid="focus-canvas" style={{ height: "100%" }}>
				<Widget state={state} />
			</div>
		);
	}

	// Unregistered kind: fall back to the error widget so a partially-landed
	// member degrades visibly instead of rendering nothing.
	const fallback = canvasRegistry.resolve("error");
	if (fallback) {
		const Fallback = fallback.component;
		return (
			<div data-testid="focus-canvas" style={{ height: "100%" }}>
				<Fallback
					state={{
						kind: "error",
						message: `No widget registered for canvas kind "${state.kind}".`,
					}}
				/>
			</div>
		);
	}

	// Error widget itself is somehow unregistered — last-resort empty render.
	return <div data-testid="focus-canvas" style={{ height: "100%" }} />;
}
