// Error-canvas widget (DAT-347, C1).
//
// Renders a tool/stream error message. Doubles as the focus canvas's fallback
// when a CanvasState kind has no registered widget (a partially-landed C2-C6
// member degrades to this instead of crashing). Reads theme tokens only.

import { Alert } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function ErrorWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "error" }>;
}) {
	return (
		<Alert
			color="red"
			variant="light"
			title="Something went wrong"
			data-testid="canvas-error"
		>
			{state.message}
		</Alert>
	);
}
