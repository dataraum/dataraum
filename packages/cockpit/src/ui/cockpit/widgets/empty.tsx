// Empty-canvas widget (DAT-347).
//
// The resting state of the focus canvas: nothing to show yet. Reads theme
// tokens only — no hardcoded px/hex.

import { Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function EmptyWidget(_props: {
	state: Extract<CanvasState, { kind: "empty" }>;
}) {
	return (
		<Stack
			gap="xs"
			align="center"
			justify="center"
			h="100%"
			data-testid="canvas-empty"
		>
			<Text c="dimmed" size="sm">
				Ask the agent to get started — results appear here.
			</Text>
		</Stack>
	);
}
