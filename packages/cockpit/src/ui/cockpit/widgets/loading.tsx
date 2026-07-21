// Loading-canvas widget (DAT-347).
//
// Shown while a tool call is in flight before its result maps to a richer
// canvas member. Reads theme tokens only.

import { Loader, Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function LoadingWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "loading" }>;
}) {
	return (
		<Stack
			gap="sm"
			align="center"
			justify="center"
			h="100%"
			data-testid="canvas-loading"
		>
			<Loader size="sm" />
			<Text c="dimmed" size="sm">
				{state.label ?? "Working…"}
			</Text>
		</Stack>
	);
}
