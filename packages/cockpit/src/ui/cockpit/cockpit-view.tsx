// Three-region cockpit view (DAT-347, C1).
//
// The inner agentic surface that fills the C0 shell's cockpit route. Two top-
// level regions side by side:
//   - the chat rail (left), and
//   - a stacked region (right) of the stage navigator over the focus canvas.
// Each region scrolls independently. Cockpit-scoped hotkeys focus the chat
// input (mod+/) and the canvas (mod+.) without colliding with the shell's ⌘K.
// All sizes/colors read from theme tokens — no hardcoded px/hex.

import { Box, Group, Stack } from "@mantine/core";
import { useHotkeys } from "@mantine/hooks";
import { useRef } from "react";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { FocusCanvas } from "#/ui/cockpit/focus-canvas";
import { StageNavigator } from "#/ui/cockpit/stage-navigator";
import { tokens } from "#/ui/theme";

export function CockpitView() {
	const { canvasState } = useCockpit();
	const chatRef = useRef<HTMLDivElement>(null);
	const canvasRef = useRef<HTMLDivElement>(null);

	// Cockpit-scoped focus hotkeys. mod+slash → chat input; mod+period → canvas.
	// Scoped to this view (registered while mounted) so they don't leak into
	// other sections; ⌘K stays the shell's command palette.
	useHotkeys([
		[
			"mod+/",
			() =>
				chatRef.current
					?.querySelector<HTMLTextAreaElement>('[data-testid="chat-input"]')
					?.focus(),
		],
		["mod+.", () => canvasRef.current?.focus()],
	]);

	return (
		<Group
			align="stretch"
			gap="md"
			wrap="nowrap"
			h="100%"
			data-testid="cockpit-view"
		>
			<Box
				ref={chatRef}
				data-testid="region-chat"
				style={{
					width: "30%",
					minWidth: "20rem",
					borderRightWidth: 1,
					borderRightStyle: "solid",
					borderRightColor: tokens.colors.border,
					overflow: "hidden",
				}}
			>
				<ChatRail />
			</Box>
			<Stack
				gap="md"
				style={{ flex: 1, overflow: "hidden" }}
				data-testid="region-work"
			>
				<StageNavigator />
				<Box
					ref={canvasRef}
					tabIndex={-1}
					data-testid="region-canvas"
					style={{
						flex: 1,
						overflowY: "auto",
						backgroundColor: tokens.colors.surface,
						borderWidth: 1,
						borderStyle: "solid",
						borderColor: tokens.colors.border,
						borderRadius: tokens.radii.md,
						padding: tokens.spacing.md,
					}}
				>
					<FocusCanvas state={canvasState} />
				</Box>
			</Stack>
		</Group>
	);
}
