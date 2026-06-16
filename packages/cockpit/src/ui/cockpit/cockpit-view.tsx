// Cockpit view (DAT-347, C1; reshaped in the redesign).
//
// Cold start → the calm centered landing. Once a conversation exists → the
// working split: the chat rail (left) beside the focus canvas (right), each
// scrolling independently. The swap happens exactly ONCE (first turn), then the
// layout holds — no per-turn reflow (the explicit "not nervous" requirement).
// Cockpit-scoped hotkeys focus the chat input (mod+/) and the canvas (mod+.).
// All sizes/colors read from theme tokens — no hardcoded px/hex.

import { Box, Button, Group, Stack, Text } from "@mantine/core";
import { useHotkeys } from "@mantine/hooks";
import { useRef } from "react";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { CockpitLanding } from "#/ui/cockpit/cockpit-landing";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { FocusCanvas } from "#/ui/cockpit/focus-canvas";
import { tokens } from "#/ui/theme";

export function CockpitView() {
	const { canvas, pinnedCallId, returnToLive, messages } = useCockpit();
	const chatRef = useRef<HTMLDivElement>(null);
	const canvasRef = useRef<HTMLDivElement>(null);

	// Cockpit-scoped focus hotkeys. mod+slash → chat input; mod+period → canvas.
	// Registered while mounted so they don't leak into other sections; ⌘K stays
	// the shell's command palette. The chat input shares its data-testid across
	// the landing + rail composers, so mod+/ works in both modes.
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

	// Cold start: no conversation yet → the centered landing instead of an empty
	// split. mod+/ still finds the (hero) composer inside it.
	if (messages.length === 0) {
		return (
			<Box ref={chatRef} h="100%" data-testid="cockpit-view">
				<CockpitLanding />
			</Box>
		);
	}

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
					// The chat is a RELATIVE column: it holds 28% of the work area so it
					// scales WITH the window (chat and canvas grow together), with a 22rem
					// floor so it never collapses below readable on a laptop. No upper
					// clamp — on a wide monitor the chat keeps its proportion instead of
					// capping while the canvas hogs the slack (DAT-527, from real usage:
					// the chat content earns the proportional room). No divider border —
					// the raised canvas card edge is the only, quiet separation.
					width: "28%",
					minWidth: "22rem",
					flexShrink: 0,
					overflow: "hidden",
				}}
			>
				<ChatRail />
			</Box>
			<Stack
				gap="md"
				// The canvas takes the slack (flex:1) but holds a 28rem floor so the chat's
				// 22rem floor + flexShrink:0 can't squeeze it toward zero on a narrow
				// window (DAT-527 review): below ~50rem the nowrap row overflows to a
				// horizontal scroll rather than collapsing the canvas.
				style={{ flex: 1, minWidth: "28rem", overflow: "hidden" }}
				data-testid="region-work"
			>
				{/* Rehydration banner (DAT-354): shown only while the canvas is pinned
				    to a past tool result, so the user knows they're viewing history and
				    can snap back to the latest live result. */}
				{pinnedCallId && (
					<Group
						justify="space-between"
						wrap="nowrap"
						gap="sm"
						data-testid="history-banner"
						style={{
							backgroundColor: tokens.colors.surface,
							borderWidth: 1,
							borderStyle: "solid",
							borderColor: tokens.colors.border,
							borderRadius: tokens.radii.sm,
							padding: tokens.spacing.xs,
						}}
					>
						<Text size="sm" c="dimmed">
							Viewing history
						</Text>
						<Button
							size="xs"
							variant="light"
							onClick={returnToLive}
							data-testid="return-to-live"
						>
							Return to live
						</Button>
					</Group>
				)}
				<Box
					ref={canvasRef}
					tabIndex={-1}
					data-testid="region-canvas"
					style={{
						flex: 1,
						minHeight: 0,
						overflowY: "auto",
						backgroundColor: tokens.colors.surface,
						borderWidth: 1,
						borderStyle: "solid",
						borderColor: tokens.colors.border,
						borderRadius: tokens.radii.md,
						padding: tokens.spacing.md,
					}}
				>
					<FocusCanvas state={canvas} />
				</Box>
			</Stack>
		</Group>
	);
}
