// The chat composer — the one input the user types into, in the working chat
// rail. A single bordered "bubble" (text area + a bottom control row), modelled
// on the familiar assistant composer: the chat-type drop-up sits quietly in the
// bottom-left (a light indicator / manual switch — language is the primary nav),
// the send button in the bottom-right. Send appears only once there's text;
// while a turn streams it becomes Stop (aborts the SSE → the server's Anthropic
// call, see /api/chat). Owns only the input + submit; messages/canvas/typeNav
// come from the provider.

import { ActionIcon, Box, Group, Stack, Textarea } from "@mantine/core";
import { SendHorizontal, Square } from "lucide-react";
import { type FormEvent, useState } from "react";
import { ChatSwitcher } from "#/ui/cockpit/chat-switcher";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { tokens } from "#/ui/theme";

export function Composer() {
	const { sendMessage, stop, isLoading, typeNav } = useCockpit();
	const [input, setInput] = useState("");
	const hasText = input.trim().length > 0;

	const submit = () => {
		const text = input.trim();
		if (!text || isLoading) return;
		setInput("");
		sendMessage(text);
	};

	const onSubmit = (e: FormEvent) => {
		e.preventDefault();
		submit();
	};

	return (
		<form onSubmit={onSubmit} data-testid="chat-form" style={{ width: "100%" }}>
			<Box
				style={{
					border: `1px solid ${tokens.colors.border}`,
					borderRadius: tokens.radii.md,
					backgroundColor: tokens.colors.surface,
					padding: tokens.spacing.xs,
				}}
			>
				<Stack gap={4}>
					<Textarea
						value={input}
						onChange={(e) => setInput(e.currentTarget.value)}
						placeholder="Ask about your data…"
						// Borderless — the bubble Box is the single frame, so the input and
						// the control row read as one composer (not a box inside a box).
						variant="unstyled"
						// Grows with the message (capped) instead of a fixed height — the
						// composer expanding as you type is expected feedback, not jitter.
						autosize
						minRows={2}
						maxRows={6}
						size="sm"
						data-testid="chat-input"
						// Not disabled while a turn streams — disabling greys the input
						// (Mantine), and we want it to stay white + composable. The submit
						// guard (isLoading) blocks Enter, and Send is replaced by Stop, so
						// a turn can't be double-sent.
						onKeyDown={(e) => {
							// Enter sends; Shift+Enter is a newline.
							if (e.key === "Enter" && !e.shiftKey) {
								e.preventDefault();
								submit();
							}
						}}
					/>
					{/* Bottom control row: the type drop-up (folds UP) bottom-left, the
					    send/stop button bottom-right. The drop-up is absent off-route
					    (no typeNav — the unit tests, the degraded path). */}
					<Group justify="space-between" wrap="nowrap" align="center" gap="xs">
						<Box>{typeNav && <ChatSwitcher {...typeNav} />}</Box>
						{isLoading ? (
							<ActionIcon
								type="button"
								variant="light"
								color="red"
								size="lg"
								aria-label="Stop generating"
								data-testid="chat-stop"
								onClick={stop}
								style={{ borderRadius: tokens.radii.sm }}
							>
								<Square size={16} />
							</ActionIcon>
						) : hasText ? (
							<ActionIcon
								type="submit"
								variant="filled"
								size="lg"
								aria-label="Send message"
								data-testid="chat-send"
								style={{ borderRadius: tokens.radii.sm }}
							>
								<SendHorizontal size={18} />
							</ActionIcon>
						) : null}
					</Group>
				</Stack>
			</Box>
		</form>
	);
}
