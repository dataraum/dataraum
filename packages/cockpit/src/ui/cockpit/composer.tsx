// Shared chat composer — the one input the user types into, used by BOTH the
// centered landing (variant "hero": large, inviting) and the working chat rail
// (variant "rail": compact). Extracted from chat-rail in the redesign so the two
// surfaces can never drift. Owns only the input + submit; messages/canvas stay
// the provider's. While a turn streams, Send becomes Stop (aborts the SSE → the
// server's Anthropic call, see /api/chat).

import { ActionIcon, Group, Textarea } from "@mantine/core";
import { SendHorizontal, Square } from "lucide-react";
import { type FormEvent, useState } from "react";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { tokens } from "#/ui/theme";

export function Composer({ variant = "rail" }: { variant?: "hero" | "rail" }) {
	const { sendMessage, stop, isLoading } = useCockpit();
	const [input, setInput] = useState("");
	const hero = variant === "hero";

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
			<Group gap="xs" wrap="nowrap" align="flex-end" p={hero ? 0 : "xs"}>
				<Textarea
					value={input}
					onChange={(e) => setInput(e.currentTarget.value)}
					placeholder="Ask about your data…"
					// Grows with the message (capped) instead of a fixed height — the
					// composer expanding as you type is expected feedback, not layout jitter.
					autosize
					minRows={hero ? 3 : 2}
					maxRows={hero ? 12 : 6}
					size={hero ? "md" : "sm"}
					radius="md"
					style={{ flex: 1 }}
					data-testid="chat-input"
					disabled={isLoading}
					onKeyDown={(e) => {
						// Enter sends; Shift+Enter is a newline.
						if (e.key === "Enter" && !e.shiftKey) {
							e.preventDefault();
							submit();
						}
					}}
				/>
				{isLoading ? (
					<ActionIcon
						type="button"
						variant="light"
						color="red"
						size={hero ? "xl" : "lg"}
						aria-label="Stop generating"
						data-testid="chat-stop"
						onClick={stop}
						style={{ borderRadius: tokens.radii.sm }}
					>
						<Square size={hero ? 18 : 16} />
					</ActionIcon>
				) : (
					<ActionIcon
						type="submit"
						variant="filled"
						size={hero ? "xl" : "lg"}
						aria-label="Send message"
						data-testid="chat-send"
						disabled={input.trim().length === 0}
						style={{ borderRadius: tokens.radii.sm }}
					>
						<SendHorizontal size={hero ? 20 : 18} />
					</ActionIcon>
				)}
			</Group>
		</form>
	);
}
