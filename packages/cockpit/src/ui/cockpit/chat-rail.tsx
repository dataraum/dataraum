// Chat rail (DAT-347, C1) — the left region of the three-region cockpit.
//
// A scrollable message list, collapsible tool-call cards, and a submit input.
// Streaming is driven ONLY by user submit (never on mount → SSR-safe). As the
// stream arrives:
//   - `text`            → appended to the in-progress assistant message
//   - `tool_call_start` → a collapsible card is opened (pending)
//   - `tool_result`     → the card is filled AND the focus canvas is updated via
//                         the tool→canvas mapper
//   - `error`           → an error bubble + the canvas flips to the error widget
// Reads theme tokens only.

import {
	ActionIcon,
	Box,
	Card,
	Collapse,
	Group,
	Loader,
	Stack,
	Text,
	Textarea,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { SendHorizontal } from "lucide-react";
import { type FormEvent, useCallback, useRef, useState } from "react";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { toolResultToCanvas } from "#/ui/cockpit/tool-result-to-canvas";
import {
	type ChatMessage,
	type ChatStreamEvent,
	useChatStream,
} from "#/ui/cockpit/use-chat-stream";
import { tokens } from "#/ui/theme";

interface ToolCall {
	id: string;
	name: string;
	result?: unknown;
}

function ToolCallCard({ call }: { call: ToolCall }) {
	const [opened, { toggle }] = useDisclosure(false);
	const pending = call.result === undefined;
	return (
		<Card
			withBorder
			padding="xs"
			radius="sm"
			data-testid={`tool-call-${call.id}`}
		>
			<Group
				justify="space-between"
				wrap="nowrap"
				onClick={toggle}
				style={{ cursor: "pointer" }}
			>
				<Text size="sm" fw={600}>
					{call.name}
				</Text>
				{pending ? (
					<Loader size="xs" />
				) : (
					<Text size="xs" c="dimmed">
						{opened ? "hide" : "show"}
					</Text>
				)}
			</Group>
			<Collapse expanded={opened}>
				<Text
					size="xs"
					c="dimmed"
					ff="monospace"
					style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}
					data-testid={`tool-call-result-${call.id}`}
				>
					{pending ? "running…" : JSON.stringify(call.result, null, 2)}
				</Text>
			</Collapse>
		</Card>
	);
}

type RailMessage =
	| { kind: "text"; role: "user" | "assistant"; content: string }
	| { kind: "tool"; call: ToolCall };

export function ChatRail() {
	const { setCanvasState } = useCockpit();
	const { streaming, send } = useChatStream();
	const [messages, setMessages] = useState<RailMessage[]>([]);
	const [input, setInput] = useState("");
	// Index of the assistant message currently being streamed into.
	const assistantIndex = useRef<number | null>(null);

	const handleEvent = useCallback(
		(event: ChatStreamEvent) => {
			switch (event.type) {
				case "text":
					setMessages((prev) => {
						const next = [...prev];
						const i = assistantIndex.current;
						if (i !== null && next[i]?.kind === "text") {
							const msg = next[i] as Extract<RailMessage, { kind: "text" }>;
							next[i] = { ...msg, content: msg.content + event.text };
						}
						return next;
					});
					break;
				case "tool_call_start":
					setMessages((prev) => [
						...prev,
						{ kind: "tool", call: { id: event.id, name: event.name } },
					]);
					break;
				case "tool_result":
					setMessages((prev) =>
						prev.map((m) =>
							m.kind === "tool" && m.call.id === event.id
								? { ...m, call: { ...m.call, result: event.result } }
								: m,
						),
					);
					// The mapper decides what the focus canvas shows next.
					setCanvasState(toolResultToCanvas(event.name, event.result));
					break;
				case "error":
					setMessages((prev) => [
						...prev,
						{ kind: "text", role: "assistant", content: `⚠ ${event.message}` },
					]);
					setCanvasState({ kind: "error", message: event.message });
					break;
				case "done":
					assistantIndex.current = null;
					break;
			}
		},
		[setCanvasState],
	);

	const onSubmit = useCallback(
		(e: FormEvent) => {
			e.preventDefault();
			const text = input.trim();
			if (!text || streaming) return;

			// Append the user message + an empty assistant message to stream into.
			setMessages((prev) => {
				const next: RailMessage[] = [
					...prev,
					{ kind: "text", role: "user", content: text },
					{ kind: "text", role: "assistant", content: "" },
				];
				assistantIndex.current = next.length - 1;
				return next;
			});
			setInput("");

			// The wire format is text-only user/assistant turns; tool cards are
			// rail-local UI, not part of the request history.
			const history: ChatMessage[] = messages
				.filter(
					(m): m is Extract<RailMessage, { kind: "text" }> => m.kind === "text",
				)
				.map((m) => ({ role: m.role, content: m.content }));
			history.push({ role: "user", content: text });

			// Canvas shows progress while the turn streams.
			setCanvasState({ kind: "loading" });
			void send(history, { onEvent: handleEvent });
		},
		[input, streaming, messages, send, handleEvent, setCanvasState],
	);

	return (
		<Stack gap="sm" h="100%" data-testid="chat-rail">
			<Box style={{ flex: 1, overflowY: "auto" }} data-testid="chat-messages">
				<Stack gap="xs" p="xs">
					{messages.map((m, i) =>
						m.kind === "tool" ? (
							<ToolCallCard key={m.call.id} call={m.call} />
						) : (
							<Text
								// Rail messages are append-only, so index keys are stable.
								// biome-ignore lint/suspicious/noArrayIndexKey: append-only list
								key={i}
								size="sm"
								c={m.role === "user" ? "text" : "dimmed"}
								fw={m.role === "user" ? 600 : 400}
								style={{ whiteSpace: "pre-wrap" }}
							>
								{m.content}
							</Text>
						),
					)}
				</Stack>
			</Box>
			<form onSubmit={onSubmit} data-testid="chat-form">
				<Group gap="xs" wrap="nowrap" p="xs">
					<Textarea
						value={input}
						onChange={(e) => setInput(e.currentTarget.value)}
						placeholder="Ask the agent…"
						rows={2}
						style={{ flex: 1 }}
						data-testid="chat-input"
						disabled={streaming}
						onKeyDown={(e) => {
							if (e.key === "Enter" && !e.shiftKey) {
								e.preventDefault();
								onSubmit(e);
							}
						}}
					/>
					<ActionIcon
						type="submit"
						variant="filled"
						size="lg"
						aria-label="Send message"
						data-testid="chat-send"
						disabled={streaming || input.trim().length === 0}
						style={{ borderRadius: tokens.radii.sm }}
					>
						<SendHorizontal size={18} />
					</ActionIcon>
				</Group>
			</form>
		</Stack>
	);
}
