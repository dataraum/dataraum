// Chat rail (DAT-347 C1, rebuilt on the TanStack AI SDK for DAT-353).
//
// useChat owns the conversation + the agentic tool-loop + SSE transport; this
// component only renders messages and drives the canvas. As messages arrive:
//   - text parts        → rendered as user / assistant bubbles
//   - tool-call parts    → a collapsible card; when the SDK pauses for approval
//                          (needsApproval tools: teach / replay) the card shows
//                          Approve / Deny → addToolApprovalResponse
//   - the latest tool result → projected to the focus canvas via the bridge
//                          (list_sources / list_tables render as widgets;
//                          write/compute results stay in the rail)
// Streaming is driven ONLY by user submit (never on mount → SSR-safe).

import {
	ActionIcon,
	Box,
	Button,
	Card,
	Collapse,
	Group,
	Loader,
	Stack,
	Text,
	Textarea,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { fetchServerSentEvents, useChat } from "@tanstack/ai-react";
import { SendHorizontal } from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { canvasFromMessages } from "#/ui/cockpit/tool-result-to-canvas";
import { tokens } from "#/ui/theme";

// The untyped tool-call part shape (we register tools server-side, so useChat
// sees them untyped). Narrowed off `part.type === "tool-call"`.
interface ToolCallPart {
	type: "tool-call";
	id: string;
	name: string;
	state: string;
	approval?: { id: string; needsApproval: boolean; approved?: boolean };
	output?: unknown;
}

function ToolCallCard({
	part,
	onApprove,
}: {
	part: ToolCallPart;
	onApprove: (approvalId: string, approved: boolean) => void;
}) {
	const [opened, { toggle }] = useDisclosure(false);
	const done = part.state === "complete";
	const approvalId = part.approval?.id;
	const awaitingApproval =
		part.state === "approval-requested" &&
		approvalId !== undefined &&
		part.approval?.approved === undefined;

	return (
		<Card
			withBorder
			padding="xs"
			radius="sm"
			data-testid={`tool-call-${part.id}`}
		>
			<Group
				justify="space-between"
				wrap="nowrap"
				onClick={toggle}
				style={{ cursor: "pointer" }}
			>
				<Text size="sm" fw={600}>
					{part.name}
				</Text>
				{done ? (
					<Text size="xs" c="dimmed">
						{opened ? "hide" : "show"}
					</Text>
				) : (
					<Loader size="xs" />
				)}
			</Group>

			{awaitingApproval && approvalId && (
				<Group gap="xs" mt="xs" data-testid={`tool-approval-${part.id}`}>
					<Button
						size="xs"
						onClick={() => onApprove(approvalId, true)}
						data-testid={`tool-approve-${part.id}`}
					>
						Approve
					</Button>
					<Button
						size="xs"
						variant="default"
						onClick={() => onApprove(approvalId, false)}
						data-testid={`tool-deny-${part.id}`}
					>
						Deny
					</Button>
				</Group>
			)}

			<Collapse expanded={opened && part.output !== undefined}>
				<Text
					size="xs"
					c="dimmed"
					ff="monospace"
					style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}
					data-testid={`tool-call-result-${part.id}`}
				>
					{JSON.stringify(part.output, null, 2)}
				</Text>
			</Collapse>
		</Card>
	);
}

export function ChatRail() {
	const { setCanvasState } = useCockpit();
	const { messages, sendMessage, isLoading, error, addToolApprovalResponse } =
		useChat({ connection: fetchServerSentEvents("/api/chat") });
	const [input, setInput] = useState("");

	// Project the latest tool result onto the focus canvas as messages arrive.
	useEffect(() => {
		const next = canvasFromMessages(messages);
		if (next) setCanvasState(next);
	}, [messages, setCanvasState]);

	// Surface a stream error on the canvas.
	useEffect(() => {
		if (error) setCanvasState({ kind: "error", message: error.message });
	}, [error, setCanvasState]);

	const onSubmit = (e: FormEvent) => {
		e.preventDefault();
		const text = input.trim();
		if (!text || isLoading) return;
		setInput("");
		setCanvasState({ kind: "loading" });
		void sendMessage(text);
	};

	return (
		<Stack gap="sm" h="100%" data-testid="chat-rail">
			<Box style={{ flex: 1, overflowY: "auto" }} data-testid="chat-messages">
				<Stack gap="xs" p="xs">
					{messages.map((m) =>
						m.parts.map((part, i) => {
							if (part.type === "text") {
								return (
									<Text
										// Parts within a message are append-only; index keys are stable.
										// biome-ignore lint/suspicious/noArrayIndexKey: append-only parts
										key={`${m.id}-${i}`}
										size="sm"
										c={m.role === "user" ? "text" : "dimmed"}
										fw={m.role === "user" ? 600 : 400}
										style={{ whiteSpace: "pre-wrap" }}
									>
										{part.content}
									</Text>
								);
							}
							if (part.type === "tool-call") {
								return (
									<ToolCallCard
										key={part.id}
										part={part as ToolCallPart}
										onApprove={(approvalId, approved) =>
											void addToolApprovalResponse({ id: approvalId, approved })
										}
									/>
								);
							}
							return null;
						}),
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
						disabled={isLoading}
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
						disabled={isLoading || input.trim().length === 0}
						style={{ borderRadius: tokens.radii.sm }}
					>
						<SendHorizontal size={18} />
					</ActionIcon>
				</Group>
			</form>
		</Stack>
	);
}
