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
	Group,
	Loader,
	Stack,
	Text,
	Textarea,
} from "@mantine/core";
import { fetchServerSentEvents, useChat } from "@tanstack/ai-react";
import { SendHorizontal } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { isCanvasTool, toolChipSummary } from "#/ui/cockpit/tool-chip-summary";
import {
	canvasFromCallId,
	canvasFromMessages,
} from "#/ui/cockpit/tool-result-to-canvas";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";
import { tokens } from "#/ui/theme";

// The untyped tool-call part shape (we register tools server-side, so useChat
// sees them untyped). Narrowed off `part.type === "tool-call"`. `arguments` is
// the SDK's JSON-encoded call input, carried in EVERY state (incl. approval-
// requested) — the chip summary reads it so teach/replay are readable before
// they run.
interface ToolCallPart {
	type: "tool-call";
	id: string;
	name: string;
	state: string;
	approval?: { id: string; needsApproval: boolean; approved?: boolean };
	arguments?: unknown;
	output?: unknown;
}

/** Lift a tool-call's parsed input off the SDK part's JSON `arguments` string. */
function parseArguments(raw: unknown): unknown {
	if (typeof raw !== "string") return raw ?? undefined;
	try {
		return JSON.parse(raw);
	} catch {
		return undefined;
	}
}

function ToolCallCard({
	part,
	onApprove,
	onRehydrate,
}: {
	part: ToolCallPart;
	onApprove: (approvalId: string, approved: boolean) => void;
	onRehydrate: (callId: string) => void;
}) {
	const done = part.state === "complete";
	const approvalId = part.approval?.id;
	const awaitingApproval =
		part.state === "approval-requested" &&
		approvalId !== undefined &&
		part.approval?.approved === undefined;

	// A canvas-producing tool's chip rehydrates the focus canvas to THIS call's
	// result on click (pins by call-id). Only once complete — an in-flight call
	// has no result to project. probe / teach / replay map to no canvas member,
	// so their chips are display-only (no click).
	const clickable = done && isCanvasTool(part.name);
	const input = parseArguments(part.arguments);
	const summary = toolChipSummary(part.name, input, part.output);

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
				onClick={clickable ? () => onRehydrate(part.id) : undefined}
				style={clickable ? { cursor: "pointer" } : undefined}
				data-testid={clickable ? `tool-chip-${part.id}` : undefined}
			>
				<Box style={{ minWidth: 0 }}>
					<Text size="sm" fw={600}>
						{part.name}
					</Text>
					<Text
						size="xs"
						c="dimmed"
						truncate
						data-testid={`tool-call-summary-${part.id}`}
					>
						{summary}
					</Text>
				</Box>
				{done ? (
					clickable ? (
						<Text size="xs" c="blue">
							view
						</Text>
					) : null
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
		</Card>
	);
}

export function ChatRail() {
	const { setCanvasState, registerChatSender, pinCanvas, pinnedCallId } =
		useCockpit();
	const { messages, sendMessage, isLoading, error, addToolApprovalResponse } =
		useChat({ connection: fetchServerSentEvents("/api/chat") });
	const [input, setInput] = useState("");

	// Publish `sendMessage` to the cockpit context so canvas widgets (which have
	// no chat handle) can drive a turn — the column→why click-through (DAT-352)
	// dispatches a why_column request this way. The canvas flips to loading so the
	// drilled column's explanation streaming feels responsive.
	useEffect(() => {
		registerChatSender((text) => {
			setCanvasState({ kind: "loading" });
			void sendMessage(text);
		});
		return () => registerChatSender(null);
	}, [registerChatSender, sendMessage, setCanvasState]);

	// Project the latest tool result onto the focus canvas as messages arrive.
	// canvasFromMessages returns a FRESH object every token tick, but the
	// projection usually hasn't changed (same tool result). Dedupe by value so we
	// don't churn the canvas on every token — re-setting an equal result-grid
	// made it re-issue its stream.
	//
	// Pinned mode (DAT-354): when the user has clicked an earlier result chip
	// (`pinnedCallId` set) the canvas is showing HISTORY, so we suppress the
	// always-project-latest behavior — a freshly-streamed result must NOT clobber
	// the pinned view. CRITICAL: while pinned we also reset the dedupe ref to
	// null. On return-to-live (`pinnedCallId` → null) the effect re-fires (the
	// pin is in the deps) and re-projects the latest UNCONDITIONALLY — even when
	// the newest result equals the pre-pin projected value, which a stale ref
	// would otherwise suppress, leaving return-to-live showing nothing. The pin
	// is a primitive string, so adding it to the deps keeps them stable.
	const lastProjectedRef = useRef<string | null>(null);
	useEffect(() => {
		if (pinnedCallId) {
			lastProjectedRef.current = null;
			return;
		}
		const next = canvasFromMessages(messages);
		if (!next) return;
		const key = JSON.stringify(next);
		if (key === lastProjectedRef.current) return;
		lastProjectedRef.current = key;
		setCanvasState(next);
	}, [messages, setCanvasState, pinnedCallId]);

	// A result-chip click pins the canvas to that call's result. We resolve the
	// canvas from the call id (reusing the same toolResultToCanvas mapper) and
	// pin in one dispatch. A call that maps to no canvas member (display-only
	// tool, or a not-yet-complete call) yields null → no-op.
	const onRehydrate = (callId: string) => {
		const canvas = canvasFromCallId(messages, callId);
		if (!canvas) return;
		pinCanvas(callId, canvas);
	};

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

	// Upload entry-mode (DAT-386; multi-file DAT-391): staged `s3://` path(s) drive
	// the EXISTING connect tool through the agent loop — one connect per file for a
	// schema preview — and, for a batch, a single select registering them as ONE
	// `file_uris` source. The tool results project onto the canvas via the same
	// canvasFromMessages bridge — no new sniff path, no canvas wiring here.
	const onUploaded = (s3Paths: string[]) => {
		if (isLoading || s3Paths.length === 0) return;
		setCanvasState({ kind: "loading" });
		if (s3Paths.length === 1) {
			void sendMessage(
				`Connect to the uploaded file at ${s3Paths[0]} (source_kind=file) and show me its schema.`,
			);
			return;
		}
		const list = s3Paths.map((p) => `- ${p}`).join("\n");
		void sendMessage(
			`I uploaded ${s3Paths.length} files to import together as ONE source:\n${list}\n\n` +
				`Connect to each file (source_kind=file) so I can preview its schema, then ` +
				`register them as a single source with the select tool — pass all ${s3Paths.length} ` +
				`as file_uris.`,
		);
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
										onRehydrate={onRehydrate}
									/>
								);
							}
							return null;
						}),
					)}
				</Stack>
			</Box>
			<UploadDropzone onUploaded={onUploaded} disabled={isLoading} />
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
