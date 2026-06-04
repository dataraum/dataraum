// Chat rail (DAT-347 C1; rebuilt on TanStack AI for DAT-353; chat lifted to the
// provider in the chat→canvas derive refactor).
//
// This component is now PURE RENDER: it reads the conversation from useCockpit()
// (the provider owns useChat) and renders it. There is no canvas state to sync —
// the provider DERIVES the canvas from the message stream, so the effect chain
// that used to mirror it (project-latest, error→empty, turn-ended reconcile) is
// gone. As messages arrive:
//   - text parts     → user / assistant bubbles
//   - tool-call parts → a collapsible card; the SDK pauses approval-gated tools
//                       (teach / replay) → Approve / Deny → addToolApprovalResponse
//   - a completed canvas-tool chip → click pins the canvas to that result
// Streaming is driven ONLY by user submit (never on mount → SSR-safe).

import {
	Alert,
	Box,
	Button,
	Card,
	Group,
	Loader,
	Stack,
	Text,
} from "@mantine/core";
import { useEffect, useRef } from "react";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { Composer } from "#/ui/cockpit/composer";
import { MarkdownMessage } from "#/ui/cockpit/markdown";
import {
	isCanvasTool,
	toolChipSummary,
	toolLabel,
} from "#/ui/cockpit/tool-chip-summary";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";

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

// Extended-thinking parts (Claude with thinking enabled). Collapsed by default —
// reasoning is supporting context, not the answer. Dropped silently before this;
// surfaced now so it's never lost if thinking is turned on server-side.
function ThinkingBlock({ content }: { content: string }) {
	return (
		<details data-testid="thinking-block">
			<summary style={{ cursor: "pointer" }}>
				<Text span size="xs" c="dimmed">
					Thinking
				</Text>
			</summary>
			<Text
				size="xs"
				c="dimmed"
				style={{ whiteSpace: "pre-wrap", fontStyle: "italic" }}
			>
				{content}
			</Text>
		</details>
	);
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
	// A denied approval is terminal: the tool never runs, so the call never
	// reaches "complete". Without this the card would spin its Loader forever
	// (the buttons vanish once `approved` is defined) — show "denied" instead.
	const denied = part.approval?.approved === false;

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
						{toolLabel(part.name)}
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
				) : denied ? (
					<Text size="xs" c="dimmed" data-testid={`tool-denied-${part.id}`}>
						denied
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
		</Card>
	);
}

export function ChatRail() {
	const {
		messages,
		sendMessage,
		isLoading,
		error,
		addToolApprovalResponse,
		pinCanvas,
	} = useCockpit();

	// A completed canvas-tool chip click pins the canvas to that call's result.
	// The provider re-derives the canvas from the call id (canvasFromCallId), so
	// we only pass the id. onRehydrate is only wired for canvas tools (clickable),
	// so a non-canvas pin can't happen here.
	const onRehydrate = (callId: string) => pinCanvas(callId);

	// Keep the conversation pinned to the latest as it streams: the composer sits
	// at the foot of a height-bounded rail and the stream scrolls INTERNALLY (see
	// the cockpit route's fixed height + the messages box's `minHeight: 0`), so on
	// every message/token tick we snap the scroll to the bottom.
	const streamRef = useRef<HTMLDivElement>(null);
	useEffect(() => {
		const el = streamRef.current;
		if (el && messages.length > 0) el.scrollTop = el.scrollHeight;
	}, [messages]);

	// Upload entry-mode (DAT-386; multi-file DAT-391): staged `s3://` path(s) drive
	// the EXISTING connect tool through the agent loop — one connect per file for a
	// schema preview — and, for a batch, a single select registering them as ONE
	// `file_uris` source. The tool results project onto the canvas via the same
	// derivation in the provider — no new sniff path, no canvas wiring here.
	const onUploaded = (s3Paths: string[]) => {
		if (isLoading || s3Paths.length === 0) return;
		if (s3Paths.length === 1) {
			sendMessage(
				`Connect to the uploaded file at ${s3Paths[0]} (source_kind=file) and show me its schema.`,
				{ label: "Reading the file…" },
			);
			return;
		}
		const list = s3Paths.map((p) => `- ${p}`).join("\n");
		sendMessage(
			`I uploaded ${s3Paths.length} files to import together as ONE source:\n${list}\n\n` +
				`Connect to each file (source_kind=file) so I can preview its schema, then ` +
				`register them as a single source with the select tool — pass all ${s3Paths.length} ` +
				`as file_uris.`,
			{ label: "Reading the files…" },
		);
	};

	// An approval-gated tool-call part is carried in BOTH the approval-request turn
	// and the post-approval turn that completes it — same part id, two messages —
	// so a naive per-message render shows the chip TWICE ("select shows twice after
	// approve"). Render each tool-call id ONCE, at its LAST occurrence (the most-
	// complete state). Maps tool-call id → "msgIdx:partIdx".
	const lastToolCallAt = new Map<string, string>();
	messages.forEach((m, mi) => {
		m.parts.forEach((part, i) => {
			if (part.type === "tool-call") lastToolCallAt.set(part.id, `${mi}:${i}`);
		});
	});

	return (
		<Stack gap="sm" h="100%" data-testid="chat-rail">
			<Box
				ref={streamRef}
				style={{ flex: 1, minHeight: 0, overflowY: "auto" }}
				data-testid="chat-messages"
			>
				<Stack gap="xs" p="xs">
					{messages.map((m, mi) =>
						m.parts.map((part, i) => {
							if (part.type === "text") {
								// User text is verbatim (they didn't write markdown); assistant
								// text renders as sanitized markdown so snippets / SQL / lists
								// stop showing as raw `**` and ``` fences.
								if (m.role === "user") {
									return (
										<Text
											// biome-ignore lint/suspicious/noArrayIndexKey: append-only parts
											key={`${m.id}-${i}`}
											size="sm"
											c="text"
											fw={600}
											style={{ whiteSpace: "pre-wrap" }}
										>
											{part.content}
										</Text>
									);
								}
								return (
									<MarkdownMessage
										// biome-ignore lint/suspicious/noArrayIndexKey: append-only parts
										key={`${m.id}-${i}`}
										content={part.content}
									/>
								);
							}
							if (part.type === "thinking") {
								return (
									// biome-ignore lint/suspicious/noArrayIndexKey: append-only parts
									<ThinkingBlock key={`${m.id}-${i}`} content={part.content} />
								);
							}
							if (part.type === "tool-call") {
								// Skip all but the last occurrence of this tool-call id (see
								// lastToolCallAt) — collapses the approval-request + completion
								// duplicate into one chip.
								if (lastToolCallAt.get(part.id) !== `${mi}:${i}`) return null;
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
					{/* A run/stream error (RUN_ERROR — e.g. the model hit max_tokens, or
					    the SSE was cut) surfaces inline at the foot of the conversation,
					    highlighted. The canvas is unaffected — it derives from results. */}
					{error && (
						<Alert
							color="red"
							variant="light"
							title="Something went wrong"
							data-testid="chat-error"
						>
							<Stack gap="xs">
								<Text size="sm">
									The assistant couldn't finish that — please try again.
								</Text>
								{/* Raw provider/transport error tucked away — never dump JSON
								    (401 x-api-key, request_id, …) at the user; keep it for debugging. */}
								<details>
									<summary style={{ cursor: "pointer" }}>
										<Text span size="xs" c="dimmed">
											Technical details
										</Text>
									</summary>
									<Text size="xs" c="dimmed" style={{ whiteSpace: "pre-wrap" }}>
										{error.message}
									</Text>
								</details>
							</Stack>
						</Alert>
					)}
				</Stack>
			</Box>
			<UploadDropzone onUploaded={onUploaded} disabled={isLoading} />
			<Composer variant="rail" />
		</Stack>
	);
}
