// Chat rail (DAT-347 C1; rebuilt on TanStack AI for DAT-353; chat lifted to the
// provider in the chat→canvas derive refactor).
//
// This component is now PURE RENDER: it reads the conversation from useCockpit()
// (the provider owns useChat) and renders it. There is no canvas state to sync —
// the provider DERIVES the canvas from the message stream, so the effect chain
// that used to mirror it (project-latest, error→empty, turn-ended reconcile) is
// gone. As messages arrive:
//   - text parts     → user / assistant bubbles
//   - tool-call parts → a collapsible card (acting tools run directly on the
//                       user's instruction — there is no approval gate)
//   - a completed canvas-tool chip → click pins the canvas to that result
// Streaming is driven ONLY by user submit (never on mount → SSR-safe).

import { Alert, Box, Card, Group, Loader, Stack, Text } from "@mantine/core";
import { useCallback, useEffect, useRef } from "react";
import { classifyChatError } from "#/lib/chat-error";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { Composer } from "#/ui/cockpit/composer";
import { MarkdownMessage } from "#/ui/cockpit/markdown";
import { isNearBottom } from "#/ui/cockpit/scroll-stick";
import {
	lastUserMessageIndex,
	type ToolCallPartLike,
	toolChipStatus,
	toolResultErrorsById,
} from "#/ui/cockpit/tool-chip-state";
import {
	isCanvasTool,
	toolChipSummary,
	toolLabel,
} from "#/ui/cockpit/tool-chip-summary";

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
	resultError,
	conversationMovedOn,
	streamIdle,
	onRehydrate,
}: {
	part: ToolCallPartLike;
	/** The correlated tool-result part's error, when one exists (state "error"). */
	resultError?: string;
	/** A later user message exists — an output-less call can never finish. */
	conversationMovedOn: boolean;
	/** The stream is not loading — an output-less call's drain is over (the
	 * stop-then-idle cell: stop() with no follow-up message). */
	streamIdle: boolean;
	onRehydrate: (callId: string) => void;
}) {
	// DAT-436: "done" is NOT `state === "complete"` — the SDK has no error-
	// terminal state (an errored call parks at "input-complete" + output.error),
	// and a severed stream orphans pending parts. toolChipStatus recognizes all
	// terminal shapes; an errored call renders an explicit error state, never an
	// infinite spinner.
	const status = toolChipStatus(part, {
		resultError,
		conversationMovedOn,
		streamIdle,
	});
	const done = status.kind === "complete";

	// A canvas-producing tool's chip rehydrates the focus canvas to THIS call's
	// result on click (pins by call-id). Only once complete — an in-flight or
	// errored call has no result to project. probe / teach map to no canvas
	// member, so their chips are display-only (no click).
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
						{toolLabel(part.name, done)}
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
				{status.kind === "complete" ? (
					clickable ? (
						<Text size="xs" c="blue">
							view
						</Text>
					) : null
				) : status.kind === "error" ? (
					// Explicit error state — the message rides on title for hover; the
					// agent's narration carries the readable explanation.
					<Text
						size="xs"
						c="red"
						title={status.message}
						data-testid={`tool-error-${part.id}`}
					>
						failed
					</Text>
				) : (
					<Loader size="xs" />
				)}
			</Group>
		</Card>
	);
}

export function ChatRail() {
	const { messages, isLoading, error, pinCanvas } = useCockpit();
	// Turn the raw provider/transport error into a cause + next step (DAT-512).
	// Derived during render — no effect, no mirrored state (React-idiom rule 1).
	const classifiedError = error ? classifyChatError(error.message) : null;

	// A completed canvas-tool chip click pins the canvas to that call's result.
	// The provider re-derives the canvas from the call id (canvasFromCallId), so
	// we only pass the id. onRehydrate is only wired for canvas tools (clickable),
	// so a non-canvas pin can't happen here.
	const onRehydrate = (callId: string) => pinCanvas(callId);

	// Keep the conversation pinned to the latest as it streams — the composer sits
	// at the foot of a height-bounded rail and the stream scrolls INTERNALLY (see
	// the cockpit route's fixed height + the messages box's `minHeight: 0`). Two
	// rules the old unconditional `scrollTop = scrollHeight` on every messages tick
	// got wrong (DAT-527):
	//   1. Stick to the bottom ONLY when the user is already there. `followRef`
	//      tracks it from the scroll handler, so scrolling UP to read history
	//      during a streaming turn is never yanked back down.
	//   2. Re-pin AFTER layout settles. A tall tool-card / widget grows its height
	//      asynchronously, so a one-shot snap read scrollHeight too early and landed
	//      short of the true bottom (worse the longer the chat). A ResizeObserver on
	//      the content re-snaps whenever its height changes while the user follows.
	// External-system effect with cleanup (conventions rule 2).
	const streamRef = useRef<HTMLDivElement>(null);
	const contentRef = useRef<HTMLDivElement>(null);
	const followRef = useRef(true);

	const onScroll = useCallback(() => {
		const el = streamRef.current;
		if (el) followRef.current = isNearBottom(el);
	}, []);

	useEffect(() => {
		const el = streamRef.current;
		const content = contentRef.current;
		if (!el || !content) return;
		const stick = () => {
			if (followRef.current) el.scrollTop = el.scrollHeight;
		};
		// Fires on observe (initial pin) and on every content-height change — a new
		// message, a streaming token, or a widget finishing its async layout.
		const observer = new ResizeObserver(stick);
		observer.observe(content);
		return () => observer.disconnect();
	}, []);

	// A tool-call part can recur across messages (e.g. the persisted assistant
	// tool-call plus its completion in a later teed turn — same part id, two
	// messages), so a naive per-message render would show the chip twice. Render
	// each tool-call id ONCE, at its LAST occurrence (the most-complete state).
	// Maps tool-call id → "msgIdx:partIdx".
	const lastToolCallAt = new Map<string, string>();
	messages.forEach((m, mi) => {
		m.parts.forEach((part, i) => {
			if (part.type === "tool-call") lastToolCallAt.set(part.id, `${mi}:${i}`);
		});
	});

	// Chip terminal-state inputs (DAT-436): errored tool-result parts by call id,
	// and the last user message index — a tool call rendered from an EARLIER
	// message belongs to a turn the conversation moved past, so an output-less
	// one can never finish (see tool-chip-state.ts). `!isLoading` is the third
	// input: isLoading spans the ENTIRE result drain (sendMessage no-ops while
	// loading), so an output-less call with the stream idle is equally dead —
	// the stop-then-idle cell, where stop() severed the drain and no later
	// message exists to move the conversation on.
	const resultErrors = toolResultErrorsById(messages);
	const lastUserIdx = lastUserMessageIndex(messages);
	const streamIdle = !isLoading;

	return (
		<Stack gap="sm" h="100%" data-testid="chat-rail">
			<Box
				ref={streamRef}
				onScroll={onScroll}
				style={{ flex: 1, minHeight: 0, overflowY: "auto" }}
				data-testid="chat-messages"
			>
				<Stack ref={contentRef} gap="xs" p="xs">
					{messages.map((m, mi) =>
						m.parts.map((part, i) => {
							if (part.type === "text") {
								// User text is verbatim (they didn't write markdown); assistant
								// text renders as sanitized markdown so snippets / SQL / lists
								// stop showing as raw `**` and ``` fences.
								if (m.role === "user") {
									// User content is always purely the visible bubble — since the
									// DAT-462 flip, model-only refs ride as forwardedProps and
									// persist as a separate model-only row — they never enter
									// message content, so there's nothing to skip here.
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
								// lastToolCallAt) — collapses any duplicate render into one chip.
								if (lastToolCallAt.get(part.id) !== `${mi}:${i}`) return null;
								return (
									<ToolCallCard
										key={part.id}
										part={part as ToolCallPartLike}
										resultError={resultErrors.get(part.id)}
										conversationMovedOn={mi < lastUserIdx}
										streamIdle={streamIdle}
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
							title={classifiedError?.title ?? "Something went wrong"}
							data-testid="chat-error"
						>
							<Stack gap="xs">
								<Text size="sm">
									{classifiedError?.body ??
										"The assistant couldn't finish that — please try again."}
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
			<Composer variant="rail" />
		</Stack>
	);
}
