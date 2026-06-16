// Cockpit state + agent chat (DAT-347 view state; DAT-353 chat lifted here).
//
// ONE provider owns BOTH the agent chat and the three-region view state. The
// focus canvas is DERIVED from the message stream during render — not stored in
// state and synced through effects — so the whole "stuck spinner / duplicate
// chip / re-issued stream" bug class can't exist:
//
//   canvas = pinned ?? live ?? (isLoading ? loading : empty)
//
// where `live = canvasFromMessages(messages)` and `pinned` re-resolves a past
// tool-call by id (canvasFromCallId). No canvas piece is stored: the imperative
// `override` axis (showCanvas) existed solely for the REST-triggered add_source
// progress hop, which DAT-436 folded into the chat-derivable select result.
// `useStableValue` returns the previous reference when the derived canvas is
// value-equal, so streaming text doesn't churn the canvas.
//
// Chat lives HERE (not trapped in a leaf) so any canvas widget can drive a turn
// through the real `sendMessage` — no registration bridge.

// MultimodalContent comes from @tanstack/ai-client (the package that defines
// it) — @tanstack/ai-react's root index does not re-export it.
import type { MultimodalContent } from "@tanstack/ai-client";
import { type UIMessage, useChat } from "@tanstack/ai-react";
import { useQueryClient } from "@tanstack/react-query";
import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
// Type-only: erased at build, so the cockpit_db (bun:sql) client never enters
// the client bundle — only the UiState shape rides along.
import type { UiState } from "#/db/cockpit/ui-state";
import {
	asWorkflowProgressEvent,
	progressQueryKey,
} from "#/lib/workflow-progress-event";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { createChatConnection } from "#/ui/cockpit/chat-connection";
import type { ChatTypeNav } from "#/ui/cockpit/chat-switcher";
import {
	canvasFromCallId,
	canvasFromMessages,
} from "#/ui/cockpit/tool-result-to-canvas";

/** Options for a turn sent from the UI. `label` captions the loading canvas
 * shown until the first result arrives ("Explaining the column…"). `refs` carries
 * model-only internals (table/column/session ids, upload uris) to the agent via
 * `forwardedProps` — the server persists them as a model-only row and folds them
 * into the user turn for the model; they NEVER enter the visible bubble. This is
 * the DAT-462 refs flip: it replaces the old in-message marker (lib/agent-refs),
 * so a refs leak is impossible by construction, not by a renderer convention. */
export interface SendOptions {
	label?: string;
	refs?: string;
}

/** The content a turn carries: a plain string, or multimodal content parts — the
 * upload handoff (DAT-423) sends a clean text part + a model-only refs part.
 * EXACTLY `sendMessage`'s param type, built on the public `MultimodalContent`
 * export (see the import note above) — no hand-mirrored shape (DAT-449). */
export type TurnContent = string | MultimodalContent;

// The context is SPLIT in two so that consumers reading only stable callbacks
// don't re-render on every streaming token. React context subscription ignores
// memo() boundaries: a single merged value (which changes every token, because
// `messages` does) would re-render EVERY useCockpit() consumer per token — even
// a widget that just needs `sendMessage`. So reactive state and stable actions
// live in separate contexts; action-only widgets read `useCockpitActions()`.

/** Reactive view + chat state — changes as the conversation streams. */
interface CockpitState {
	// Agent chat (lifted from ChatRail). The SDK owns the agentic tool-loop + SSE
	// transport; we expose just what the UI renders.
	messages: ReadonlyArray<UIMessage>;
	isLoading: boolean;
	error: Error | undefined;
	// The DERIVED focus canvas.
	canvas: CanvasState;
	/** Non-null while the canvas shows a PAST tool result (a clicked chip),
	 * addressed by tool-call id. Drives the "viewing history" banner. */
	pinnedCallId: string | null;
}

/** Stable action handles — referentially constant for the provider's lifetime,
 * so a consumer that reads ONLY these never re-renders from context. */
interface CockpitActions {
	/** Send a turn into the agent loop. Sets the loading caption. Callable from
	 * any widget (no bridge). Accepts a plain string or multimodal content (the
	 * upload handoff sends a clean text part + a model-only refs part — DAT-423). */
	sendMessage: (content: TurnContent, opts?: SendOptions) => void;
	/** Abort the in-flight turn (cancels the SSE stream → the server aborts the
	 * Anthropic call — see /api/chat). */
	stop: () => void;
	/** Pin the canvas to a past tool-call's result (re-derived from messages). */
	pinCanvas: (callId: string) => void;
	/** Clear the pin → the canvas snaps back to the live latest. */
	returnToLive: () => void;
	/** The chat-type drop-up wiring (availability + active kind + open/new),
	 * resolved by the route and rendered in the composer. Absent off-route (the
	 * unit tests, the degraded path) → the composer omits the drop-up. Stable per
	 * conversation (the provider is keyed by id), so it rides the actions context. */
	typeNav?: ChatTypeNav;
}

const CockpitStateContext = createContext<CockpitState | null>(null);
const CockpitActionsContext = createContext<CockpitActions | null>(null);

/** Return the previous reference when `value` is deep-equal (by JSON), so a
 * derived object that recomputes every render but rarely CHANGES doesn't churn
 * memoized consumers. This is the canvas dedupe — `useMemo` keyed on the
 * serialized value (NOT the value's identity, which changes every render).
 * React MAY discard the cache (Suspense initial mount; react.dev caveats) —
 * the cost is one extra re-render of memoized consumers, never correctness
 * (the old useRef version equally reset on unmount/remount). No ref is
 * written during render (conventions rule 8 / react.dev useRef pitfall —
 * DAT-451). */
function useStableValue<T>(value: T): T {
	const key = JSON.stringify(value);
	// `key` IS the value's identity — depending on `value` (a fresh reference
	// each render) would defeat the dedupe.
	// biome-ignore lint/correctness/useExhaustiveDependencies: key replaces value as the identity dep
	return useMemo(() => value, [key]);
}

export function CockpitProvider({
	children,
	conversationId,
	initialMessages,
	initialUiState,
	onPersistPin,
	seedMessage,
	typeNav,
}: {
	children: ReactNode;
	// Server-owned conversation hydration (DAT-462): the persisted thread id +
	// its display transcript + restored UI state, from the route loader. Optional
	// so the provider still mounts (a fresh, unhydrated chat) without a loader —
	// the degraded path and the unit tests.
	conversationId?: string;
	initialMessages?: Array<UIMessage>;
	initialUiState?: UiState | null;
	/** Persist a canvas-pin change (server fn from the route); fire-and-forget. */
	onPersistPin?: (pinnedCallId: string | null) => void;
	/** The landing nav-agent's opening message (DAT-534) — sent ONCE on mount into
	 * a freshly-created, empty chat (the "tell" entry). Carried via router state,
	 * so it's absent on reload (the message is persisted by then). */
	seedMessage?: string;
	/** Chat-type drop-up wiring from the route — see CockpitActions.typeNav. */
	typeNav?: ChatTypeNav;
}) {
	// The subscribe transport (Phase 2A) keys BOTH the long-lived subscribe channel
	// (/api/chat-stream) and the send body off ONE conversation id, which MUST
	// match — so resolve it up front. The loader provides it; the degraded/test
	// path (no loader) gets a stable generated id so send + subscribe still target
	// the same channel.
	const threadId = useMemo(
		() => conversationId ?? crypto.randomUUID(),
		[conversationId],
	);
	// The agentic chat loop + subscribe transport. The connection is memoized per
	// thread: a fresh connection object each render would recreate the underlying
	// ChatClient (per the SDK contract), dropping the conversation + its open
	// subscription. `threadId` pins the persisted conversation so a reload
	// re-attaches; `initialMessages` seeds the restored transcript (the canvas
	// re-derives from it — incl. any in-flight progress widget, which re-polls to
	// done).
	const connection = useMemo(() => createChatConnection(threadId), [threadId]);
	// Per-turn model-only refs (DAT-462 flip) ride on AG-UI `forwardedProps`. The
	// react `useChat` hook drops sendMessage's 2nd (per-call body) arg, so we use
	// the INSTANCE forwardedProps option instead — but make it a STABLE holder we
	// mutate per send: ChatClient spreads its CURRENT contents into each request
	// (`{...forwardedProps}` at send time), so setting `holder.refs` synchronously
	// in sendTurn before sendMessage() attaches them to exactly that turn. Stable
	// ref → useChat never recreates the client over it.
	const refsHolder = useMemo<{ refs?: string }>(() => ({}), []);
	// Live workflow progress (Phase 2A.3): the server watcher pushes a CUSTOM
	// progress chunk per tick; write each into the Query cache so the progress
	// widget's `useQuery` re-renders live (its `refetchInterval` is now a one-shot
	// seed). `onChunk` fires for EVERY stream chunk — a standalone CUSTOM event
	// outside a run still lands here — so it's the reliable seam (not onCustomEvent,
	// which rides the run-scoped processor). This replaces the widget's poll.
	const queryClient = useQueryClient();
	const onProgressChunk = useCallback(
		(chunk: unknown) => {
			const ev = asWorkflowProgressEvent(chunk);
			if (ev) {
				queryClient.setQueryData(
					progressQueryKey(ev.workflow_id, ev.run_id),
					ev.progress,
				);
			}
		},
		[queryClient],
	);
	const { messages, isLoading, error, sendMessage, stop } = useChat({
		connection,
		// Subscribe on mount and stay subscribed between turns, so the server can
		// push a turn (a run-completion narration) into an idle chat (Phase 2A).
		live: true,
		onChunk: onProgressChunk,
		forwardedProps: refsHolder,
		threadId,
		initialMessages,
	});

	const [pinnedCallId, setPinnedCallId] = useState<string | null>(
		initialUiState?.pinnedCallId ?? null,
	);
	// The pending loading caption for the next turn.
	const [pendingLabel, setPendingLabel] = useState<string | undefined>(
		undefined,
	);

	const sendTurn = useCallback(
		(content: TurnContent, opts?: SendOptions) => {
			setPendingLabel(opts?.label);
			// Attach this turn's model-only refs to the stable forwardedProps holder
			// the client spreads at send time (DAT-462). Set or clear synchronously
			// BEFORE sendMessage so they land on exactly this turn and don't leak into
			// the next. They never enter `content`, so the rail can't render them.
			if (opts?.refs != null) refsHolder.refs = opts.refs;
			else delete refsHolder.refs;
			// `TurnContent` IS the SDK's `string | MultimodalContent` (type-only
			// import), so this call is assignable by construction — an SDK bump
			// that reshapes the param surfaces at the import, not here.
			void sendMessage(content);
		},
		[sendMessage, refsHolder],
	);

	// Seed auto-send (DAT-534): the landing nav-agent classified the opening
	// message + created this chat, then navigated here with the text in router
	// state. Send it ONCE, on mount, into the still-empty transcript — that runs
	// it through the normal /api/chat flow (persist + the agent's reply). Effect
	// (convention 2: an external action, like the scroll-pin + NDJSON-fold) because
	// it bridges router state → the chat send. Double-guarded against re-fire/reload:
	// a ref makes it once-only, and `messages.length === 0` means a reload (seed
	// gone from state, transcript already persisted) never re-sends.
	const seededRef = useRef(false);
	useEffect(() => {
		if (seededRef.current || !seedMessage || messages.length > 0) return;
		seededRef.current = true;
		sendTurn(seedMessage);
	}, [seedMessage, messages.length, sendTurn]);

	// Pin/unpin also persists the canvas focus (DAT-462) so a reload returns to
	// the same view. Fire-and-forget through the route's server fn — a failed
	// write must never block the interaction (saveUiState is best-effort too).
	const pinCanvas = useCallback(
		(callId: string) => {
			setPinnedCallId(callId);
			onPersistPin?.(callId);
		},
		[onPersistPin],
	);
	const returnToLive = useCallback(() => {
		setPinnedCallId(null);
		onPersistPin?.(null);
	}, [onPersistPin]);

	// Derive the canvas from the stream. canvasFromMessages / canvasFromCallId
	// are pure; the precedence is the whole state machine:
	//   pinned (history)  >  live (latest result)
	//   >  loading (a turn is running, nothing to show yet)  >  empty.
	const live = canvasFromMessages(messages);
	const pinned =
		pinnedCallId !== null ? canvasFromCallId(messages, pinnedCallId) : null;
	const canvas = useStableValue<CanvasState>(
		pinned ??
			live ??
			(isLoading
				? { kind: "loading", label: pendingLabel }
				: { kind: "empty" }),
	);

	// Reactive state — recreated each streaming tick (messages/canvas change).
	const state = useMemo<CockpitState>(
		() => ({ messages, isLoading, error, canvas, pinnedCallId }),
		[messages, isLoading, error, canvas, pinnedCallId],
	);

	// Mostly-stable actions — sendTurn/stop/pinCanvas/returnToLive are useCallback'd
	// over a memoized client, so they're created once. `typeNav` is the exception:
	// the route rebuilds it when the layout loader revalidates (on navigation), so
	// action-only consumers get ONE extra render per navigation — infrequent, never
	// per streaming token (which is the re-render the context split exists to avoid).
	const actions = useMemo<CockpitActions>(
		() => ({
			sendMessage: sendTurn,
			stop,
			pinCanvas,
			returnToLive,
			typeNav,
		}),
		[sendTurn, stop, pinCanvas, returnToLive, typeNav],
	);

	return (
		<CockpitActionsContext.Provider value={actions}>
			<CockpitStateContext.Provider value={state}>
				{children}
			</CockpitStateContext.Provider>
		</CockpitActionsContext.Provider>
	);
}

/** Reactive view + chat state. A consumer re-renders on every streaming tick —
 * use only when you render that streaming state. */
export function useCockpitState(): CockpitState {
	const value = useContext(CockpitStateContext);
	if (value === null) {
		throw new Error("useCockpitState must be used within a CockpitProvider");
	}
	return value;
}

/** Stable action handles. Reading ONLY this never re-renders from context — the
 * right hook for widgets that just dispatch (sendMessage / pinCanvas / …). */
export function useCockpitActions(): CockpitActions {
	const value = useContext(CockpitActionsContext);
	if (value === null) {
		throw new Error("useCockpitActions must be used within a CockpitProvider");
	}
	return value;
}

/** Convenience: state + actions merged. Subscribes to BOTH contexts, so it
 * re-renders every streaming tick — use only when a component needs both
 * reactive state AND actions (ChatRail, CockpitView). */
export function useCockpit(): CockpitState & CockpitActions {
	return { ...useCockpitState(), ...useCockpitActions() };
}
