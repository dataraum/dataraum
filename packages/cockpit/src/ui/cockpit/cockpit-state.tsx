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
import {
	fetchServerSentEvents,
	type UIMessage,
	useChat,
} from "@tanstack/ai-react";
import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useMemo,
	useState,
} from "react";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import {
	canvasFromCallId,
	canvasFromMessages,
} from "#/ui/cockpit/tool-result-to-canvas";

/** Options for a turn sent from the UI. `label` captions the loading canvas
 * shown until the first result arrives ("Explaining the column…"). */
export interface SendOptions {
	label?: string;
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
	addToolApprovalResponse: (response: {
		id: string;
		approved: boolean;
	}) => Promise<void>;
	/** Pin the canvas to a past tool-call's result (re-derived from messages). */
	pinCanvas: (callId: string) => void;
	/** Clear the pin → the canvas snaps back to the live latest. */
	returnToLive: () => void;
}

const CockpitStateContext = createContext<CockpitState | null>(null);
const CockpitActionsContext = createContext<CockpitActions | null>(null);

/** Return the previous reference when `value` is deep-equal (by JSON), so a
 * derived object that recomputes every render but rarely CHANGES doesn't churn
 * memoized consumers. This is the canvas dedupe — `useMemo` keyed on the
 * serialized value (NOT the value's identity, which changes every render):
 * same key → React returns the cached reference. No ref is written during
 * render (conventions rule 8 / react.dev useRef pitfall — DAT-451). */
function useStableValue<T>(value: T): T {
	const key = JSON.stringify(value);
	// `key` IS the value's identity — depending on `value` (a fresh reference
	// each render) would defeat the dedupe.
	// biome-ignore lint/correctness/useExhaustiveDependencies: key replaces value as the identity dep
	return useMemo(() => value, [key]);
}

export function CockpitProvider({ children }: { children: ReactNode }) {
	// The agentic chat loop + SSE transport. The connection is memoized: a fresh
	// connection object each render would recreate the underlying ChatClient (per
	// the SDK contract), dropping the conversation.
	const connection = useMemo(() => fetchServerSentEvents("/api/chat"), []);
	const {
		messages,
		isLoading,
		error,
		sendMessage,
		stop,
		addToolApprovalResponse,
	} = useChat({ connection });

	const [pinnedCallId, setPinnedCallId] = useState<string | null>(null);
	// The pending loading caption for the next turn.
	const [pendingLabel, setPendingLabel] = useState<string | undefined>(
		undefined,
	);

	const sendTurn = useCallback(
		(content: TurnContent, opts?: SendOptions) => {
			setPendingLabel(opts?.label);
			// `TurnContent` IS the SDK's `string | MultimodalContent` (type-only
			// import), so this call is assignable by construction — an SDK bump
			// that reshapes the param surfaces at the import, not here.
			void sendMessage(content);
		},
		[sendMessage],
	);

	const pinCanvas = useCallback(
		(callId: string) => setPinnedCallId(callId),
		[],
	);
	const returnToLive = useCallback(() => setPinnedCallId(null), []);

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

	// Stable actions — every dep is a stable callback (useChat's are useCallback'd
	// over a memoized client; ours are useCallback([])), so this value is created
	// ONCE. Action-only consumers reading it never re-render from context.
	const actions = useMemo<CockpitActions>(
		() => ({
			sendMessage: sendTurn,
			stop,
			addToolApprovalResponse,
			pinCanvas,
			returnToLive,
		}),
		[sendTurn, stop, addToolApprovalResponse, pinCanvas, returnToLive],
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
