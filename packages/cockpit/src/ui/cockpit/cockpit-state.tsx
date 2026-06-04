// Cockpit state + agent chat (DAT-347 view state; DAT-353 chat lifted here).
//
// ONE provider owns BOTH the agent chat and the three-region view state. The
// focus canvas is DERIVED from the message stream during render — not stored in
// state and synced through effects — so the whole "stuck spinner / duplicate
// chip / re-issued stream" bug class can't exist:
//
//   canvas = pinned ?? override ?? live ?? (isLoading ? loading : empty)
//
// where `live = canvasFromMessages(messages)` and `pinned` re-resolves a past
// tool-call by id (canvasFromCallId). The ONLY stored canvas piece is `override`
// — an imperative swap a widget makes from a REST result (add_source progress),
// cleared on the next turn. `useStableValue` returns the previous reference when
// the derived canvas is value-equal, so streaming text doesn't churn the canvas.
//
// Chat lives HERE (not trapped in a leaf) so any canvas widget can drive a turn
// through the real `sendMessage` — no registration bridge.

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
	useRef,
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
	/** Send a turn into the agent loop. Clears any imperative canvas override and
	 * sets the loading caption. Callable from any widget (no bridge). */
	sendMessage: (text: string, opts?: SendOptions) => void;
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
	/** Imperatively show a canvas member NOT derivable from the chat stream (the
	 * add_source progress widget, seeded by a REST trigger). Cleared on the next
	 * `sendMessage`. The one legitimate stored-canvas write. */
	showCanvas: (canvas: CanvasState) => void;
}

const CockpitStateContext = createContext<CockpitState | null>(null);
const CockpitActionsContext = createContext<CockpitActions | null>(null);

/** Return the previous reference when `value` is deep-equal (by JSON), so a
 * derived object that recomputes every render but rarely CHANGES doesn't churn
 * memoized consumers. This is the canvas dedupe — relocated out of an effect and
 * into the render-time derivation, where derived state belongs. */
function useStableValue<T>(value: T): T {
	const ref = useRef<{ key: string; value: T } | null>(null);
	const key = JSON.stringify(value);
	if (ref.current === null || ref.current.key !== key) {
		ref.current = { key, value };
	}
	return ref.current.value;
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
	// The one imperative canvas override + the pending loading caption.
	const [override, setOverride] = useState<CanvasState | null>(null);
	const [pendingLabel, setPendingLabel] = useState<string | undefined>(
		undefined,
	);

	const sendTurn = useCallback(
		(text: string, opts?: SendOptions) => {
			// A new turn supersedes any imperative override and re-captions loading.
			setOverride(null);
			setPendingLabel(opts?.label);
			void sendMessage(text);
		},
		[sendMessage],
	);

	const pinCanvas = useCallback(
		(callId: string) => setPinnedCallId(callId),
		[],
	);
	const returnToLive = useCallback(() => setPinnedCallId(null), []);
	const showCanvas = useCallback(
		(canvas: CanvasState) => setOverride(canvas),
		[],
	);

	// Derive the canvas from the stream + the override axes. canvasFromMessages /
	// canvasFromCallId are pure; the precedence is the whole state machine:
	//   pinned (history)  >  override (imperative)  >  live (latest result)
	//   >  loading (a turn is running, nothing to show yet)  >  empty.
	const live = canvasFromMessages(messages);
	const pinned =
		pinnedCallId !== null ? canvasFromCallId(messages, pinnedCallId) : null;
	const canvas = useStableValue<CanvasState>(
		pinned ??
			override ??
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
			showCanvas,
		}),
		[
			sendTurn,
			stop,
			addToolApprovalResponse,
			pinCanvas,
			returnToLive,
			showCanvas,
		],
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
 * right hook for widgets that just dispatch (sendMessage / showCanvas / …). */
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
