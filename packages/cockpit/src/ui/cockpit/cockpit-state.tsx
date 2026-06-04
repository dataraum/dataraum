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
import type { Stage } from "#/journey/stages";
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

interface CockpitContextValue {
	// View state.
	activeStage: Stage;
	setActiveStage: (stage: Stage) => void;

	// Agent chat (lifted from ChatRail). The SDK owns the agentic tool-loop +
	// SSE transport; we expose just what the UI renders / drives.
	messages: ReadonlyArray<UIMessage>;
	isLoading: boolean;
	error: Error | undefined;
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

	// The DERIVED focus canvas + its two override axes.
	canvas: CanvasState;
	/** Non-null while the canvas shows a PAST tool result (a clicked chip),
	 * addressed by tool-call id. Drives the "viewing history" banner. */
	pinnedCallId: string | null;
	/** Pin the canvas to a past tool-call's result (re-derived from messages). */
	pinCanvas: (callId: string) => void;
	/** Clear the pin → the canvas snaps back to the live latest. */
	returnToLive: () => void;
	/** Imperatively show a canvas member NOT derivable from the chat stream (the
	 * add_source progress widget, seeded by a REST trigger). Cleared on the next
	 * `sendMessage`. The one legitimate stored-canvas write. */
	showCanvas: (canvas: CanvasState) => void;
}

const CockpitContext = createContext<CockpitContextValue | null>(null);

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

	const [activeStage, setActiveStageState] = useState<Stage>("add_source");
	const [pinnedCallId, setPinnedCallId] = useState<string | null>(null);
	// The one imperative canvas override + the pending loading caption.
	const [override, setOverride] = useState<CanvasState | null>(null);
	const [pendingLabel, setPendingLabel] = useState<string | undefined>(
		undefined,
	);

	const setActiveStage = useCallback(
		(stage: Stage) => setActiveStageState(stage),
		[],
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

	const value = useMemo<CockpitContextValue>(
		() => ({
			activeStage,
			setActiveStage,
			messages,
			isLoading,
			error,
			sendMessage: sendTurn,
			stop,
			addToolApprovalResponse,
			canvas,
			pinnedCallId,
			pinCanvas,
			returnToLive,
			showCanvas,
		}),
		[
			activeStage,
			setActiveStage,
			messages,
			isLoading,
			error,
			sendTurn,
			stop,
			addToolApprovalResponse,
			canvas,
			pinnedCallId,
			pinCanvas,
			returnToLive,
			showCanvas,
		],
	);

	return (
		<CockpitContext.Provider value={value}>{children}</CockpitContext.Provider>
	);
}

export function useCockpit(): CockpitContextValue {
	const value = useContext(CockpitContext);
	if (value === null) {
		throw new Error("useCockpit must be used within a CockpitProvider");
	}
	return value;
}
