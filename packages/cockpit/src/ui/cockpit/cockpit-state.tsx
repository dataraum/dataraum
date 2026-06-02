// Cockpit view state (DAT-347, C1).
//
// The three-region agentic view shares a tiny reducer: which journey stage is
// active, and what the focus canvas is showing. The chat rail dispatches canvas
// updates (via the tool→canvas mapper); the stage navigator dispatches stage
// changes. Kept deliberately small — widgets read `canvasState`, they don't add
// their own context.

import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useMemo,
	useReducer,
	useRef,
} from "react";
import type { Stage } from "#/journey/stages";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/** Send a turn into the agent chat loop. The chat lives in ChatRail (which owns
 * `useChat`); a canvas widget has no `sendMessage`, so ChatRail registers its
 * sender here once and widgets reach it through `useCockpit().sendChatMessage`
 * (DAT-352, CRITICAL #2 — the column→why click-through is cross-cutting). */
export type SendChatMessage = (text: string) => void;

interface CockpitState {
	activeStage: Stage;
	canvasState: CanvasState;
}

type CockpitAction =
	| { type: "setActiveStage"; stage: Stage }
	| { type: "setCanvasState"; canvasState: CanvasState };

// Default: the only interactive stage in C1, with an empty canvas waiting for
// the first tool result.
const INITIAL_STATE: CockpitState = {
	activeStage: "add_source",
	canvasState: { kind: "empty" },
};

function cockpitReducer(
	state: CockpitState,
	action: CockpitAction,
): CockpitState {
	switch (action.type) {
		case "setActiveStage":
			return { ...state, activeStage: action.stage };
		case "setCanvasState":
			return { ...state, canvasState: action.canvasState };
	}
}

interface CockpitContextValue extends CockpitState {
	setActiveStage: (stage: Stage) => void;
	setCanvasState: (canvasState: CanvasState) => void;
	// Register the chat sender (ChatRail calls this once with its `sendMessage`).
	registerChatSender: (send: SendChatMessage | null) => void;
	// Send a turn into the agent loop from anywhere (a canvas widget). A no-op
	// until ChatRail has registered its sender — never throws.
	sendChatMessage: SendChatMessage;
}

const CockpitContext = createContext<CockpitContextValue | null>(null);

export function CockpitProvider({ children }: { children: ReactNode }) {
	const [state, dispatch] = useReducer(cockpitReducer, INITIAL_STATE);

	// The chat sender lives in a ref, not state: ChatRail registers it in an
	// effect, and a widget reads it lazily on click. Holding it in state would
	// re-render every consumer on registration (and ChatRail's `sendMessage`
	// identity is stable anyway), so a ref keeps the context value stable.
	const chatSenderRef = useRef<SendChatMessage | null>(null);

	// Dispatchers are stable for the provider's lifetime (dispatch identity is
	// constant). This matters: ChatRail keys its canvas effect on setCanvasState,
	// so an identity that changed every dispatch would re-fire the effect, which
	// re-dispatches a fresh canvas object → infinite render loop. useCallback([])
	// pins them; the canvas effect now only re-runs on a real message change.
	const setActiveStage = useCallback(
		(stage: Stage) => dispatch({ type: "setActiveStage", stage }),
		[],
	);
	const setCanvasState = useCallback(
		(canvasState: CanvasState) =>
			dispatch({ type: "setCanvasState", canvasState }),
		[],
	);
	const registerChatSender = useCallback((send: SendChatMessage | null) => {
		chatSenderRef.current = send;
	}, []);
	// Stable identity (reads the ref lazily) so it doesn't re-fire consumers'
	// effects; a click before ChatRail registers is a silent no-op, not a crash.
	const sendChatMessage = useCallback<SendChatMessage>((text) => {
		chatSenderRef.current?.(text);
	}, []);

	const value = useMemo<CockpitContextValue>(
		() => ({
			...state,
			setActiveStage,
			setCanvasState,
			registerChatSender,
			sendChatMessage,
		}),
		[
			state,
			setActiveStage,
			setCanvasState,
			registerChatSender,
			sendChatMessage,
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
