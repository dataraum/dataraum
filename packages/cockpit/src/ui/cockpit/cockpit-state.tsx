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
} from "react";
import type { Stage } from "#/journey/stages";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

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
}

const CockpitContext = createContext<CockpitContextValue | null>(null);

export function CockpitProvider({ children }: { children: ReactNode }) {
	const [state, dispatch] = useReducer(cockpitReducer, INITIAL_STATE);

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

	const value = useMemo<CockpitContextValue>(
		() => ({ ...state, setActiveStage, setCanvasState }),
		[state, setActiveStage, setCanvasState],
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
