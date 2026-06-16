// In-chat readiness (DAT-534) — a NON-BLOCKING signal shown inside a chat when
// its kind can't meaningfully act yet: do-X-first (no data imported) or
// wait-for-Y (a run is in progress). Derived from workspace data-state + the
// chat's own in-flight runs. Rendered as a greyed banner; it never disables the
// composer (the agent can still explain / guide). Pure so the state→message
// mapping is unit-tested in isolation; the chat route loader supplies the state.

import type { ConversationKind } from "#/db/cockpit/conversations";

export interface ChatReadinessState {
	/** ≥1 imported table exists in the workspace (the data to stage / analyse). */
	hasTables: boolean;
	/** This chat has a Temporal run in progress (conversation-scoped, DAT-528). */
	hasActiveRun: boolean;
}

export interface ChatReadiness {
	/** `blocked` = do X first (greyed, advisory); `waiting` = a run is finishing. */
	tone: "blocked" | "waiting";
	message: string;
}

/**
 * The readiness signal for a chat of `kind`, or `null` when it's ready to act (no
 * banner). `connect` is always ready — you can always bring in data. `stage` and
 * `analyse` need imported data first (do-X-first), and yield to an in-flight run
 * (wait-for-Y). Advisory only: never blocks input.
 */
export function chatReadiness(
	kind: ConversationKind,
	state: ChatReadinessState,
): ChatReadiness | null {
	if (kind === "connect") return null;
	if (!state.hasTables) {
		return {
			tone: "blocked",
			message: "No data yet — import some in a Connect chat first.",
		};
	}
	if (state.hasActiveRun) {
		return {
			tone: "waiting",
			message:
				"A run is in progress — results will appear here when it finishes.",
		};
	}
	return null;
}
