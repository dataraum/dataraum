// Chat-type availability (DAT-533) — the deterministic state→types mapping the
// nav switcher reads to dim unavailable types. No LLM (the entry-router is S4):
// a plain function over workspace state. Pure + side-effect-free so the mapping
// is unit-tested in isolation; the route loader supplies the state (a cockpit_db
// + metadata read) and renders the result.

import type { ConversationKind } from "#/db/cockpit/conversations";

/** Workspace signals that gate which chat types are startable. Coarse for now
 * (just whether imported data exists); extendable (typed-vs-raw via tables.layer,
 * a framed-vertical gate, …) without touching the switcher. */
export interface WorkspaceChatState {
	/** ≥1 imported table exists → there is data to stage / analyse. */
	hasTables: boolean;
}

/** Per-kind availability for the switcher: whether it's startable, and (when not)
 * the tooltip reason shown on the dimmed icon. */
export interface ChatTypeAvailability {
	kind: ConversationKind;
	available: boolean;
	/** Why it's dimmed — the tooltip. `null` when available. */
	reason: string | null;
}

/** The order the switcher renders, mirroring the onboarding journey. */
export const CHAT_KINDS: ReadonlyArray<ConversationKind> = [
	"connect",
	"stage",
	"analyse",
];

const NEEDS_DATA = "Import data in a Connect chat first.";

/**
 * Map workspace state → per-kind availability (DAT-533). `connect` is always
 * startable (you can always bring in data); `stage` and `analyse` need imported
 * data. Deterministic — the unit-tested core of the switcher's dimming.
 */
export function chatTypesFromState(
	state: WorkspaceChatState,
): ReadonlyArray<ChatTypeAvailability> {
	return CHAT_KINDS.map((kind) => {
		if (kind === "connect") return { kind, available: true, reason: null };
		// stage + analyse both gate on imported data (typing happens inside
		// add_source, so "imported" and "typed" coincide — DAT-533 keeps it coarse).
		return state.hasTables
			? { kind, available: true, reason: null }
			: { kind, available: false, reason: NEEDS_DATA };
	});
}
