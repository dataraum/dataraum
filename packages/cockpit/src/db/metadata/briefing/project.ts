// Per-chat projection (DAT-632) — pure. State is singular (the full briefing);
// this only reorders emphasis, foregrounding the actions a given chat can act on
// and demoting the rest to a one-line "elsewhere" pointer. So a Connect chat
// foregrounds import/typing and points at "Stage: 3 columns blocked →" rather
// than dumping it inline.

import type { ConversationKind } from "#/db/cockpit/conversations";
import type {
	BriefingBackgroundPointer,
	ProjectedBriefing,
	WorkspaceBriefing,
} from "./types";

const ALL_KINDS: readonly ConversationKind[] = ["connect", "stage", "analyse"];

/**
 * Project the (already-ranked) briefing onto one chat kind. No recompute — it
 * partitions `nextActions` by `targetChat`. `nextActions` is assumed sorted by
 * priority, so the first action per kind is its most urgent and becomes that
 * kind's background pointer.
 */
export function projectBriefing(
	briefing: WorkspaceBriefing,
	kind: ConversationKind,
): ProjectedBriefing {
	const foreground = briefing.nextActions.filter((a) => a.targetChat === kind);
	const background: BriefingBackgroundPointer[] = [];
	for (const other of ALL_KINDS) {
		if (other === kind) continue;
		const top = briefing.nextActions.find((a) => a.targetChat === other);
		if (top !== undefined) background.push({ chat: other, label: top.label });
	}
	return { kind, foreground, background };
}
