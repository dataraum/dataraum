// Pure: a briefing `nextAction` → the chat message its chip seeds (DAT-634). The
// briefing canvas foregrounds the actions THIS chat can act on; clicking a chip
// sends one of these as a user turn, so the agent opens already working it.

import type { BriefingAction } from "#/db/metadata/briefing/types";

export function nextActionSeed(action: BriefingAction): string {
	switch (action.kind) {
		case "replay":
			return "Apply the pending teaches by running replay over the workspace.";
		case "teach":
			// The label carries the specifics ("3 columns blocked …" / "5
			// operating-model items …"), so fold it into the request.
			return `Help me address this — ${action.label}.`;
		case "begin_session":
			return "Build the model over the imported tables (begin_session).";
		case "operating_model":
			return "Run the operating model over the workspace.";
		case "answer":
			return "What can I analyze in this data?";
		case "review_blocker":
			return `A run needs input: ${action.label}. Help me resolve it.`;
		default: {
			// Exhaustive over BriefingActionKind — adding a kind without a case above
			// is a compile error here (a real guarantee, not just a comment).
			const _exhaustive: never = action.kind;
			return _exhaustive;
		}
	}
}
