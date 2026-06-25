// Per-turn agent digest (DAT-634) — pure. A compact, PROJECTED summary of the
// briefing for the chat's kind, appended to the agent's per-turn system context so
// it can say "3 columns are blocked" without a tool round-trip (closing the
// asymmetry where the cockpit chat agent — unlike the engine GraphAgent — was
// blind to readiness). Projected so a Connect chat isn't told to ground metrics,
// nor an Analyse chat to fix typing — those surface only as a brief "elsewhere"
// pointer. Bounded (counts + a capped blocker list); full detail stays behind the
// why_* tools.

import type { ConversationKind } from "#/db/cockpit/conversations";
import { projectBriefing } from "./project";
import type { WorkspaceBriefing } from "./types";

/** Top blockers named in the digest — the rest stay behind why_column. */
const DIGEST_BLOCKER_CAP = 5;

export function formatBriefingDigest(
	briefing: WorkspaceBriefing,
	kind: ConversationKind,
): string | null {
	const a = briefing.attention;
	const { foreground, background } = projectBriefing(briefing, kind);

	const facts: string[] = [];
	if (a.columnsBlocked > 0) facts.push(`${a.columnsBlocked} columns blocked`);
	if (a.columnsInvestigate > 0)
		facts.push(`${a.columnsInvestigate} columns to investigate`);
	if (a.stuckArtifacts.total > 0)
		facts.push(
			`${a.stuckArtifacts.total} operating-model items need grounding`,
		);
	if (a.pendingTeaches.needsReplay)
		facts.push(`${a.pendingTeaches.count} teaches pending (replay to apply)`);

	// Nothing notable AND nothing for this chat to do → no digest (the base
	// workspace-context block already names the tables + vertical).
	if (facts.length === 0 && foreground.length === 0) return null;

	const blockers = a.readinessBlockers
		.slice(0, DIGEST_BLOCKER_CAP)
		.map((b) => (b.source ? `${b.source}/${b.label}` : b.label));

	const parts: string[] = [
		`WORKSPACE STATE — ${facts.length > 0 ? `${facts.join(", ")}.` : "nothing blocking."}`,
	];
	if (blockers.length > 0) parts.push(`Blocked: ${blockers.join(", ")}.`);
	if (foreground.length > 0)
		parts.push(
			`Suggested next here: ${foreground.map((f) => f.label).join("; ")}.`,
		);
	if (background.length > 0)
		parts.push(
			`Elsewhere: ${background.map((b) => `${b.chat} — ${b.label}`).join("; ")}.`,
		);
	parts.push("Use look_table / why_column for the detail behind these.");
	return parts.join(" ");
}
