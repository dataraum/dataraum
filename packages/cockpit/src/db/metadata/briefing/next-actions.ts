// The deterministic call-to-action ladder (DAT-632) — pure over `progress` +
// `attention`. No LLM, no IO: this is the rule set that turns the briefing's
// state into a ranked list of "do this next", each routed to the chat that owns
// the relevant tools.

import type { ConversationKind } from "#/db/cockpit/conversations";
import type {
	BriefingAction,
	BriefingAttention,
	BriefingProgress,
} from "./types";

/** Which chat owns a run-stage's follow-up (the stage's tools live there). */
export function stageToChat(stage: string): ConversationKind {
	// add_source → Connect; begin_session + operating_model → Stage.
	return stage === "add_source" ? "connect" : "stage";
}

function plural(n: number, one: string, many: string): string {
	return n === 1 ? one : many;
}

/**
 * The call-to-action ladder, returned sorted by priority (ascending).
 * `awaiting_input` outranks everything — a run is parked ON the user; then
 * pending replays, then unblock-by-teach, then the forward-motion staging
 * actions, then "ready to answer".
 */
export function computeNextActions(
	progress: BriefingProgress,
	attention: BriefingAttention,
): BriefingAction[] {
	const actions: BriefingAction[] = [];

	// P0 — a run is parked waiting for a human teach (DAT-553). One per item so
	// the chat routing stays accurate; the note IS the human-facing reason.
	for (const item of attention.awaitingInput) {
		actions.push({
			kind: "review_blocker",
			label: item.note ?? `A ${item.stage} run needs your input`,
			targetChat: stageToChat(item.stage),
			priority: 0,
		});
	}

	// P1 — teaches written but not yet applied; a replay re-grounds them.
	if (attention.pendingTeaches.needsReplay) {
		const n = attention.pendingTeaches.count;
		actions.push({
			kind: "replay",
			label: `${n} ${plural(n, "teach", "teaches")} pending — replay to apply`,
			targetChat: "stage",
			priority: 1,
		});
	}

	// P2 — blocked columns need a teach to unblock.
	if (attention.columnsBlocked > 0) {
		const n = attention.columnsBlocked;
		actions.push({
			kind: "teach",
			label: `${n} ${plural(n, "column", "columns")} blocked — teach to unblock`,
			targetChat: "stage",
			priority: 2,
		});
	}

	// P2 — operating-model artifacts that couldn't ground; a teach fixes the bind.
	if (attention.stuckArtifacts.total > 0) {
		const n = attention.stuckArtifacts.total;
		actions.push({
			kind: "teach",
			label: `${n} operating-model ${plural(n, "item", "items")} need grounding — teach to fix`,
			targetChat: "stage",
			priority: 2,
		});
	}

	// P3 — forward motion: imported but not staged → begin_session.
	if (
		progress.stage === "empty" &&
		(progress.connect === "ready" || progress.connect === "needs_attention")
	) {
		actions.push({
			kind: "begin_session",
			label: "Tables imported — start a Stage chat to build the model",
			targetChat: "stage",
			priority: 3,
		});
	}

	// P3 — staged but no operating model yet → run it.
	if (
		progress.analyse === "empty" &&
		(progress.stage === "ready" || progress.stage === "needs_attention")
	) {
		actions.push({
			kind: "operating_model",
			label: "Model staged — run the operating model",
			targetChat: "stage",
			priority: 3,
		});
	}

	// P3 — the operating model RAN but the framed vertical declares nothing to build
	// one from (DAT-845). NOT a "run it again" loop: a re-run can't fix a vertical
	// that declares no validations, cycles, or metrics — the declarations must be
	// ADDED (framed) first. An honest dead-end routed to the Stage chat, where framing
	// lives. The `operating_model` nudge above never fires here (analyse is
	// `nothing_declared`, not `empty`), and "Ready to answer" below is gated on
	// `ready`, so this state neither loops nor claims answerable.
	if (progress.analyse === "nothing_declared") {
		actions.push({
			kind: "declare",
			label:
				"No operating model — this vertical declares no validations, cycles, or metrics; add declarations to build one",
			targetChat: "stage",
			priority: 3,
		});
	}

	// P4 — everything's ready and nothing blocks answers.
	if (progress.analyse === "ready" && attention.columnsBlocked === 0) {
		actions.push({
			kind: "answer",
			label: "Ready to answer questions",
			targetChat: "analyse",
			priority: 4,
		});
	}

	// Stable sort by priority (Array.prototype.sort is stable) — preserves the
	// insertion order within a tier (e.g. blocked-columns before stuck-artifacts).
	return actions.sort((a, b) => a.priority - b.priority);
}
