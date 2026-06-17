// Grounding-readiness oracle (DAT-551 P3c) — the agent's self-check.
//
// Reads the head-joined `current_entropy_readiness` view (latest snapshot per
// target) for a run's typed tables: per (column/table) target, its readiness
// `band`, `worst_intent_risk`, and the ranked `top_drivers`. The grounding-teach
// agent reads this to (a) decide whether a measurable gap remains and (b) verify a
// teach helped — re-reading after a replay tells it if the band improved. The
// `target` string ("column:<table>.<col>") carries the human names the agent needs
// to formulate a `unit` teach; `top_drivers` (e.g. type_fidelity / null_semantics /
// unit_entropy) tell it WHICH mechanical grounding teach a gap calls for.

import { inArray } from "drizzle-orm";
import { metadataDb } from "./client";
import { currentEntropyReadiness } from "./schema";

/** One target's latest readiness — the agent's per-column gap signal. */
export interface GroundingReadinessRow {
	/** The readiness target, e.g. "column:payments.amount" or "table:payments" —
	 * carries the table + column NAMES the agent teaches a `unit` by. */
	target: string;
	tableId: string | null;
	columnId: string | null;
	/** "ready" | "investigate" | "blocked" (defaults to "unknown" if the view row
	 * carried no band). */
	band: string;
	/** Worst per-intent risk in [0,1]; higher = more disagreement/ignorance. */
	worstIntentRisk: number;
	/** Ranked drivers [{node, state, impact_delta}] — the detectors driving the
	 * risk; the agent maps these to the grounding teach that addresses them. */
	topDrivers: unknown;
}

/**
 * The latest readiness for the run's typed tables (DAT-551). Empty when nothing is
 * measured yet. The agent compares this across replays to know if its teaches moved
 * the band; the workflow loop uses "all ready" as the clean-exit signal.
 */
export async function readGroundingReadiness(
	tableIds: string[],
): Promise<GroundingReadinessRow[]> {
	if (tableIds.length === 0) return [];
	const rows = await metadataDb
		.select({
			target: currentEntropyReadiness.target,
			tableId: currentEntropyReadiness.tableId,
			columnId: currentEntropyReadiness.columnId,
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			topDrivers: currentEntropyReadiness.topDrivers,
		})
		.from(currentEntropyReadiness)
		.where(inArray(currentEntropyReadiness.tableId, tableIds));
	return rows.map((r) => ({
		target: r.target ?? "",
		tableId: r.tableId,
		columnId: r.columnId,
		band: r.band ?? "unknown",
		worstIntentRisk: r.worstIntentRisk ?? 0,
		topDrivers: r.topDrivers,
	}));
}
