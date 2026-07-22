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

import { and, eq, inArray } from "drizzle-orm";
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
	/** The rollup's coverage (DAT-853): 'measured' | 'partial' | 'unmeasured'.
	 * 'unmeasured' means the loss-path detectors ALL abstained (gap reasons), so
	 * `band`='ready' is VACUOUS — the agent must treat it as a gap, not clean.
	 * Defaults to "measured" only if the view row carried no coverage (the column
	 * is NOT NULL underneath — this coalesce is a view-type artifact). */
	coverage: string;
	/** The self-describing abstention trace (DAT-853): [{detector, reason, intents}]
	 * — WHY a loss-path detector could not measure. The agent reads this to see if
	 * an unmeasured/partial target is a mechanical gap it can still ground. */
	abstentions: unknown;
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
			coverage: currentEntropyReadiness.coverage,
			abstentions: currentEntropyReadiness.abstentions,
			topDrivers: currentEntropyReadiness.topDrivers,
		})
		.from(currentEntropyReadiness)
		// Pin the add_source GRAIN (via_table_head, DAT-597). `current_entropy_readiness`
		// is multi-grain: after a begin_session/operating_model run a column carries a
		// SECOND row sealed by the catalog head (the session re-adjudication). The
		// grounding loop grounds the add_source layer, so it must assess the add_source
		// run's own verdict — NOT a stale catalog row that a replay-after-session would
		// otherwise mix in (two rows per column → duplicate/conflicting gaps). On a first
		// import no catalog head exists yet, so this is a no-op there. (The inspect tools
		// keep the OPPOSITE pick — pickCurrentRow, catalog supersedes — by design.)
		.where(
			and(
				inArray(currentEntropyReadiness.tableId, tableIds),
				eq(currentEntropyReadiness.viaTableHead, true),
			),
		);
	return rows.map((r) => ({
		target: r.target ?? "",
		tableId: r.tableId,
		columnId: r.columnId,
		band: r.band ?? "unknown",
		worstIntentRisk: r.worstIntentRisk ?? 0,
		// NOT NULL underneath (engine default 'measured'); the view types it nullable.
		// This coalesce is unreachable, but fail CLOSED: an unexpected null biases the
		// gap filter toward "not measured" (one wasted LLM look) rather than exiting
		// the grounding loop green on a target it never measured — the epic's failure.
		coverage: r.coverage ?? "unmeasured",
		abstentions: r.abstentions,
		topDrivers: r.topDrivers,
	}));
}
