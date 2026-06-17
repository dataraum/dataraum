// Pure decision for one grounding-loop round (DAT-551 P3c).
//
// Pure + zero-import so it is (a) sandbox-safe to import into the journey workflow
// and (b) unit-testable WITHOUT a Temporal test server — the loop's control flow is
// otherwise replay-fixture/smoke covered, like the breaker reducer. Given the
// agent's verdict for a round and how many replay attempts remain, decide whether to
// replay (re-measure after teaches), finish clean, or surface for a human.

/** What the journey does after one assessAndGround round. */
export type GroundingStep =
	| { action: "replay" }
	| {
			action: "surface";
			reason: "judgement" | "exhausted";
			note: string | null;
	  }
	| { action: "done" };

export interface GroundingVerdict {
	/** Mechanical grounding teaches applied this round. */
	appliedCount: number;
	/** A non-mechanical gap remains that a human must address. */
	needsJudgement: boolean;
	/** What to tell the human (when surfacing). */
	judgementNote: string | null;
}

/**
 * Decide the next step. `attemptsRemaining` is the number of replays still allowed.
 * - Applied teaches AND attempts left → replay (re-run add_source to re-measure).
 * - Applied teaches BUT out of attempts → surface (couldn't converge in budget).
 * - No teaches applied → nothing mechanical left: surface if a judgement gap
 *   remains, else done (clean).
 */
export function decideGroundingStep(
	verdict: GroundingVerdict,
	attemptsRemaining: number,
): GroundingStep {
	if (verdict.appliedCount > 0) {
		if (attemptsRemaining > 0) return { action: "replay" };
		return {
			action: "surface",
			reason: "exhausted",
			note: verdict.judgementNote,
		};
	}
	if (verdict.needsJudgement) {
		return {
			action: "surface",
			reason: "judgement",
			note: verdict.judgementNote,
		};
	}
	return { action: "done" };
}
