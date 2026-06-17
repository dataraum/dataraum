// Unit tests for the pure grounding-loop decision (DAT-551 P3c). The loop's wiring
// is replay-fixture/smoke covered; this pins the branch logic.

import { describe, expect, it } from "vitest";
import { decideGroundingStep } from "./grounding-step";

describe("decideGroundingStep (DAT-551)", () => {
	it("replays when teaches were applied and attempts remain", () => {
		expect(
			decideGroundingStep(
				{ appliedCount: 2, needsJudgement: false, judgementNote: null },
				2,
			),
		).toEqual({ action: "replay" });
	});

	it("surfaces 'exhausted' when teaches were applied but no attempts remain", () => {
		expect(
			decideGroundingStep(
				{ appliedCount: 1, needsJudgement: false, judgementNote: "x" },
				0,
			),
		).toEqual({ action: "surface", reason: "exhausted", note: "x" });
	});

	it("is done when nothing was applied and no judgement is needed (clean)", () => {
		expect(
			decideGroundingStep(
				{ appliedCount: 0, needsJudgement: false, judgementNote: null },
				3,
			),
		).toEqual({ action: "done" });
	});

	it("surfaces 'judgement' when nothing mechanical was applied but a human gap remains", () => {
		expect(
			decideGroundingStep(
				{
					appliedCount: 0,
					needsJudgement: true,
					judgementNote: "payments.method needs a concept",
				},
				3,
			),
		).toEqual({
			action: "surface",
			reason: "judgement",
			note: "payments.method needs a concept",
		});
	});

	it("prioritises replay over a judgement note while attempts remain (re-measure first)", () => {
		// Applied teaches + a flagged judgement gap + attempts left → replay; the
		// judgement is re-evaluated next round on fresh readiness.
		expect(
			decideGroundingStep(
				{ appliedCount: 1, needsJudgement: true, judgementNote: "later" },
				1,
			),
		).toEqual({ action: "replay" });
	});
});
