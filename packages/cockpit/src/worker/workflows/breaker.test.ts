// Unit tests for the journey circuit-breaker reducer (DAT-530 P3b.2). Pure fold,
// no Temporal — the replay fixture covers determinism, these cover the trip/reset
// logic.

import { describe, expect, it } from "vitest";
import { applyOutcome, BREAKER_THRESHOLD } from "./breaker";

const ARMED = { autoMode: true, consecutiveFailures: 0 };

describe("applyOutcome (DAT-530 breaker)", () => {
	it("clears the tally on a success and keeps auto-mode armed", () => {
		expect(
			applyOutcome({ autoMode: true, consecutiveFailures: 2 }, true),
		).toEqual({ autoMode: true, consecutiveFailures: 0 });
	});

	it("counts failures without tripping below the threshold", () => {
		let s = ARMED;
		s = applyOutcome(s, false);
		expect(s).toEqual({ autoMode: true, consecutiveFailures: 1 });
		s = applyOutcome(s, false);
		expect(s).toEqual({ autoMode: true, consecutiveFailures: 2 });
	});

	it("trips auto-mode off at exactly the threshold", () => {
		let s = ARMED;
		for (let i = 0; i < BREAKER_THRESHOLD; i++) s = applyOutcome(s, false);
		expect(s).toEqual({
			autoMode: false,
			consecutiveFailures: BREAKER_THRESHOLD,
		});
	});

	it("a success after a trip clears the tally but does NOT re-arm (manual reset only)", () => {
		const tripped = { autoMode: false, consecutiveFailures: 3 };
		expect(applyOutcome(tripped, true)).toEqual({
			autoMode: false,
			consecutiveFailures: 0,
		});
	});

	it("keeps counting failures while already tripped (stays off)", () => {
		const tripped = { autoMode: false, consecutiveFailures: 3 };
		expect(applyOutcome(tripped, false)).toEqual({
			autoMode: false,
			consecutiveFailures: 4,
		});
	});

	it("honours a custom threshold", () => {
		let s = ARMED;
		s = applyOutcome(s, false, 1);
		expect(s).toEqual({ autoMode: false, consecutiveFailures: 1 });
	});
});
