// Unit test for the drill Slice menu's honest-provenance tier decision
// (DAT-673). The JSX badge itself follows band-badge.tsx's precedent (no
// dedicated render test — that module has none either); what's worth pinning
// is the JUDGEMENT of which tier an axis falls into, since that's the
// "no invented tiers" contract the guidance chip depends on.

import { describe, expect, it } from "vitest";

import {
	type AxisGuidanceInput,
	axisGuidanceTier,
	formatSignificant,
} from "./axis-guidance";

const input = (over: Partial<AxisGuidanceInput> = {}): AxisGuidanceInput => ({
	driverGain: null,
	sliceRelevance: null,
	sliceInterest: null,
	...over,
});

describe("axisGuidanceTier", () => {
	it("is 'driver' when a measured driver_rankings gain names this column", () => {
		expect(axisGuidanceTier(input({ driverGain: 0.1 }))).toBe("driver");
	});

	it("is 'primary' when the cataloguing agent judged it primary", () => {
		expect(axisGuidanceTier(input({ sliceInterest: "primary" }))).toBe(
			"primary",
		);
	});

	it("is 'supporting' when the cataloguing agent judged it supporting", () => {
		expect(axisGuidanceTier(input({ sliceInterest: "supporting" }))).toBe(
			"supporting",
		);
	});

	it("is 'unjudged' when catalogued and scored but never judged into a tier", () => {
		expect(axisGuidanceTier(input({ sliceRelevance: 0.42 }))).toBe("unjudged");
	});

	it("is null for a pure substrate column — no invented tier for 'nothing catalogued this'", () => {
		expect(axisGuidanceTier(input())).toBeNull();
	});

	it("a driver gain outranks a curated interest tier, matching orderAxesByDrivers' precedence", () => {
		expect(
			axisGuidanceTier(
				input({
					driverGain: 0.05,
					sliceInterest: "primary",
					sliceRelevance: 0.9,
				}),
			),
		).toBe("driver");
	});

	it("a judged interest tier outranks a bare relevance score", () => {
		expect(
			axisGuidanceTier(
				input({ sliceInterest: "supporting", sliceRelevance: 0.9 }),
			),
		).toBe("supporting");
	});
});

// Review-round fix: toFixed(2) collapsed small measured gains into a
// self-contradicting "0.00" and merged distinct gains at 2dp — the ticket's
// own recorded example (bank_transactions.amount → reconciled, gain 0.0016)
// would have rendered as "0.00". 3 significant digits fixes both.
describe("formatSignificant", () => {
	it("keeps 3 significant digits for a small real gain instead of collapsing it to zero", () => {
		expect(formatSignificant(0.0016)).toBe("0.00160");
	});

	it("matches the ticket's other recorded live gains", () => {
		expect(formatSignificant(0.104)).toBe("0.104");
		expect(formatSignificant(0.101)).toBe("0.101");
	});

	it("never renders a nonzero value as a lying '0.00' — omits the number instead", () => {
		expect(formatSignificant(0.0000001)).not.toBe("0.00");
		expect(formatSignificant(0.0000001)).not.toBeNull();
	});

	it("returns null for exactly zero — nothing honest to show", () => {
		expect(formatSignificant(0)).toBeNull();
	});

	it("uses fixed notation, never exponential, at any realistic magnitude", () => {
		for (const n of [0.1, 0.31, 0.999, 1.5, 12.345]) {
			expect(formatSignificant(n)).not.toMatch(/e/i);
		}
	});
});
