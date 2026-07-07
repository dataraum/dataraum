import { describe, expect, it } from "vitest";

import {
	canonicalDigest,
	diffVerdicts,
	type VerdictSnapshot,
} from "#/lib/measure/verdict-diff";

function snapshot(overrides: Partial<VerdictSnapshot> = {}): VerdictSnapshot {
	return {
		intents: [
			{ digest: "aaa", status: "confirmed" },
			{ digest: "bbb", status: "declined" },
		],
		metrics: [
			{ key: "dso", state: "executed", stateReason: null, lineageDigest: "d1" },
			{
				key: "current_ratio",
				state: "grounded",
				stateReason: "composed but not executed: bind failure",
				lineageDigest: "d2",
			},
		],
		...overrides,
	};
}

describe("canonicalDigest", () => {
	it("is stable under object key order", () => {
		expect(canonicalDigest({ a: 1, b: [2, { c: 3 }] })).toBe(
			canonicalDigest({ b: [2, { c: 3 }], a: 1 }),
		);
	});

	it("distinguishes structurally different payloads", () => {
		expect(canonicalDigest({ a: 1 })).not.toBe(canonicalDigest({ a: 2 }));
	});
});

describe("diffVerdicts", () => {
	it("reports no flips for identical snapshots", () => {
		expect(diffVerdicts(snapshot(), snapshot())).toEqual([]);
	});

	it("flags an intent status flip (confirmed → declined)", () => {
		const after = snapshot({
			intents: [
				{ digest: "aaa", status: "declined" },
				{ digest: "bbb", status: "declined" },
			],
		});
		expect(diffVerdicts(snapshot(), after)).toEqual([
			{
				kind: "intent-status",
				key: "aaa",
				before: "confirmed",
				after: "declined",
			},
		]);
	});

	it("flags intent membership variance in both directions", () => {
		const after = snapshot({
			intents: [
				{ digest: "aaa", status: "confirmed" },
				{ digest: "ccc", status: "confirmed" },
			],
		});
		expect(diffVerdicts(snapshot(), after)).toEqual([
			{
				kind: "intent-membership",
				key: "bbb",
				before: "declined",
				after: null,
			},
			{
				kind: "intent-membership",
				key: "ccc",
				before: null,
				after: "confirmed",
			},
		]);
	});

	it("flags a metric state flip without double-counting its reason", () => {
		const after = snapshot({
			metrics: [
				{
					key: "dso",
					state: "grounded",
					stateReason: "composed but not executed: SQL error",
					lineageDigest: "d1",
				},
				{
					key: "current_ratio",
					state: "grounded",
					stateReason: "composed but not executed: bind failure",
					lineageDigest: "d2",
				},
			],
		});
		expect(diffVerdicts(snapshot(), after)).toEqual([
			{
				kind: "metric-state",
				key: "dso",
				before: "executed",
				after: "grounded",
			},
		]);
	});

	it("flags a reason change on an unchanged state (the low-confidence caveat)", () => {
		const after = snapshot({
			metrics: [
				{
					key: "dso",
					state: "executed",
					stateReason: "low grounding confidence: 0.4",
					lineageDigest: "d1",
				},
				{
					key: "current_ratio",
					state: "grounded",
					stateReason: "composed but not executed: bind failure",
					lineageDigest: "d2",
				},
			],
		});
		expect(diffVerdicts(snapshot(), after)).toEqual([
			{
				kind: "metric-reason",
				key: "dso",
				before: null,
				after: "low grounding confidence: 0.4",
			},
		]);
	});

	it("flags lineage changes independently of state, and metric membership", () => {
		const after = snapshot({
			metrics: [
				{
					key: "dso",
					state: "executed",
					stateReason: null,
					lineageDigest: "d9",
				},
				// current_ratio vanished from the declared set
			],
		});
		expect(diffVerdicts(snapshot(), after)).toEqual([
			{
				kind: "metric-membership",
				key: "current_ratio",
				before: "grounded",
				after: null,
			},
			{ kind: "metric-lineage", key: "dso", before: "d1", after: "d9" },
		]);
	});
});
