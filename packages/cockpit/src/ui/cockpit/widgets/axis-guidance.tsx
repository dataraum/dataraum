// Axis guidance badge (DAT-673) — the drill Slice menu's honest-provenance
// chip: which axes lead the menu because something MEASURED them, versus
// which are offered with no signal at all. Shared visual vocabulary, same
// precedent as `band-badge.tsx` (one Mantine Badge, a closed color map keyed
// by a semantic vocabulary string) rather than inlined per-widget.
//
// The tier vocabulary is closed and DERIVED, never invented: "driver" means a
// measured `driver_rankings` gain named this column; "primary"/"supporting"
// are the cataloguing agent's own judgement (DAT-879 slice_interest);
// "unjudged" means the column IS catalogued and scored (slice_relevance) but
// was never judged into a tier. A column with NONE of these — pure substrate,
// nothing ever catalogued it — gets no badge at all: that absence is real,
// not a fifth tier standing in for "we don't know."

import { Badge } from "@mantine/core";

export type AxisGuidanceTier = "driver" | "primary" | "supporting" | "unjudged";

/** The subset of `DrillAxis` the tier decision needs (kept narrow so callers
 *  — including tests — don't have to construct a full axis). */
export interface AxisGuidanceInput {
	driverGain: number | null;
	sliceRelevance: number | null;
	sliceInterest: string | null;
}

/**
 * Which honest provenance tier (if any) an axis's guidance chip shows (pure).
 *
 * Priority, matching the SAME precedence `orderAxesByDrivers` already applies
 * to the menu's ordering (a measured driver gain outranks curated intuition):
 * a driver gain wins even when the axis ALSO carries a curated interest tier
 * — the axis is a "driver" chip, not a "primary" one, in that case.
 */
export function axisGuidanceTier(
	axis: AxisGuidanceInput,
): AxisGuidanceTier | null {
	if (axis.driverGain !== null) return "driver";
	if (axis.sliceInterest === "primary") return "primary";
	if (axis.sliceInterest === "supporting") return "supporting";
	if (axis.sliceRelevance !== null) return "unjudged";
	return null;
}

const TIER_COLOR: Record<AxisGuidanceTier, string> = {
	driver: "grape",
	primary: "green",
	supporting: "teal",
	unjudged: "gray",
};

const TIER_LABEL: Record<AxisGuidanceTier, string> = {
	driver: "Driver",
	primary: "Primary",
	supporting: "Supporting",
	unjudged: "Unjudged",
};

/**
 * 3 significant digits, FIXED notation (this feeds a small UI chip, never
 * exponential) — NOT `formatDrivers`'s 2-decimal `g()` (query-context.ts),
 * which was the wrong precedent to match: that convention feeds an LLM
 * PROMPT context block, where a real driver gain can be smaller than 2dp
 * resolves (the ticket's own recorded example, `bank_transactions.amount` →
 * `reconciled`, gain 0.0016) — 2dp collapses it into a self-contradicting
 * "Driver · 0.00" AND merges genuinely distinct small gains onto the same
 * displayed value. Returns `null` (omit the number) when the value still
 * rounds to zero at 3 significant digits — there is nothing honest left to
 * show at that point, so the chip drops the number rather than lie with
 * "0.00".
 */
export function formatSignificant(n: number): string | null {
	if (n === 0) return null;
	const abs = Math.abs(n);
	// 3 significant digits: a number of magnitude 10^k needs (2 - k) decimal
	// places for its leading digit to be the 3rd significant one (e.g.
	// 0.1 → k=-1 → 3dp "0.100"; 0.0016 → k=-3 → 5dp "0.00160").
	const decimals = Math.max(0, Math.min(2 - Math.floor(Math.log10(abs)), 10));
	const formatted = n.toFixed(decimals);
	return Number(formatted) === 0 ? null : formatted;
}

/** The Slice menu's provenance chip for one axis — `null` (renders nothing)
 *  when the axis carries no catalog or driver signal at all. */
export function AxisGuidanceBadge({ axis }: { axis: AxisGuidanceInput }) {
	const tier = axisGuidanceTier(axis);
	if (tier === null) return null;
	// Only "driver" carries its number on the chip — a measured gain is worth
	// disclosing precisely. "Unjudged" drops its raw relevance float (nit):
	// the tier label alone reads better on a small chip, and the number added
	// no honest disclosure `axisGuidanceTier` didn't already carry.
	const gain =
		tier === "driver" && axis.driverGain !== null
			? formatSignificant(axis.driverGain)
			: null;
	const label =
		gain !== null ? `${TIER_LABEL[tier]} · ${gain}` : TIER_LABEL[tier];
	return (
		<Badge color={TIER_COLOR[tier]} variant="light" size="sm" tt="none">
			{label}
		</Badge>
	);
}
