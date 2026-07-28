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

/** 2-decimal fixed, matching `formatDrivers`'s `g()` convention
 *  (query-context.ts) — the same gain number rendered the same way wherever
 *  it appears. */
const fixed2 = (n: number): string => n.toFixed(2);

/** The Slice menu's provenance chip for one axis — `null` (renders nothing)
 *  when the axis carries no catalog or driver signal at all. */
export function AxisGuidanceBadge({ axis }: { axis: AxisGuidanceInput }) {
	const tier = axisGuidanceTier(axis);
	if (tier === null) return null;
	const label =
		tier === "driver" && axis.driverGain !== null
			? `${TIER_LABEL[tier]} · ${fixed2(axis.driverGain)}`
			: tier === "unjudged" && axis.sliceRelevance !== null
				? `${TIER_LABEL[tier]} · ${fixed2(axis.sliceRelevance)}`
				: TIER_LABEL[tier];
	return (
		<Badge color={TIER_COLOR[tier]} variant="light" size="sm" tt="none">
			{label}
		</Badge>
	);
}
