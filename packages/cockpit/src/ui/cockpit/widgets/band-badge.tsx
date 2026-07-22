// Shared readiness vocabulary (DAT-451) — the band badge + intent labels every
// readiness surface renders from (evidence-detail precedent: shared visual
// vocabulary is shared code, not per-widget copies). Previously triplicated
// across workspace-inventory / table-readiness / column-why, with rendering
// drift already visible (raw lowercase band vs humanized title-case).

import { Badge, Group, Text } from "@mantine/core";

import { humanizeBand } from "#/ui/cockpit/widgets/inventory-grouping";

// The three entropy intents, as the ENGINE NODE KEYS persisted in readiness
// rows (`network.get_intent_nodes()`), not bare words — matching on the wrong
// string silently renders every per-intent cell as a dash.
export const INTENTS = [
	"query_intent",
	"aggregation_intent",
	"reporting_intent",
] as const;

/** Friendly labels for the intent node keys. String-keyed so callers can fall
 * back (`INTENT_LABEL[intent] ?? intent`) for an unexpected key. */
export const INTENT_LABEL: Record<string, string> = {
	query_intent: "Query",
	aggregation_intent: "Aggregation",
	reporting_intent: "Reporting",
};

// Band → Mantine color. An absent band (not analyzed) renders as a muted dash,
// not a color, so "unknown" never reads as "ready".
const BAND_COLOR: Record<string, string> = {
	ready: "green",
	investigate: "yellow",
	blocked: "red",
};

/** The readiness-band badge: title-cased label, band color, muted dash for an
 * absent band. ONE rendering everywhere — band vocabulary must not drift.
 *
 * `coverage` (DAT-853) is the rollup outcome. An 'unmeasured' band is VACUOUS —
 * the engine keeps the band vocabulary frozen, so a never-measured target reads
 * band='ready'; the badge renders "Not measured" (muted), NEVER a green ready
 * badge. A 'partial' rollup renders the band WITH a "partial" qualifier so the
 * practitioner sees the band rests on incomplete measurement. */
export function BandBadge({
	band,
	coverage,
}: {
	band: string | null | undefined;
	coverage?: string | null;
}) {
	if (coverage === "unmeasured") {
		return (
			<Badge color="gray" variant="light" size="sm" tt="none">
				Not measured
			</Badge>
		);
	}
	if (!band) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	const badge = (
		<Badge
			color={BAND_COLOR[band] ?? "gray"}
			variant="light"
			size="sm"
			tt="none"
		>
			{humanizeBand(band)}
		</Badge>
	);
	if (coverage === "partial") {
		return (
			<Group gap={4} align="center" wrap="nowrap">
				{badge}
				<Text span size="xs" c="dimmed">
					partial
				</Text>
			</Group>
		);
	}
	return badge;
}
