// Shared readiness vocabulary (DAT-451) — the band badge + intent labels every
// readiness surface renders from (evidence-detail precedent: shared visual
// vocabulary is shared code, not per-widget copies). Previously triplicated
// across workspace-inventory / table-readiness / column-why, with rendering
// drift already visible (raw lowercase band vs humanized title-case).

import { Badge, Group, Text } from "@mantine/core";

import { humanizeBand } from "#/ui/cockpit/widgets/inventory-grouping";

// The three entropy intents, as the persisted readiness-row KEYS (the loss
// table's `LossConfig.intents()`, packages/engine/src/dataraum/entropy/loss.py
// — the Bayesian network + network.yaml were deleted in DAT-442, so these are
// no longer "network node keys"), not bare words — matching on the wrong
// string silently renders every per-intent cell as a dash.
//
// Named `presentation_intent` (DAT-883 rename, was `reporting_intent`): the
// highest-stakes tier — a number PRESENTED as finished/labeled/authoritative
// to a stakeholder with no further chance to caveat it — renamed off
// "reporting" to stop colliding with the cockpit's own `Report` entity
// (reports/mint, unrelated).
export const INTENTS = [
	"query_intent",
	"aggregation_intent",
	"presentation_intent",
] as const;

/** Friendly labels for the intent node keys. String-keyed so callers can fall
 * back (`INTENT_LABEL[intent] ?? intent`) for an unexpected key. */
export const INTENT_LABEL: Record<string, string> = {
	query_intent: "Query",
	aggregation_intent: "Aggregation",
	presentation_intent: "Presentation",
};

// Band → Mantine color. An absent band (not analyzed) renders as a muted dash,
// not a color, so "unknown" never reads as "ready".
const BAND_COLOR: Record<string, string> = {
	ready: "green",
	investigate: "yellow",
	blocked: "red",
};

// `band` is untrusted persisted text — a report's `confidence.band` is
// validated at MINT (mint.ts's MintBodySchema) but not on every read, and a
// direct DB edit or a future writer could still put anything in the column.
// A plain `BAND_COLOR[band]` lookup resolves an inherited key like
// "constructor" through Object.prototype to a FUNCTION (truthy, so `??
// "gray"` never fires), and Mantine's color parser throws on a non-string
// color deep inside render — taking the whole page down for one bad row
// (the step-check-badge.tsx `severityColor` precedent, same bug class).
// `Object.hasOwn` guards the lookup to the object's OWN keys only.
function bandColor(band: string): string {
	return Object.hasOwn(BAND_COLOR, band) ? BAND_COLOR[band] : "gray";
}

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
		<Badge color={bandColor(band)} variant="light" size="sm" tt="none">
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
