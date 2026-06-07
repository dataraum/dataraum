// Shared readiness vocabulary (DAT-451) — the band badge + intent labels every
// readiness surface renders from (evidence-detail precedent: shared visual
// vocabulary is shared code, not per-widget copies). Previously triplicated
// across workspace-inventory / table-readiness / column-why, with rendering
// drift already visible (raw lowercase band vs humanized title-case).

import { Badge, Text } from "@mantine/core";

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
 * absent band. ONE rendering everywhere — band vocabulary must not drift. */
export function BandBadge({ band }: { band: string | null | undefined }) {
	if (!band) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Badge
			color={BAND_COLOR[band] ?? "gray"}
			variant="light"
			size="sm"
			tt="none"
		>
			{humanizeBand(band)}
		</Badge>
	);
}
