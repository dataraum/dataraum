// Shared lifecycle-state badge (DAT-465) — the declared → grounded → executed
// progress badge that EVERY operating_model family renders (validation, cycle,
// metric). The lifecycle state is substrate-generic (it lives on
// current_lifecycle_artifacts, the same view all three families declare into),
// so one badge module per rule 13 — per-widget copies of a shared vocabulary
// diverge (the band-badge lesson). Factored out of validation-badges.tsx, which
// keeps only the validation-specific pass/fail verdict.
//
// The value is the engine's persisted state string — never recomputed here.

import { Badge, Text, Tooltip } from "@mantine/core";

// Lifecycle state → Mantine color. A non-executed state in a promoted run always
// carries a state_reason (the fail-loud contract), so anything short of executed
// reads as needs-attention amber; executed is neutral progress-done blue — NOT
// green, because a family's verdict/quality badge (validation pass/fail, cycle
// completion) owns good/bad.
const STATE_COLOR: Record<string, string> = {
	declared: "orange",
	grounded: "yellow",
	executed: "blue",
};

/** The lifecycle-state badge: engine state string, muted dash when absent. */
export function LifecycleStateBadge({
	state,
}: {
	state: string | null | undefined;
}) {
	if (!state) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Badge
			color={STATE_COLOR[state] ?? "gray"}
			variant="light"
			size="sm"
			tt="none"
			// The lifecycle state is a short closed-enum word (declared / grounded /
			// executed). Mantine's Badge label clips with an ellipsis by default, so in
			// a starved table column it rendered "Executed" → "Exec…" (DAT-579). Let the
			// label show in full and the badge size to its content — these labels are
			// never long enough to need truncation.
			styles={{ label: { overflow: "visible" } }}
		>
			{state.charAt(0).toUpperCase() + state.slice(1)}
		</Badge>
	);
}

// Grounding-confidence caveat badge (DAT-631) — a metric's quality badge, the
// analog of the validation pass/fail verdict (the state badge owns PROGRESS,
// never good/bad; quality rides a separate badge — see validation-badges.tsx).
//
// A metric whose SQL composes + verifies still reaches `executed`, but the graph
// agent records the WEAKEST per-concept grounding confidence and, when it falls
// below the engine's floor, stamps a caveat onto the EXECUTED artifact's
// `state_reason` (engine metrics_phase `_low_confidence_reason`). That is the
// only writer of a reason on an executed metric, so on an executed artifact a
// present `state_reason` IS the low-confidence flag — no string parsing, an
// executed metric is silent unless flagged. This surfaces the honesty the engine
// already produces so a 0.35-confidence proxy stops reading identically to a
// confidently-grounded metric (the DAT-631 headline). Renders nothing when the
// metric is confident (executed, no reason) or not yet executed (the state badge
// carries the why-not) — a dash would be noise next to the state badge.
export function GroundingConfidenceBadge({
	state,
	stateReason,
}: {
	state: string | null | undefined;
	stateReason: string | null | undefined;
}) {
	if (state !== "executed" || !stateReason) {
		return null;
	}
	return (
		<Tooltip label={stateReason} multiline maw={320} withArrow>
			<Badge
				color="orange"
				variant="light"
				size="sm"
				tt="none"
				data-testid="grounding-confidence-badge"
				styles={{ label: { overflow: "visible" } }}
			>
				Low confidence
			</Badge>
		</Tooltip>
	);
}
