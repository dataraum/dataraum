// Shared lifecycle-state badge (DAT-465) — the declared → grounded → executed
// progress badge that EVERY operating_model family renders (validation, cycle,
// metric). The lifecycle state is substrate-generic (it lives on
// current_lifecycle_artifacts, the same view all three families declare into),
// so one badge module per rule 13 — per-widget copies of a shared vocabulary
// diverge (the band-badge lesson). Factored out of validation-badges.tsx, which
// keeps only the validation-specific pass/fail verdict.
//
// The value is the engine's persisted state string — never recomputed here.

import { Badge, Text } from "@mantine/core";

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
		>
			{state.charAt(0).toUpperCase() + state.slice(1)}
		</Badge>
	);
}
