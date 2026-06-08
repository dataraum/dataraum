// Shared validation vocabulary (DAT-440) — the lifecycle-state badge and the
// pass/fail verdict badge both validation surfaces (validation-list /
// validation-why) render from. One module per rule 13 (the band-badge lesson:
// per-widget copies diverge).
//
// Two DISTINCT facts, two badges: `state` is lifecycle progress (declared →
// grounded → executed — how far the engine got), `passed` is the executed
// verdict. An executed-but-failed validation correctly shows both. Values are
// the engine's persisted strings — never recomputed here.

import { Badge, Text } from "@mantine/core";

// Lifecycle state → Mantine color. A non-executed state in a promoted run
// always carries a state_reason (the fail-loud contract), so anything short of
// executed reads as needs-attention amber; executed is neutral progress-done
// blue (NOT green — the verdict badge owns good/bad).
const STATE_COLOR: Record<string, string> = {
	declared: "orange",
	grounded: "yellow",
	executed: "blue",
};

/** The lifecycle-state badge: engine state string, muted dash when absent. */
export function ValidationStateBadge({
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

/** The executed verdict: Passed / Failed, muted dash while there is none
 * (not executed yet — the state badge says why not). */
export function ValidationVerdictBadge({
	passed,
}: {
	passed: boolean | null | undefined;
}) {
	if (passed === null || passed === undefined) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Badge color={passed ? "green" : "red"} variant="light" size="sm" tt="none">
			{passed ? "Passed" : "Failed"}
		</Badge>
	);
}
