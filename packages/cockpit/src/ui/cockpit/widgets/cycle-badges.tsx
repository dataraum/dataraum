// Cycle-specific completion badge (DAT-465) — the structural completion a
// business cycle surfaces once it executes (the cycle analog of validation's
// pass/fail verdict). One module per rule 13 — both cycle surfaces (cycle-list /
// cycle-why) render it. The lifecycle-state badge it sits next to is
// substrate-generic and lives in lifecycle-badges.tsx (LifecycleStateBadge).
//
// `completion_rate` is the engine's persisted structural measurement (completed
// cycles / total records, 0–1) — never recomputed here. The colour is a read of
// that rate, not a re-derivation: a healthy cycle (≥80%) reads green, a stalling
// one amber, a broken one red (the cycles.yaml health_factors thresholds).

import { Badge, Text } from "@mantine/core";

// Completion-rate → Mantine color. Thresholds mirror the cycles.yaml
// `health_factors` guidance: >80% healthy, <50% a warning sign.
function completionColor(rate: number): string {
	if (rate >= 0.8) return "green";
	if (rate >= 0.5) return "yellow";
	return "red";
}

/** The completion badge: `NN%` coloured by health, muted dash while there is no
 * measurement (not executed yet — the lifecycle-state badge says why not). */
export function CycleCompletionBadge({
	rate,
}: {
	rate: number | null | undefined;
}) {
	if (rate === null || rate === undefined) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Badge color={completionColor(rate)} variant="light" size="sm" tt="none">
			{Math.round(rate * 100)}%
		</Badge>
	);
}
