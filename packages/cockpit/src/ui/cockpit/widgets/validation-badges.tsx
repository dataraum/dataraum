// Validation-specific verdict badge (DAT-440) — the pass/fail result a validation
// surfaces once it executes. The lifecycle-state badge it used to sit next to is
// substrate-generic and moved to lifecycle-badges.tsx (LifecycleStateBadge,
// DAT-465); what stays here is validation-only.
//
// `state` (lifecycle progress) and `passed` (the executed verdict) are two
// DISTINCT facts: an executed-but-failed validation correctly shows the executed
// state badge AND a Failed verdict. Values are the engine's persisted strings —
// never recomputed here.

import { Badge, Text } from "@mantine/core";

/** The executed verdict: Passed / Failed, muted dash while there is none
 * (not executed yet — the lifecycle-state badge says why not). */
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
