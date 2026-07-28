// Step-check badge (DAT-840) — shared render of a metric graph step's declared
// post-execution checks (the engine's `GraphStep.validations` / the shipped
// YAML's singular `validation:` block, enforced by `graphs/verifier.py`
// against the executed value; a violation flags the metric, it never
// suppresses the number). BOTH DAG-rendering paths narrow their own step
// shape independently (`tools/operating-model-graph.ts`'s `MetricStep`, the
// live model-canvas DAG; `lib/metric-dag.ts`'s `DagStep`, the shipped/override
// shadow DAG) — that duplication is the existing precedent (see
// `lib/metric-dag.ts`'s header) — but the VISUAL vocabulary for a check must
// not diverge between the two surfaces (rule 13), so it lives once here,
// accepting either narrow structurally (no import from either module).
//
// These are DECLARED checks, not an executed verdict — contrast
// `ValidationVerdictBadge`/`LifecycleStateBadge` (validation-badges.tsx /
// lifecycle-badges.tsx), which render a STANDALONE `validations` lifecycle
// artifact's run state + pass/fail. A graph-step check has no run state at
// this substrate; the badge IS the condition, colored by severity, with the
// business-readable message (when present) in a tooltip.

import { Badge, Group, Tooltip } from "@mantine/core";
import { ShieldCheck } from "lucide-react";

/** The minimal shape either narrow's step-check array satisfies — structural,
 *  so this module needs no import from `tools/` or `lib/`. */
export interface StepCheckView {
	condition: string;
	severity: string | null;
	message: string | null;
}

// The engine's severity enum (`info | warning | error | critical`, default
// "error" — `metric-spec.ts`'s `GraphStepSchema.validation` describes the
// same vocabulary). Unknown/absent severity falls back to "error"'s color,
// not a neutral one — a declared check is a promise the value will be
// examined, so the default reads as attention-worthy.
const SEVERITY_COLOR: Record<string, string> = {
	info: "gray",
	warning: "yellow",
	error: "orange",
	critical: "red",
};

function severityColor(severity: string | null): string {
	return SEVERITY_COLOR[severity ?? "error"] ?? SEVERITY_COLOR.error;
}

/** One check as a compact badge: the condition text, colored by severity, the
 *  business-readable message (when present) in a tooltip. */
export function StepCheckBadge({ check }: { check: StepCheckView }) {
	const badge = (
		<Badge
			color={severityColor(check.severity)}
			variant="light"
			size="xs"
			tt="none"
		>
			{check.condition}
		</Badge>
	);
	return check.message ? (
		<Tooltip label={check.message} multiline maw={280} withArrow>
			{badge}
		</Tooltip>
	) : (
		badge
	);
}

// Bound the row (rule 15) — a step normally declares one or two checks; this
// only guards a pathological declaration from spilling the row.
const MAX_VISIBLE_CHECKS = 6;

/** A step's checks as a compact wrapped row of badges — for a widget with room
 *  per step (the DAG step list). Renders nothing for an empty list, so a step
 *  without checks looks exactly like today. */
export function StepCheckBadges({ checks }: { checks: StepCheckView[] }) {
	if (checks.length === 0) return null;
	const visible = checks.slice(0, MAX_VISIBLE_CHECKS);
	const overflow = checks.length - visible.length;
	return (
		<Group gap={4} wrap="wrap" data-testid="step-check-badges">
			{visible.map((c) => (
				<StepCheckBadge
					key={`${c.condition}|${c.severity}|${c.message}`}
					check={c}
				/>
			))}
			{overflow > 0 && (
				<Badge color="gray" variant="outline" size="xs" tt="none">
					+{overflow}
				</Badge>
			)}
		</Group>
	);
}

/** A step's checks as ONE compact icon + tooltip — for a space-constrained
 *  widget (the canvas node face). Renders nothing for an empty list. The
 *  tooltip lists every condition (severity: condition — message), so nothing
 *  is lost to the compact form. */
export function StepCheckIndicator({ checks }: { checks: StepCheckView[] }) {
	if (checks.length === 0) return null;
	const label = checks
		.map((c) =>
			[`${c.severity ?? "error"}: ${c.condition}`, c.message]
				.filter(Boolean)
				.join(" — "),
		)
		.join("; ");
	// The worst (first-listed-severity) color loosely orients the icon; the
	// tooltip carries the real detail, so a mixed-severity set isn't lossy.
	const color = severityColor(checks[0].severity);
	return (
		<Tooltip label={label} multiline maw={280} withArrow>
			<ShieldCheck
				size={14}
				color={`var(--mantine-color-${color}-filled)`}
				style={{ flexShrink: 0 }}
				data-testid="step-check-indicator"
			/>
		</Tooltip>
	);
}
