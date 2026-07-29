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

/** A check tagged with the step that declared it — the canvas-node indicator
 *  surfaces the union of every step's checks (DAT-840 owner ruling), so each
 *  entry needs to say which step it's on. Structural, same reasoning as
 *  `StepCheckView`. */
export interface StepCheckWithOrigin extends StepCheckView {
	stepId: string;
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

// `severity` is untrusted persisted text (the engine's own YAML/induction
// writers set it, but nothing validates it round-trips through this cockpit's
// bounds) — a plain `SEVERITY_COLOR[key]` lookup resolves an inherited key
// like "constructor" or "toString" through Object.prototype to a FUNCTION
// (truthy, so `?? SEVERITY_COLOR.error` never fires), and Mantine's
// parse-theme-color throws on a non-string color deep inside render, taking
// the whole Model page down. `Object.hasOwn` guards the lookup to the
// object's OWN keys only.
function severityColor(severity: string | null): string {
	const key = severity ?? "error";
	return Object.hasOwn(SEVERITY_COLOR, key)
		? SEVERITY_COLOR[key]
		: SEVERITY_COLOR.error;
}

/** One check as a compact badge: the condition text, colored by severity. The
 *  tooltip always carries the severity word (color alone doesn't read as
 *  "critical" vs "warning" to everyone) plus the business-readable message
 *  when present. */
export function StepCheckBadge({ check }: { check: StepCheckView }) {
	const severity = check.severity ?? "error";
	const tooltipLabel = check.message
		? `${severity} — ${check.message}`
		: severity;
	return (
		<Tooltip label={tooltipLabel} multiline maw={280} withArrow>
			<Badge
				color={severityColor(check.severity)}
				variant="light"
				size="xs"
				tt="none"
				data-testid="step-check-badge"
			>
				{check.condition}
			</Badge>
		</Tooltip>
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
			{visible.map((c, i) => (
				<StepCheckBadge
					// biome-ignore lint/suspicious/noArrayIndexKey: static list (an already-narrowed DAG's declared checks), never reordered; a composite content key collides on two identical checks
					key={i}
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

/** A metric's checks across EVERY step (DAT-840 owner ruling — the canvas
 *  node is the only check surface for a runnable metric, and the engine's
 *  verifier flags on any step, not just the output step) as ONE compact icon
 *  + tooltip — for the space-constrained canvas node face. Renders nothing
 *  for an empty list. Each tooltip entry names its step, so a check on a
 *  leaf extract doesn't read as if it were on the output. */
export function StepCheckIndicator({
	checks,
}: {
	checks: StepCheckWithOrigin[];
}) {
	if (checks.length === 0) return null;
	const label = checks
		.map((c) => {
			const head = `${c.stepId} · ${c.severity ?? "error"}: ${c.condition}`;
			return c.message ? `${head} — ${c.message}` : head;
		})
		.join("; ");
	// The first-listed check's color loosely orients the icon; the tooltip
	// (and its `aria-label` twin, for keyboard/screen-reader reach) carries
	// the full per-step detail, so a mixed-severity set isn't lossy.
	const color = severityColor(checks[0].severity);
	return (
		<Tooltip label={label} multiline maw={280} withArrow>
			<ShieldCheck
				size={14}
				color={`var(--mantine-color-${color}-filled)`}
				style={{ flexShrink: 0 }}
				data-testid="step-check-indicator"
				// Mantine's Tooltip only opens on hover/focus — a keyboard or
				// screen-reader user needs the icon to be focusable AND to carry
				// the content directly (aria-label), not just visually on hover.
				tabIndex={0}
				role="img"
				aria-label={label}
			/>
		</Tooltip>
	);
}
