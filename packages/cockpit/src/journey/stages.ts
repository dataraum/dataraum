// The engine journey (DAT-347, C1) — the ordered spine the cockpit narrates.
//
// Seven stages in the order a practitioner moves through them. Each stage id is
// one of the engine pipeline stages defined in src/ui/theme.ts (the `Stage`
// union) — there is no second source of stage names. The stage *colors* live in
// theme tokens (tokens.colors.stage); this file owns the *sequence* and the
// per-stage interaction semantics the agentic view reasons about.

import type { Stage } from "#/ui/theme";

/** Re-export so journey consumers don't have to reach into the theme. */
export type { Stage };

/**
 * One stage in the journey. `id` keys into `tokens.colors.stage`; `interactive`
 * marks the stages the practitioner drives directly in the cockpit (C1: only
 * `add_source` — the rest are entered/observed, not hand-operated). `label` is
 * the human-facing name for the stage navigator.
 */
export interface JourneyStage {
	id: Stage;
	label: string;
	/**
	 * Whether the practitioner operates this stage from the cockpit. Only
	 * `add_source` is interactive in C1; later columns flip more on as their
	 * widgets land. The stage navigator disables non-interactive chips.
	 */
	interactive: boolean;
}

/**
 * The journey, in order. `connect → frame → select → add_source →
 * begin_session → operating_model → answer`. The order here is the order the
 * stage navigator renders left-to-right.
 */
export const JOURNEY_STAGES: readonly JourneyStage[] = [
	{ id: "connect", label: "Connect", interactive: false },
	{ id: "frame", label: "Frame", interactive: false },
	{ id: "select", label: "Select", interactive: false },
	{ id: "add_source", label: "Add Source", interactive: true },
	{ id: "begin_session", label: "Begin Session", interactive: false },
	{ id: "operating_model", label: "Operating Model", interactive: false },
	{ id: "answer", label: "Answer", interactive: false },
] as const;

/**
 * How ready a stage is for the practitioner to act on — a discriminated union so
 * the UI can branch exhaustively. C1 defines the shape; the readiness *signal*
 * (which stage is which) is computed from engine state in a later column.
 *
 * - `ready`        — the stage can be entered/operated now.
 * - `investigate`  — entered, but something needs the practitioner's attention.
 * - `blocked`      — cannot proceed; `reason` explains why.
 * - `not_entered`  — upstream stages haven't reached this one yet.
 */
export type Readiness =
	| { kind: "ready" }
	| { kind: "investigate"; note: string }
	| { kind: "blocked"; reason: string }
	| { kind: "not_entered" };

/**
 * Cost of re-entering a stage after it has been left (re-running upstream work,
 * replay surgery, etc.). STUB for C1 — the slot is reserved so the stage
 * navigator and readiness logic can wire to it without a churn later. The real
 * cost model (replay scope × stage depth) lands with the replay column.
 */
export function reEntryCost(_stage: Stage): never {
	throw new Error("reEntryCost is not implemented (DAT-347 C1 stub)");
}
