// The engine journey (DAT-347) — the ordered spine the cockpit narrates.
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
 * marks the stages the practitioner drives directly in the cockpit (today only
 * `add_source` — the rest are entered/observed, not hand-operated). `label` is
 * the human-facing name for the stage navigator.
 */
export interface JourneyStage {
	id: Stage;
	label: string;
	/**
	 * Whether the practitioner operates this stage from the cockpit. Only
	 * `add_source` is interactive today; more flip on as their widgets land.
	 * The stage navigator disables non-interactive chips.
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
 * the UI can branch exhaustively. This is the SHAPE only; the readiness *signal*
 * (which stage is which) is computed from engine state elsewhere.
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
 * a full source replay, etc.). STILL A STUB — the slot is reserved so the stage
 * navigator and readiness logic can wire to it without a churn later. The real
 * cost model (re-run depth) is not built yet.
 */
export function reEntryCost(_stage: Stage): never {
	throw new Error("reEntryCost is not implemented (DAT-347 stub)");
}

// --- onboarding readiness from Source.stage (DAT-378) -----------------------
//
// A Source carries a `stage` cursor (engine `sources.stage` column, mirrored
// into the Drizzle metadata schema): the furthest onboarding stage the cockpit
// has driven it through. The cockpit walks a source `connect → frame → select →
// add_source` BEFORE the AddSourceWorkflow triggers, persisting the cursor after
// each step so a reload resumes where it left off. These four are the
// *onboarding* prefix of JOURNEY_STAGES; the tail (`begin_session` onward) is
// engine-execution state the workflow drives, not a Source cursor, so it is out
// of this readiness map.

/** The onboarding stages a Source.stage cursor can name, in order. */
export const ONBOARDING_STAGES = [
	"connect",
	"frame",
	"select",
	"add_source",
] as const satisfies readonly Stage[];

export type OnboardingStage = (typeof ONBOARDING_STAGES)[number];

/** True when `stage` is one of the onboarding cursor values. */
export function isOnboardingStage(stage: string): stage is OnboardingStage {
	return (ONBOARDING_STAGES as readonly string[]).includes(stage);
}

/**
 * Readiness of one onboarding `target` stage given a Source's persisted `stage`
 * cursor (the furthest stage reached, or `null` for a brand-new source that has
 * not been connected yet).
 *
 * The cursor's position relative to `target` decides the kind:
 * - cursor reached or passed `target` → that stage is done, so the NEXT stage is
 *   the one to act on; the reached stage itself reads `ready` (re-enterable).
 * - `target` is exactly one step past the cursor → `ready` (the next action).
 * - `target` is further ahead → `not_entered` (upstream hasn't reached it).
 *
 * A `null` cursor means only `connect` is `ready`; everything downstream is
 * `not_entered`. An unknown cursor string (a stage value the cockpit doesn't
 * recognize) is treated as `null` so a forward-compat engine value can't make a
 * downstream stage spuriously look ready.
 */
export function onboardingReadiness(
	cursor: string | null | undefined,
	target: OnboardingStage,
): Readiness {
	const targetIdx = ONBOARDING_STAGES.indexOf(target);
	// A new/unknown source: only the first stage (connect) can be acted on.
	const cursorIdx =
		cursor && isOnboardingStage(cursor)
			? ONBOARDING_STAGES.indexOf(cursor)
			: -1;

	if (targetIdx <= cursorIdx) {
		// The source has reached (or passed) this stage — it can be re-entered.
		return { kind: "ready" };
	}
	if (targetIdx === cursorIdx + 1) {
		// The single next stage to act on.
		return { kind: "ready" };
	}
	// Further downstream — upstream work hasn't reached it yet.
	return { kind: "not_entered" };
}
