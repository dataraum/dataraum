// The grammar of the engine-written `entropy_readiness.intents` / `top_drivers`
// JSONB (DAT-394/399), shared by every reader (look_table, why_column) so the
// "one JSONB shape" contract is mechanical, not duplicated per tool. If the
// engine changes the persisted driver/intent shape, this is the single edit.
//
// Parsed leniently at the call site (zod `safeParse`): a malformed/absent blob
// degrades to empty rather than throwing a read tool.

import { z } from "zod";

/** One readiness driver — a labeled network dimension with its causal impact.
 * Self-describing (carries its own `label` + `dimension_path`), so the cockpit
 * needs no engine network vocabulary (DAT-399 B). */
export const ReadinessDriver = z.object({
	node: z.string(),
	dimension_path: z.string(),
	label: z.string(),
	state: z.string(),
	impact_delta: z.number(),
});
export type ReadinessDriver = z.infer<typeof ReadinessDriver>;

/** One per-intent readiness entry inside `entropy_readiness.intents`. `intent`
 * is the engine's intent-layer NODE KEY (e.g. `aggregation_intent`). */
export const PersistedIntent = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
	drivers: z.array(ReadinessDriver).default([]),
});
export type PersistedIntent = z.infer<typeof PersistedIntent>;

/** One entry of `entropy_readiness.abstentions` (DAT-853) — the self-describing
 * trace of a loss-path detector that could NOT measure: which detector, WHY
 * (`missing_inputs` / `detector_error` / `insufficient_data` / `not_applicable`),
 * and the intents whose risk is now missing a contributor. This is what turns a
 * vacuous "ready" band into an honest "not measured (reason)" for the reader. */
export const Abstention = z.object({
	detector: z.string(),
	reason: z.string(),
	intents: z.array(z.string()).default([]),
});
export type Abstention = z.infer<typeof Abstention>;
