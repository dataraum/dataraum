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
