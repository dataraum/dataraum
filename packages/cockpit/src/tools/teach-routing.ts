// Teach → affected engine stage (DAT-531). A teach is a pure config_overlay
// write; this map says WHICH whole stage must re-run to apply it — the ENTRY
// stage. Downstream staleness then derives from the generation-head log
// (stage-staleness.ts), so this map only names the shallowest stage a teach
// touches, never the cascade.
//
// Born-loud, not a frozen switch: the teach-type enum changes over time, so every
// `TeachType` MUST appear in `TEACH_STAGE` (a missing one is a COMPILE error via
// the `Record<TeachType, …>`), and an unknown runtime string (overlay rows carry
// `type` as varchar) THROWS rather than silently misrouting.

import type { RunStage } from "#/db/cockpit/runs";
import { TEACH_TYPES, type TeachType } from "#/tools/teach.validation";

/** The engine stage each teach type's correction re-grounds at (DAT-531). The
 * `Record<TeachType, …>` is the completeness guard — adding a teach type without a
 * stage here fails to type-check. */
const TEACH_STAGE: Record<TeachType, RunStage> = {
	// Grounding teaches re-ground typed columns — applied by re-running add_source.
	// (A concept change re-grounds here too; operating_model going stale off it
	// falls out of the head log downstream, not this map.)
	type_pattern: "add_source",
	null_value: "add_source",
	unit: "add_source",
	concept: "add_source",
	// A concept-property patch re-grounds the semantic binding too (add_source);
	// operating_model going stale off a concept change derives downstream.
	concept_property: "add_source",
	// A rebind appends the column to the target concept's indicators → it only
	// takes effect through the next run's grounding prompt (add_source).
	rebind: "add_source",
	// Relationships + dimension hierarchies are begin_session products
	// (`run_relationships` / `run_dimension_hierarchies`).
	relationship: "begin_session",
	hierarchy: "begin_session",
	// The operating-model families — the cheap case: re-run operating_model only.
	validation: "operating_model",
	cycle: "operating_model",
	metric: "operating_model",
};

/**
 * The stage that must re-run to apply a teach of `type`. Accepts a raw string
 * (overlay rows are varchar-typed) and fails born-loud on an unmapped type — a new
 * teach type can't silently route nowhere.
 */
export function affectedStage(type: string): RunStage {
	const stage = (TEACH_STAGE as Record<string, RunStage | undefined>)[type];
	if (!stage) {
		throw new Error(
			`[teach-routing] no stage mapped for teach type '${type}' — every teach ` +
				`type must declare its affected stage (DAT-531). Known types: ${TEACH_TYPES.join(", ")}`,
		);
	}
	return stage;
}
