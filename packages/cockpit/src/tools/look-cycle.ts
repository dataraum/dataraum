// look_cycle tool (DAT-465) — a session's operating_model business-cycle
// overview. The cycle analog of look_validation: where that grids a session's
// declared validations with their lifecycle state + executed verdict, this grids
// the session's declared cycles with their lifecycle state + measured completion.
//
// Pure read via the shared lifecycle-artifacts reader (docs/architecture/persistence.md, DAT-453): the
// `cycle`-typed `current_lifecycle_artifacts` rows are the authoritative declared
// set (the engine declares ONE artifact per declared canonical_type — vocabulary
// + teaches — in business_cycles_phase), and `current_detected_business_cycles`
// carries the measured detection (cycle_name, business_value, completion_rate).
// The join key is `artifact_key == canonical_type` (the engine keys BOTH by it).
// A declared-but-not-detected cycle keeps its artifact with a state_reason and no
// detection row — the "visibly impossible" case, surfaced first-class. State and
// reason are the engine's persisted values verbatim — never re-derived here
// (only digest-sanitized). Read-only → no approval.
//
// The DB read is integration-smoke-covered (scripts/smoke-operating-model.ts);
// the pure row→shape projection is unit-tested via `projectCycleOverview`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactRow,
	readLifecycleArtifactRows,
	readOperatingModelHead,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { currentDetectedBusinessCycles } from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";

// --- The tool's output: one row per declared cycle.

const CycleOverview = z.object({
	// The cycle's key (== the lifecycle artifact_key, e.g. "order_to_cash") —
	// feeds why_cycle for the drill-down.
	canonical_type: z.string(),
	// The detected cycle's descriptive name (e.g. "Order-to-Cash Cycle"); null
	// for a declared cycle that wasn't detected in this workspace.
	cycle_name: z.string().nullable(),
	// Lifecycle state: declared → grounded → executed. A non-executed state is
	// always paired with `state_reason` (the fail-loud contract).
	state: z.string(),
	// WHY the cycle stopped short of executed (e.g. "not detected in this
	// workspace") — the engine's reason verbatim; null once executed.
	state_reason: z.string().nullable(),
	// Detection facts, joined by canonical_type — null when not detected.
	business_value: z.string().nullable(),
	is_known_type: z.boolean().nullable(),
	confidence: z.number().nullable(),
	// The structural completion measurement: rate (0–1), completed count, total
	// records considered. null when not measured.
	completion_rate: z.number().nullable(),
	completed_cycles: z.number().nullable(),
	total_records: z.number().nullable(),
});
export type CycleOverview = z.infer<typeof CycleOverview>;

const LookCycleResult = z.object({
	// False when the workspace has no promoted operating_model run yet — the widget
	// should say "not run" rather than imply zero declared cycles.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	cycles: z.array(CycleOverview),
});
export type LookCycleResult = z.infer<typeof LookCycleResult>;

/** One current_detected_business_cycles row at LIST grain, keyed by its
 * canonical_type (the join key to the lifecycle artifact). */
export interface CycleDetectionRow {
	cycleName: string | null;
	businessValue: string | null;
	isKnownType: boolean | null;
	confidence: number | null;
	completionRate: number | null;
	completedCycles: number | null;
	totalRecords: number | null;
}

/**
 * Project one lifecycle artifact (+ its joined detection row, when present) to
 * the tool's shape. Pure (no DB) so the join + sanitization is unit-testable.
 * `state_reason` / `cycle_name` are engine-built free text that can embed raw
 * `src_<digest>__` physical names — pass them through the digest backstop before
 * they reach the agent (the validation projection precedent).
 */
export function projectCycleOverview(
	artifact: LifecycleArtifactRow,
	detected: CycleDetectionRow | undefined,
): CycleOverview {
	return {
		canonical_type: artifact.artifactKey,
		cycle_name:
			detected?.cycleName == null ? null : stripSrcDigests(detected.cycleName),
		state: artifact.state ?? "",
		state_reason:
			artifact.stateReason === null
				? null
				: stripSrcDigests(artifact.stateReason),
		business_value: detected?.businessValue ?? null,
		is_known_type: detected?.isKnownType ?? null,
		confidence: detected?.confidence ?? null,
		completion_rate: detected?.completionRate ?? null,
		completed_cycles: detected?.completedCycles ?? null,
		total_records: detected?.totalRecords ?? null,
	};
}

/** Per-cycle lifecycle + detection for the workspace's promoted operating_model run. */
export async function lookCycle(): Promise<LookCycleResult> {
	// `analyzed` = the workspace PROMOTED an operating_model run — distinct from
	// "promoted but zero declared cycles" (a vertical with none), which must not
	// read as never-ran.
	const head = await readOperatingModelHead();
	if (!head) {
		return {
			analyzed: false,
			pending_teaches: 0,
			cycles: [],
		};
	}

	// The current_* views ARE the promoted run (docs/architecture/persistence.md, DAT-453): the head join
	// lives in the database. The shared reader scopes to cycle artifacts — the
	// authoritative declared set.
	const artifacts: LifecycleArtifactRow[] =
		await readLifecycleArtifactRows("cycle");

	const rawDetected = await metadataDb
		.select({
			canonicalType: currentDetectedBusinessCycles.canonicalType,
			cycleName: currentDetectedBusinessCycles.cycleName,
			businessValue: currentDetectedBusinessCycles.businessValue,
			isKnownType: currentDetectedBusinessCycles.isKnownType,
			confidence: currentDetectedBusinessCycles.confidence,
			completionRate: currentDetectedBusinessCycles.completionRate,
			completedCycles: currentDetectedBusinessCycles.completedCycles,
			totalRecords: currentDetectedBusinessCycles.totalRecords,
		})
		.from(currentDetectedBusinessCycles);
	const detectedByType = new Map<string, CycleDetectionRow>(
		rawDetected.map((d) => [
			d.canonicalType ?? "",
			{
				cycleName: d.cycleName,
				businessValue: d.businessValue,
				isKnownType: d.isKnownType,
				confidence: d.confidence,
				completionRate: d.completionRate,
				completedCycles: d.completedCycles,
				totalRecords: d.totalRecords,
			},
		]),
	);

	const cycles = artifacts.map((a) =>
		projectCycleOverview(a, detectedByType.get(a.artifactKey)),
	);

	const pending = await getPendingOverlays();

	return {
		analyzed: true,
		pending_teaches: pending.length,
		cycles,
	};
}

export const lookCycleTool = toolDefinition({
	name: "look_cycle",
	description:
		"Show the workspace's operating-model business cycles — every declared cycle " +
		"with its lifecycle state (declared / grounded / executed), the reason it " +
		"could not be measured when it stopped short (e.g. not detected in this " +
		"workspace), and the structural completion (completion rate + completed / " +
		"total counts). Read-only; reflects the promoted operating_model run (run " +
		"the operating_model tool first). pending_teaches counts un-applied teaches " +
		"across the workspace. Use `why_cycle` to drill into a specific cycle.",
	inputSchema: z.object({}),
	outputSchema: LookCycleResult,
}).server(() => lookCycle());
