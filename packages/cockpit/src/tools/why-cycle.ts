// why_cycle tool (DAT-465) — explain one business cycle's state.
//
// The per-cycle drill-down behind look_cycle, mirroring why_validation in shape
// (found discriminant, session-scoped read over the promoted run, pure
// unit-tested projection, NO LLM synthesis — the engine's rows are rendered
// verbatim). The drill-down's value over the list is the grounded detail: what
// the cycle bound against, the structural completion measurement, the status
// column it was measured on, the detected stages + entity flows, and the
// detection evidence.
//
// Read-only → no approval. The pure row→shape assembly (`projectWhyCycle`) is
// unit-tested; the live DB read is integration-smoke-covered
// (scripts/smoke-operating-model.ts).

import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactDetail,
	readLifecycleArtifact,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { currentDetectedBusinessCycles } from "../db/metadata/schema";
import { renderEvidenceDetail, stripSrcDigests } from "../lib/display-names";

// --- Tool output (mirrors the why_* found/anatomy conventions, keyed on the
// cycle's canonical_type).

const WhyCycleResult = z.object({
	canonical_type: z.string(),
	// False when the canonical_type matched no lifecycle artifact (and no
	// detection row) in the session's promoted operating_model run.
	found: z.boolean(),
	// The detected cycle's descriptive name; null when not detected.
	cycle_name: z.string().nullable(),
	// Lifecycle: declared → grounded → executed; the engine's persisted state.
	state: z.string().nullable(),
	// WHY it stopped short of executed — the engine's reason verbatim
	// (digest-sanitized); null once executed. The "visibly impossible" surface.
	state_reason: z.string().nullable(),
	// The lifecycle strictness dial the artifact was declared with.
	strictness: z.number().nullable(),
	// What the cycle bound against (the base-run map), rendered through the shared
	// evidence sanitizer — "" when the artifact never grounded.
	grounded_against: z.string(),
	// Detection facts — null when not detected.
	is_known_type: z.boolean().nullable(),
	business_value: z.string().nullable(),
	confidence: z.number().nullable(),
	description: z.string().nullable(),
	// The structural completion measurement.
	completion_rate: z.number().nullable(),
	completed_cycles: z.number().nullable(),
	total_records: z.number().nullable(),
	// The status column the completion was measured on (and the value that means
	// complete) — the measurement's provenance, digest-sanitized.
	status_table: z.string().nullable(),
	status_column: z.string().nullable(),
	completion_value: z.string().nullable(),
	// The detected stages, entity flows, participating tables, and detection
	// evidence — unknown-shape JSON rendered through the shared sanitizer
	// (bounded arrays, truncated leaves); "" when absent.
	stages: z.string(),
	entity_flows: z.string(),
	tables_involved: z.string(),
	evidence: z.string(),
	pending_teaches: z.number(),
});
export type WhyCycleResult = z.infer<typeof WhyCycleResult>;

/** The cycle's lifecycle artifact row (null = no such artifact) — the shared
 * lifecycle-detail shape, aliased here for the projection's callers. */
export type WhyCycleArtifactRow = LifecycleArtifactDetail;

/** The cycle's detection row (null = not detected / no row). */
export interface WhyCycleDetectionRow {
	cycleName: string | null;
	isKnownType: boolean | null;
	businessValue: string | null;
	confidence: number | null;
	description: string | null;
	completionRate: number | null;
	completedCycles: number | null;
	totalRecords: number | null;
	statusTable: string | null;
	statusColumn: string | null;
	completionValue: string | null;
	stages: unknown;
	entityFlows: unknown;
	tablesInvolved: unknown;
	evidence: unknown;
}

/**
 * Assemble the why-payload from the artifact + detection rows. Pure (no DB) so
 * the sanitization + null-handling is unit-testable. `found` distinguishes "no
 * such cycle in this run" from a found-but-not-executed one. Engine-built free
 * text (`state_reason`, `cycle_name`, `description`, `status_table`) can embed
 * raw `src_<digest>__` physical names — every string passes the digest backstop;
 * unknown-shape JSON (`grounded_against`, `stages`, `entity_flows`,
 * `tables_involved`, `evidence`) renders through the shared evidence sanitizer,
 * never assumed.
 */
export function projectWhyCycle(
	canonicalType: string,
	artifact: WhyCycleArtifactRow | null,
	detected: WhyCycleDetectionRow | null,
	pendingTeaches: number,
): WhyCycleResult {
	return {
		canonical_type: canonicalType,
		found: artifact !== null || detected !== null,
		cycle_name:
			detected?.cycleName == null ? null : stripSrcDigests(detected.cycleName),
		state: artifact?.state ?? null,
		state_reason:
			artifact?.stateReason == null
				? null
				: stripSrcDigests(artifact.stateReason),
		strictness: artifact?.strictness ?? null,
		grounded_against: renderEvidenceDetail(artifact?.groundedAgainst),
		is_known_type: detected?.isKnownType ?? null,
		business_value: detected?.businessValue ?? null,
		confidence: detected?.confidence ?? null,
		description:
			detected?.description == null
				? null
				: stripSrcDigests(detected.description),
		completion_rate: detected?.completionRate ?? null,
		completed_cycles: detected?.completedCycles ?? null,
		total_records: detected?.totalRecords ?? null,
		status_table:
			detected?.statusTable == null
				? null
				: stripSrcDigests(detected.statusTable),
		status_column:
			detected?.statusColumn == null
				? null
				: stripSrcDigests(detected.statusColumn),
		completion_value:
			detected?.completionValue == null
				? null
				: stripSrcDigests(detected.completionValue),
		stages: renderEvidenceDetail(detected?.stages),
		entity_flows: renderEvidenceDetail(detected?.entityFlows),
		tables_involved: renderEvidenceDetail(detected?.tablesInvolved),
		evidence: renderEvidenceDetail(detected?.evidence),
		pending_teaches: pendingTeaches,
	};
}

export interface WhyCycleInput {
	canonical_type: string;
}

/** Explain one cycle's state: lifecycle + grounding + measured completion. */
export async function whyCycle(input: WhyCycleInput): Promise<WhyCycleResult> {
	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database — no head resolution, no runId plumbing. No promoted
	// run → empty views → not found. The shared reader pins artifact_type =
	// 'cycle' (the key is unique only WITHIN a type — validations/metrics share
	// this view).
	const artifactRow = await readLifecycleArtifact(
		"cycle",
		input.canonical_type,
	);

	const [detectionRow] = await metadataDb
		.select({
			cycleName: currentDetectedBusinessCycles.cycleName,
			isKnownType: currentDetectedBusinessCycles.isKnownType,
			businessValue: currentDetectedBusinessCycles.businessValue,
			confidence: currentDetectedBusinessCycles.confidence,
			description: currentDetectedBusinessCycles.description,
			completionRate: currentDetectedBusinessCycles.completionRate,
			completedCycles: currentDetectedBusinessCycles.completedCycles,
			totalRecords: currentDetectedBusinessCycles.totalRecords,
			statusTable: currentDetectedBusinessCycles.statusTable,
			statusColumn: currentDetectedBusinessCycles.statusColumn,
			completionValue: currentDetectedBusinessCycles.completionValue,
			stages: currentDetectedBusinessCycles.stages,
			entityFlows: currentDetectedBusinessCycles.entityFlows,
			tablesInvolved: currentDetectedBusinessCycles.tablesInvolved,
			evidence: currentDetectedBusinessCycles.evidence,
		})
		.from(currentDetectedBusinessCycles)
		.where(
			eq(currentDetectedBusinessCycles.canonicalType, input.canonical_type),
		)
		.limit(1);

	const pending = await getPendingOverlays();

	return projectWhyCycle(
		input.canonical_type,
		artifactRow,
		detectionRow ?? null,
		pending.length,
	);
}

export const whyCycleTool = toolDefinition({
	name: "why_cycle",
	description:
		"Explain ONE business cycle's state in a session's operating-model run — " +
		"its lifecycle state with the reason it could not be measured (when it " +
		"stopped short of executed), what it bound against, the structural " +
		"completion (rate + counts), the status column it was measured on, and the " +
		"detected stages, entity flows, and evidence. Read-only. Use after " +
		"look_cycle to drill into a specific cycle; identify it by its " +
		"canonical_type.",
	inputSchema: z.object({
		canonical_type: z
			.string()
			.describe("The cycle to explain (a canonical_type from look_cycle)."),
	}),
	outputSchema: WhyCycleResult,
}).server((input) => whyCycle(input));
