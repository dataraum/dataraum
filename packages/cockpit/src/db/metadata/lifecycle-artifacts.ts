// Shared current_lifecycle_artifacts reader (DAT-465) — the read half of the
// typed-artifact lifecycle substrate (DAT-438), pinned by artifact_type.
//
// Every operating_model family (validation, cycle, metric) declares its
// lifecycle artifacts into the SAME current_lifecycle_artifacts view,
// discriminated by artifact_type. So look_* and why_* across all three families
// read the SAME rows the SAME way — the promoted run's head check, the typed
// list, and the single typed row — and differ ONLY in the family-specific RESULT
// join (validation → current_validation_results, cycle →
// current_detected_business_cycles, metric → re-run the promoted SQL). This
// module owns that shared read so a new family's tool is mostly its own schema +
// its own second read (the DAT-465 → DAT-466 substrate goal).
//
// The current_* views ARE the promoted operating_model run (ADR-0008/DAT-453):
// the head join lives in the database, so the reads here carry no run-id
// plumbing. `readOperatingModelHead` is the one extra read — the "analyzed"
// check, distinguishing "promoted but zero declared artifacts" (a vertical that
// ships none) from "never ran", which the empty current_* views alone cannot.

import { and, asc, eq } from "drizzle-orm";

import { metadataDb } from "./client";
import { catalogHeadTarget } from "./relationship-target";
import { currentLifecycleArtifacts, metadataSnapshotHead } from "./schema";

/** One current_lifecycle_artifacts row at LIST grain — what look_* needs per
 * declared artifact. `artifactKey` is the family identity (validation_id /
 * canonical_type / graph_id). */
export interface LifecycleArtifactRow {
	artifactKey: string;
	state: string | null;
	stateReason: string | null;
}

/** One current_lifecycle_artifacts row at DETAIL grain — what why_* needs to
 * drill into a single artifact (adds the strictness dial + what it bound
 * against). */
export interface LifecycleArtifactDetail {
	state: string | null;
	stateReason: string | null;
	strictness: number | null;
	groundedAgainst: unknown;
}

/**
 * The promoted operating_model run id for the workspace, or null when none is
 * promoted (DAT-506: the workspace catalog head, no session axis). The
 * "analyzed" signal: the current_* views can't tell "promoted, zero declared
 * artifacts" from "never ran" (both yield empty rows), so the head pass-through
 * stays for exactly this check.
 */
export async function readOperatingModelHead(): Promise<string | null> {
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, catalogHeadTarget()),
				eq(metadataSnapshotHead.stage, "operating_model"),
			),
		)
		.limit(1);
	return head?.runId ?? null;
}

/**
 * Every declared artifact of one type in a session's promoted run, ordered by
 * key. Scope to the artifact_type explicitly — the lifecycle substrate is shared
 * across families, so an unpinned read would mix validations, cycles, and
 * metrics.
 */
export async function readLifecycleArtifactRows(
	artifactType: string,
): Promise<LifecycleArtifactRow[]> {
	const rows = await metadataDb
		.select({
			artifactKey: currentLifecycleArtifacts.artifactKey,
			state: currentLifecycleArtifacts.state,
			stateReason: currentLifecycleArtifacts.stateReason,
		})
		.from(currentLifecycleArtifacts)
		.where(eq(currentLifecycleArtifacts.artifactType, artifactType))
		.orderBy(asc(currentLifecycleArtifacts.artifactKey));
	// View columns type as nullable (a Postgres view carries no NOT NULL) —
	// coalesce the identity field the underlying table guarantees.
	return rows.map((r) => ({
		artifactKey: r.artifactKey ?? "",
		state: r.state,
		stateReason: r.stateReason,
	}));
}

/**
 * One artifact's detail row (why_*), or null when the key isn't in the promoted
 * run. artifact_key is unique only WITHIN a type, so the type pin is required —
 * a cycle and a validation could share a key.
 */
export async function readLifecycleArtifact(
	artifactType: string,
	artifactKey: string,
): Promise<LifecycleArtifactDetail | null> {
	const [row] = await metadataDb
		.select({
			state: currentLifecycleArtifacts.state,
			stateReason: currentLifecycleArtifacts.stateReason,
			strictness: currentLifecycleArtifacts.strictness,
			groundedAgainst: currentLifecycleArtifacts.groundedAgainst,
		})
		.from(currentLifecycleArtifacts)
		.where(
			and(
				eq(currentLifecycleArtifacts.artifactType, artifactType),
				eq(currentLifecycleArtifacts.artifactKey, artifactKey),
			),
		)
		.limit(1);
	return row ?? null;
}
