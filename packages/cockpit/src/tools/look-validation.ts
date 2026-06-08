// look_validation tool (DAT-440) — a session's operating_model validation
// overview. The validation analog of look_relationships: where that grids a
// session's detected relationships, this grids the session's declared
// validations with their lifecycle state and executed result.
//
// Pure read via the Drizzle metadata client over the promoted-read surface
// (ADR-0008/DAT-453): `current_lifecycle_artifacts` carries every declared
// validation's lifecycle row (state declared/grounded/executed + the
// "visibly impossible" state_reason when it could not run), and
// `current_validation_results` the executed outcome (status/passed/message) —
// both head-joined in the database to the session's promoted `operating_model`
// run. The join key between them is `artifact_key == validation_id` (the
// engine writes BOTH rows per declared spec). State, reason, and message are
// the engine's persisted values verbatim — never re-derived here (only
// digest-sanitized). Read-only → no approval.
//
// The DB read is integration-smoke-covered (scripts/smoke-operating-model.ts);
// the pure row→shape projection is unit-tested via `projectValidationOverview`.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { sessionHeadTarget } from "../db/metadata/relationship-target";
import {
	currentLifecycleArtifacts,
	currentValidationResults,
	metadataSnapshotHead,
} from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";

// --- The tool's output: one row per declared validation.

const ValidationOverview = z.object({
	// The validation's key (== the lifecycle artifact_key, e.g.
	// "gl_invoice_match") — feeds why_validation for the drill-down.
	validation_id: z.string(),
	// Lifecycle state: declared → grounded → executed. A non-executed state is
	// always paired with `state_reason` (the fail-loud contract).
	state: z.string(),
	// WHY the validation stopped short of executed (e.g. "Missing required
	// tables: …") — the engine's reason verbatim; null once executed.
	state_reason: z.string().nullable(),
	severity: z.string().nullable(),
	// The executed result, joined by validation_id: the engine's status string,
	// pass/fail, and message — null fields when no result row exists.
	status: z.string().nullable(),
	passed: z.boolean().nullable(),
	message: z.string().nullable(),
});
export type ValidationOverview = z.infer<typeof ValidationOverview>;

const LookValidationResult = z.object({
	session_id: z.string(),
	// False when the session has no promoted operating_model run yet — the
	// widget should say "not run" rather than imply zero declared validations.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	validations: z.array(ValidationOverview),
});
export type LookValidationResult = z.infer<typeof LookValidationResult>;

/** One current_lifecycle_artifacts row, as Drizzle returns it. */
export interface LifecycleArtifactRow {
	artifactKey: string;
	state: string | null;
	stateReason: string | null;
}

/** One current_validation_results row, keyed by its validation_id. */
export interface ValidationResultRow {
	status: string | null;
	severity: string | null;
	passed: boolean | null;
	message: string | null;
}

/**
 * Project one lifecycle artifact (+ its joined result row, when present) to the
 * tool's shape. Pure (no DB) so the join + sanitization is unit-testable.
 * `state_reason` / `message` are engine-built free text that can embed raw
 * `src_<digest>__` physical names — pass them through the digest backstop
 * before they reach the agent (the workflow_status failure.message precedent).
 */
export function projectValidationOverview(
	artifact: LifecycleArtifactRow,
	result: ValidationResultRow | undefined,
): ValidationOverview {
	return {
		validation_id: artifact.artifactKey,
		state: artifact.state ?? "",
		state_reason:
			artifact.stateReason === null
				? null
				: stripSrcDigests(artifact.stateReason),
		severity: result?.severity ?? null,
		status: result?.status ?? null,
		passed: result?.passed ?? null,
		message: result?.message == null ? null : stripSrcDigests(result.message),
	};
}

export interface LookValidationInput {
	session_id: string;
}

/** Per-validation lifecycle + result for one session's promoted operating_model run. */
export async function lookValidation(
	input: LookValidationInput,
): Promise<LookValidationResult> {
	// `analyzed` = the session PROMOTED an operating_model run — distinct from
	// "promoted but zero declared validations" (a vertical with none), which must
	// not read as never-ran. The head pass-through stays on the read surface for
	// exactly this check; the rows themselves come from the current_* views.
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, sessionHeadTarget(input.session_id)),
				eq(metadataSnapshotHead.stage, "operating_model"),
			),
		)
		.limit(1);
	if (!head?.runId) {
		return {
			session_id: input.session_id,
			analyzed: false,
			pending_teaches: 0,
			validations: [],
		};
	}

	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database. Scope to validation artifacts explicitly — the
	// lifecycle substrate is typed and later slices add cycles/metrics artifacts.
	const rawArtifacts = await metadataDb
		.select({
			artifactKey: currentLifecycleArtifacts.artifactKey,
			state: currentLifecycleArtifacts.state,
			stateReason: currentLifecycleArtifacts.stateReason,
		})
		.from(currentLifecycleArtifacts)
		.where(
			and(
				eq(currentLifecycleArtifacts.sessionId, input.session_id),
				eq(currentLifecycleArtifacts.artifactType, "validation"),
			),
		)
		.orderBy(asc(currentLifecycleArtifacts.artifactKey));
	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the identity field the underlying table guarantees.
	const artifacts: LifecycleArtifactRow[] = rawArtifacts.map((a) => ({
		artifactKey: a.artifactKey ?? "",
		state: a.state,
		stateReason: a.stateReason,
	}));

	const rawResults = await metadataDb
		.select({
			validationId: currentValidationResults.validationId,
			status: currentValidationResults.status,
			severity: currentValidationResults.severity,
			passed: currentValidationResults.passed,
			message: currentValidationResults.message,
		})
		.from(currentValidationResults)
		.where(eq(currentValidationResults.sessionId, input.session_id));
	const resultByKey = new Map<string, ValidationResultRow>(
		rawResults.map((r) => [
			r.validationId ?? "",
			{
				status: r.status,
				severity: r.severity,
				passed: r.passed,
				message: r.message,
			},
		]),
	);

	const validations = artifacts.map((a) =>
		projectValidationOverview(a, resultByKey.get(a.artifactKey)),
	);

	const pending = await getPendingOverlays();

	return {
		session_id: input.session_id,
		analyzed: true,
		pending_teaches: pending.length,
		validations,
	};
}

export const lookValidationTool = toolDefinition({
	name: "look_validation",
	description:
		"Show a session's operating-model validation outcomes — every declared " +
		"validation with its lifecycle state (declared / grounded / executed), " +
		"the reason it could not run when it stopped short, and the executed " +
		"result (pass / fail + message). Read-only; reflects the promoted " +
		"operating_model run for the session (run the operating_model tool " +
		"first). pending_teaches counts un-applied teaches across the workspace. " +
		"Use `why_validation` to drill into a specific validation.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe(
				"The begin_session session whose validations to inspect (its session_id).",
			),
	}),
	outputSchema: LookValidationResult,
}).server((input) => lookValidation(input));
