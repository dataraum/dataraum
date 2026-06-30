// why_validation tool (DAT-440) — explain one validation's state.
//
// The per-validation drill-down behind look_validation, mirroring
// why_relationship / why_table in shape (found discriminant, a read over the
// workspace's promoted run, pure unit-tested projection). It differs in ONE
// deliberate way: NO LLM synthesis. The why_* readiness tools narrate
// structured numeric drivers/evidence; a validation's rows are already
// engine-authored prose — `state_reason` says exactly why it could not run,
// `message` what the execution found — and the contract is to render that
// outcome verbatim, never re-derive it. The drill-down's value over the list is
// the grounded detail: the SQL that ran, what the spec bound against, the
// strictness dial, timestamps, and the result's detail payload.
//
// Read-only → no approval. The pure row→shape assembly
// (`projectWhyValidation`) is unit-tested; the live DB read is
// integration-smoke-covered (scripts/smoke-operating-model.ts).

import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactDetail,
	readLifecycleArtifact,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { currentValidationResults } from "../db/metadata/schema";
import { renderEvidenceDetail, stripSrcDigests } from "../lib/display-names";
import { columnsUsedStrings } from "./look-validation";
import { DEFAULT_TOLERANCE, type Verdict } from "./validation-verdict";
import type { ValidationParams } from "./validation-verdict-runner";

// --- Tool output (mirrors the why_* found/anatomy conventions, keyed on the
// validation id).

const WhyValidationResult = z.object({
	validation_id: z.string(),
	// False when the id matched no lifecycle artifact (and no result row) in the
	// session's promoted operating_model run.
	found: z.boolean(),
	// Lifecycle: declared → grounded → executed; the engine's persisted state.
	state: z.string().nullable(),
	// WHY it stopped short of executed — the engine's reason verbatim
	// (digest-sanitized); null once executed. The "visibly impossible" surface.
	state_reason: z.string().nullable(),
	// The lifecycle strictness dial the artifact was declared with.
	strictness: z.number().nullable(),
	// What the spec bound against (tables/columns), rendered through the shared
	// evidence sanitizer — "" when the artifact never grounded.
	grounded_against: z.string(),
	// The executed result row (null fields when none exists yet).
	status: z.string().nullable(),
	severity: z.string().nullable(),
	passed: z.boolean().nullable(),
	message: z.string().nullable(),
	// The SQL the engine generated + ran for this validation — evidence for the
	// verdict, digest-sanitized for display (NOT a re-run key).
	sql_used: z.string().nullable(),
	executed_at: z.string().nullable(),
	// The result's detail payload, rendered through the shared evidence
	// sanitizer — "" when absent.
	details: z.string(),
	// The exact "table.column" entries the executed check read (DAT-509) —
	// which columns a failed check implicates. Empty until executed.
	columns_used: z.array(z.string()),
	pending_teaches: z.number(),
});
export type WhyValidationResult = z.infer<typeof WhyValidationResult>;

/** The validation's lifecycle artifact row (null = no such artifact) — the
 * shared lifecycle-detail shape, aliased here for the projection's callers. */
export type WhyValidationArtifactRow = LifecycleArtifactDetail;

/** The validation's result row — a pure SQL store (ADR-0017; null = no row). */
export interface WhyValidationResultRow {
	sqlUsed: string | null;
	executedAt: Date | null;
	// JSON column — unknown at the boundary, narrowed by columnsUsedStrings.
	columnsUsed: unknown;
}

/**
 * Assemble the why-payload from the artifact + result rows. Pure (no DB) so
 * the sanitization + null-handling is unit-testable. `found` distinguishes "no
 * such validation in this run" from a found-but-not-executed one. Engine-built
 * free text (`state_reason`, `message`, `sql_used`) can embed raw
 * `src_<digest>__` physical names — every string passes the digest backstop;
 * unknown-shape JSON (`grounded_against`, `details`) renders through the
 * shared evidence sanitizer, never assumed.
 */
export function projectWhyValidation(
	validationId: string,
	artifact: WhyValidationArtifactRow | null,
	result: WhyValidationResultRow | null,
	verdict: Verdict | undefined,
	params: ValidationParams | undefined,
	pendingTeaches: number,
): WhyValidationResult {
	return {
		validation_id: validationId,
		found: artifact !== null || result !== null,
		state: artifact?.state ?? null,
		state_reason:
			artifact?.stateReason == null
				? null
				: stripSrcDigests(artifact.stateReason),
		strictness: artifact?.strictness ?? null,
		grounded_against: renderEvidenceDetail(artifact?.groundedAgainst),
		// The verdict is recomputed on demand (ADR-0017), not read; `severity` is
		// the declared spec param; `details` renders the recomputed measurement.
		status: verdict?.status ?? null,
		severity: params?.severity ?? null,
		passed: verdict?.passed ?? null,
		message: verdict?.message == null ? null : stripSrcDigests(verdict.message),
		sql_used: result?.sqlUsed == null ? null : stripSrcDigests(result.sqlUsed),
		executed_at: result?.executedAt?.toISOString() ?? null,
		details: verdict
			? renderEvidenceDetail({
					deviation: verdict.deviation,
					magnitude: verdict.magnitude,
					tolerance: params?.tolerance,
				})
			: "",
		columns_used: columnsUsedStrings(result?.columnsUsed),
		pending_teaches: pendingTeaches,
	};
}

export interface WhyValidationInput {
	validation_id: string;
}

/** Explain one validation's state: lifecycle + grounding + executed result. */
export async function whyValidation(
	input: WhyValidationInput,
): Promise<WhyValidationResult> {
	// The current_* views ARE the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database — no head resolution, no runId plumbing. No promoted
	// run → empty views → not found. The shared reader pins artifact_type =
	// 'validation' (the key is unique only WITHIN a type — cycles/metrics share
	// this view).
	const artifactRow = await readLifecycleArtifact(
		"validation",
		input.validation_id,
	);

	const [resultRow] = await metadataDb
		.select({
			sqlUsed: currentValidationResults.sqlUsed,
			executedAt: currentValidationResults.executedAt,
			columnsUsed: currentValidationResults.columnsUsed,
		})
		.from(currentValidationResults)
		.where(eq(currentValidationResults.validationId, input.validation_id))
		.limit(1);

	const pending = await getPendingOverlays();

	// Verdict computed ON DEMAND (ADR-0017): re-run sql_used + judge with the
	// declared tolerance from the vertical's specs. Server-only runner, lazy.
	const { resolveActiveWorkspaceRow } = await import("../db/cockpit/registry");
	const { vertical } = await resolveActiveWorkspaceRow();
	const { loadValidationParams, runValidationVerdicts } = await import(
		"./validation-verdict-runner"
	);
	const params = await loadValidationParams(vertical);
	const param = params.get(input.validation_id);
	const verdicts = resultRow
		? await runValidationVerdicts([
				{
					validationId: input.validation_id,
					sqlUsed: resultRow.sqlUsed,
					tolerance: param?.tolerance ?? DEFAULT_TOLERANCE,
				},
			])
		: new Map<string, Verdict>();

	return projectWhyValidation(
		input.validation_id,
		artifactRow ?? null,
		resultRow ?? null,
		verdicts.get(input.validation_id),
		param,
		pending.length,
	);
}

export const whyValidationTool = toolDefinition({
	name: "why_validation",
	description:
		"Explain ONE validation's state in a session's operating-model run — its " +
		"lifecycle state with the reason it could not run (when it stopped short " +
		"of executed), what it bound against, the SQL that executed, and the " +
		"result's message and details. Read-only. Use after look_validation to " +
		"drill into a specific validation; identify it by its validation_id.",
	inputSchema: z.object({
		validation_id: z
			.string()
			.describe(
				"The validation to explain (a validation_id from look_validation).",
			),
	}),
	outputSchema: WhyValidationResult,
}).server((input) => whyValidation(input));
