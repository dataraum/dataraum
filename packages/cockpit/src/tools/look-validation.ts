// look_validation tool (DAT-440) — a session's operating_model validation
// overview. The validation analog of look_relationships: where that grids a
// session's detected relationships, this grids the session's declared
// validations with their lifecycle state and executed result.
//
// Pure read via the Drizzle metadata client over the promoted-read surface
// (docs/architecture/persistence.md, DAT-453): the shared lifecycle-artifacts reader
// (`db/metadata/lifecycle-artifacts.ts`) carries every declared validation's
// lifecycle row (state declared/grounded/executed + the "visibly impossible"
// state_reason when it could not run), pinned to `artifact_type = 'validation'`;
// `current_validation_results` carries the executed outcome
// (status/passed/message) — both head-joined in the database to the session's
// promoted `operating_model` run. The join key between them is
// `artifact_key == validation_id` (the engine writes BOTH rows per declared
// spec). State, reason, and message are the engine's persisted values verbatim —
// never re-derived here (only digest-sanitized). Read-only → no approval.
//
// The DB read is integration-smoke-covered (scripts/smoke-operating-model.ts);
// the pure row→shape projection is unit-tested via `projectValidationOverview`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	type LifecycleArtifactRow,
	readLifecycleArtifactRows,
	readOperatingModelHead,
} from "../db/metadata/lifecycle-artifacts";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import { currentValidationResults } from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";
import { DEFAULT_TOLERANCE, type Verdict } from "./validation-verdict";
import type { ValidationParams } from "./validation-verdict-runner";

// Re-exported for the projection's callers/tests — the row shape now lives in
// the shared lifecycle-artifacts substrate (one definition across families).
export type { LifecycleArtifactRow } from "../db/metadata/lifecycle-artifacts";

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
	// The exact "table.column" entries the executed check read (DAT-509) — the
	// same set a failed critical fans its column-grain entropy out to, so the
	// agent can name the implicated columns (and drill in via look_table).
	// Empty until executed.
	columns_used: z.array(z.string()),
});
export type ValidationOverview = z.infer<typeof ValidationOverview>;

const LookValidationResult = z.object({
	// False when the workspace has no promoted operating_model run yet — the
	// widget should say "not run" rather than imply zero declared validations.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	validations: z.array(ValidationOverview),
});
export type LookValidationResult = z.infer<typeof LookValidationResult>;

/**
 * Narrow the `columns_used` JSON column to its engine contract: a list of
 * LLM-declared "table.column" strings, possibly carrying the physical
 * `src_<digest>__` prefix — digest-stripped like every engine string surfaced
 * to the agent. Anything off-contract degrades to empty, never throws.
 */
export function columnsUsedStrings(value: unknown): string[] {
	if (!Array.isArray(value)) return [];
	return value
		.filter((entry): entry is string => typeof entry === "string")
		.map((entry) => stripSrcDigests(entry));
}

/** One current_validation_results row — a pure SQL store (docs/architecture/grounding.md). */
export interface ValidationResultRow {
	sqlUsed: string | null;
	// JSON column — unknown at the boundary, narrowed in the projector.
	columnsUsed: unknown;
}

/**
 * Project one lifecycle artifact (+ its on-demand verdict + declared params) to
 * the tool's shape. Pure (no DB) so the join + sanitization is unit-testable.
 * The verdict is NOT read from a stored column (docs/architecture/grounding.md): it is recomputed by
 * re-running `sql_used` (see `runValidationVerdicts`); `severity` is the declared
 * spec param. `state_reason` / `message` are engine-built free text that can
 * embed raw `src_<digest>__` physical names — digest-backstopped here.
 */
export function projectValidationOverview(
	artifact: LifecycleArtifactRow,
	result: ValidationResultRow | undefined,
	verdict: Verdict | undefined,
	params: ValidationParams | undefined,
): ValidationOverview {
	return {
		validation_id: artifact.artifactKey,
		state: artifact.state ?? "",
		state_reason:
			artifact.stateReason === null
				? null
				: stripSrcDigests(artifact.stateReason),
		severity: params?.severity ?? null,
		status: verdict?.status ?? null,
		passed: verdict?.passed ?? null,
		message: verdict?.message == null ? null : stripSrcDigests(verdict.message),
		columns_used: columnsUsedStrings(result?.columnsUsed),
	};
}

/** Per-validation lifecycle + result for the workspace's promoted operating_model run. */
export async function lookValidation(): Promise<LookValidationResult> {
	// `analyzed` = the workspace PROMOTED an operating_model run — distinct from
	// "promoted but zero declared validations" (a vertical with none), which must
	// not read as never-ran. The head pass-through stays on the read surface for
	// exactly this check; the rows themselves come from the current_* views.
	// Resolved at the workspace catalog head (DAT-506), so it carries no session.
	const head = await readOperatingModelHead();
	if (!head) {
		return {
			analyzed: false,
			pending_teaches: 0,
			validations: [],
		};
	}

	// The current_* views ARE the promoted run (docs/architecture/persistence.md, DAT-453): the head join
	// lives in the database. The shared reader scopes to validation artifacts —
	// the lifecycle substrate is typed and shared with cycles/metrics.
	const artifacts: LifecycleArtifactRow[] =
		await readLifecycleArtifactRows("validation");

	const rawResults = await metadataDb
		.select({
			validationId: currentValidationResults.validationId,
			sqlUsed: currentValidationResults.sqlUsed,
			columnsUsed: currentValidationResults.columnsUsed,
		})
		.from(currentValidationResults);
	const resultByKey = new Map<string, ValidationResultRow>(
		rawResults.map((r) => [
			r.validationId ?? "",
			{ sqlUsed: r.sqlUsed, columnsUsed: r.columnsUsed },
		]),
	);

	// The verdict is computed ON DEMAND (docs/architecture/grounding.md): re-run each grounded
	// `sql_used` on the lake and judge it with the declared tolerance from the
	// vertical's specs (the engine stores neither the verdict nor the params).
	// The runner is server-only (lake/node bindings) — lazy-imported so this
	// module's graph stays node-free for any client that imports its types.
	const { resolveActiveWorkspaceRow } = await import("../db/cockpit/registry");
	const { vertical } = await resolveActiveWorkspaceRow();
	const { loadValidationParams, runValidationVerdicts } = await import(
		"./validation-verdict-runner"
	);
	const params = await loadValidationParams(vertical);
	const verdicts = await runValidationVerdicts(
		[...resultByKey.entries()].map(([validationId, row]) => ({
			validationId,
			sqlUsed: row.sqlUsed,
			tolerance: params.get(validationId)?.tolerance ?? DEFAULT_TOLERANCE,
		})),
	);

	const validations = artifacts.map((a) =>
		projectValidationOverview(
			a,
			resultByKey.get(a.artifactKey),
			verdicts.get(a.artifactKey),
			params.get(a.artifactKey),
		),
	);

	const pending = await getPendingOverlays();

	return {
		analyzed: true,
		pending_teaches: pending.length,
		validations,
	};
}

export const lookValidationTool = toolDefinition({
	name: "look_validation",
	description:
		"Show the workspace's operating-model validation outcomes — every declared " +
		"validation with its lifecycle state (declared / grounded / executed), " +
		"the reason it could not run when it stopped short, and the executed " +
		"result (pass / fail + message). Read-only; reflects the promoted " +
		"operating_model run (run the operating_model tool first). pending_teaches " +
		"counts un-applied teaches across the workspace. Use `why_validation` to " +
		"drill into a specific validation.",
	inputSchema: z.object({}),
	outputSchema: LookValidationResult,
}).server(() => lookValidation());
