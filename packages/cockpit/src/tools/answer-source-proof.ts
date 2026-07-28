// The parts-at-source PROOF (DAT-678) — the one step that turns the answer
// sub-agent's declaration into something the UI is allowed to act on.
//
// Split from `duckdb/answer-source.ts` (which stays pure and connection-free)
// because this is the piece that owns a lake connection: the composer, the
// narrowing and the proof STATEMENT are all pure and testable without a
// database; only the execution is not.

import {
	type AnswerDrillSource,
	answerSourceScalarSql,
	runAnswerSourceProof,
} from "#/duckdb/answer-source";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";

/**
 * Prove a candidate declaration against the answer's own validated statement.
 * Returns the handle only when the recomposed scalar reproduces the answer's
 * non-NULL value; otherwise null, and null simply means the answer surface
 * drills tier A instead.
 *
 * Best-effort BY CONSTRUCTION: this decides whether an extra affordance
 * appears, so no path through it may fail the answer. Every branch — an
 * uncomposable candidate, a bind error, a disagreement, an unavailable lake —
 * returns null.
 */
export async function proveAnswerSource(
	candidate: AnswerDrillSource | null,
	answerSql: string,
): Promise<AnswerDrillSource | null> {
	if (!candidate) return null;
	const scalarSql = answerSourceScalarSql(candidate);
	if (scalarSql === null) return null;
	try {
		return await withLakeConnection(async (conn) => {
			// Engine scope, matching every other drill path. Declared relations are
			// ALWAYS bare by the time they reach here — `bareRelationName` reduced
			// the model's `lake.<layer>.<name>` at the narrow, because mosaic-sql
			// would quote a qualified string as one identifier. `USE lake.typed` is
			// what makes the bare name resolve.
			await applyEngineScope(conn);
			return (await runAnswerSourceProof(conn, scalarSql, answerSql))
				? candidate
				: null;
		});
	} catch (err) {
		console.warn(`[cockpit] answer source proof unavailable: ${err}`);
		return null;
	}
}
