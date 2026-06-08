// The uniform agent-actionable error envelope (tool-API consistency pass 2).
//
// Three things could go wrong in a tool; they map to TWO signals, not three:
//   1. "Looked, didn't find / partial" — a legitimate RESULT, carried by a
//      discriminant on the success shape (the `why_*` `found: boolean` pattern,
//      or an empty array from `list_*`). NOT an error.
//   2. "Couldn't act, but you can fix it" — an AGENT-ACTIONABLE failure (a bad
//      query, a stale id, an unmet precondition). Returned as `{ error }` so the
//      model reads the message and retries IN-LOOP instead of the turn dying on
//      an opaque "Error executing tool: …" string. This is the `teach` precedent,
//      generalized.
//   3. Infra (DB down, engine unreachable) — NOT the agent's to fix. Still throws.
//
// Read/query tools whose failures are dominantly agent-fixable (run_sql, probe —
// bad SQL) wrap their call in `asAgentError`: a thrown driver error becomes
// `{ error }` rather than killing the turn. Write/compute tools that need to
// separate actionable from infra throw `AgentActionableError` for the former and
// let infra propagate; `catchActionable` converts only the former.

import { z } from "zod";

/** The agent-actionable error branch. Unioned onto a tool's success schema via
 * `withAgentError` so the model sees `{ error }` as a valid (recoverable) output. */
export const AgentErrorSchema = z.object({
	error: z
		.string()
		.describe(
			"The call could not complete, but the cause is yours to fix — correct " +
				"the query, the id, or an unmet precondition and retry. Surfaced as " +
				"data (not a thrown error) so the turn continues.",
		),
});
export type AgentError = z.infer<typeof AgentErrorSchema>;

/** Narrow an unknown tool output to the error branch. */
export function isAgentError(value: unknown): value is AgentError {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof (value as { error?: unknown }).error === "string"
	);
}

/** Wrap a success schema with the agent-error branch: `Success | { error }`. */
export function withAgentError<T extends z.ZodTypeAny>(success: T) {
	return z.union([success, AgentErrorSchema]);
}

/** An agent-fixable failure raised from deep in a tool's logic. Write/compute
 * tools throw this for actionable failures; `catchActionable` turns it into the
 * `{ error }` envelope while letting infra errors propagate (throw). */
export class AgentActionableError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AgentActionableError";
	}
}

/** Run a fn, converting ANY throw into `{ error }`. For read/query tools whose
 * failures are dominantly agent-fixable (bad SQL) — a degraded infra error
 * returned as data still beats an opaque turn-killing string. */
export async function asAgentError<T>(
	fn: () => Promise<T>,
): Promise<T | AgentError> {
	try {
		return await fn();
	} catch (err) {
		return { error: err instanceof Error ? err.message : String(err) };
	}
}

/** Run a fn, converting ONLY `AgentActionableError` into `{ error }`; everything
 * else (infra) propagates. For write/compute tools that must keep that line. */
export async function catchActionable<T>(
	fn: () => Promise<T>,
): Promise<T | AgentError> {
	try {
		return await fn();
	} catch (err) {
		if (err instanceof AgentActionableError) return { error: err.message };
		throw err;
	}
}
