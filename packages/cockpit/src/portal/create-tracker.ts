// In-process record of workspace-create runs (DAT-821). SERVER-ONLY, portal.
//
// The registry `state` column is the durable cursor (lifecycle.ts); this map
// adds only what the registry deliberately does not persist: whether THIS
// portal process still has the op in flight, who started it (the pre-row
// authz window in create.functions.ts), and the failure message when it died
// here. A portal restart empties the map — the progress UI then sees a bare
// `creating` row and offers the same-id retry that createWorkspace's
// convergence contract exists for. Success DELETES the entry: `ready` in the
// registry says everything. Failed entries stay so the error survives poll
// cycles; they are overwritten by the retry and bounded by the handful of
// workspaces a human abandons mid-create.

import "@tanstack/react-start/server-only";

export interface CreateRun {
	/** The session user who triggered this run — authorizes progress polls that
	 * arrive before the registry row (and its membership) exists. */
	userId: string;
	status: "running" | "failed";
	/** The lifecycle failure, verbatim — lifecycle errors are written for the
	 * operator (subdomain claims, readiness timeouts with the resume hint). */
	error?: string;
}

const runs = new Map<string, CreateRun>();

/** Record a fired create op. Attaches the terminal handlers, so the
 * fire-and-forget promise in the server fn never surfaces as an unhandled
 * rejection. The entry's OBJECT IDENTITY is the generation token: two
 * near-simultaneous starts for one id (double-submit racing past the
 * "already running" guard's awaits) both call this, and only the LATEST
 * entry's handlers may touch the map — the loser's near-instant
 * advisory-lock rejection must not overwrite the live run's record with a
 * misleading failure. */
export function trackCreateRun(
	workspaceId: string,
	userId: string,
	op: Promise<unknown>,
): void {
	const entry: CreateRun = { userId, status: "running" };
	runs.set(workspaceId, entry);
	op.then(
		() => {
			if (runs.get(workspaceId) === entry) {
				runs.delete(workspaceId);
			}
		},
		(err: unknown) => {
			if (runs.get(workspaceId) === entry) {
				runs.set(workspaceId, {
					userId,
					status: "failed",
					error: err instanceof Error ? err.message : String(err),
				});
			}
		},
	);
}

export function createRunFor(workspaceId: string): CreateRun | null {
	return runs.get(workspaceId) ?? null;
}
