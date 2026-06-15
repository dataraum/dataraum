// Workspace registry resolver (DAT-461) — the source of truth for "which
// workspace", replacing scattered `config.dataraumWorkspaceId` env reads.
//
// Phase 1 is single-active-workspace: the registry holds ONE row, seeded from
// `DATARAUM_WORKSPACE_ID`, and `resolveActiveWorkspace()` returns it. The value
// is still the env-designated workspace — what changes is that the cockpit_db
// `workspaces` table is now the system of record (its row backs the FK on
// `sessions.workspaceId`, and reads go through here). Per-request workspace
// SELECTION (a switcher) is Phase 3 (DAT-357); tools have no per-request context
// channel today (TanStack AI `.server((input)=>…)` passes only input), so a
// single resolver is the correct seam now.
//
// Seeding is LAZY here rather than a boot step: the compose `cockpit-migrate`
// init service applies the SCHEMA only, and host dev (cockpit run outside docker)
// has no init step at all — so the registry self-populates on first resolve,
// idempotently, working identically everywhere.

import { eq } from "drizzle-orm";
import { config } from "../../config";
import { cockpitDb } from "./client";
import { actors, workspaces } from "./schema";

// The single coarse actor (DAT-460): no auth, no multi-user yet. `sessions`
// stamp `createdBy` with this so attribution exists without engine-side actor_id
// plumbing (the retired DAT-365). Real actors/auth are Phase 3 (DAT-357).
export const DEFAULT_ACTOR_ID = "default";

// The no-vertical placeholder a cold-start workspace is seeded with (DAT-505) —
// mirrors the engine's `_adhoc` and the schema column default. Vertical is a
// WORKSPACE property; the per-add_source channel retires in DAT-506.
const DEFAULT_VERTICAL = "_adhoc";

/** The engine's `ws_<id>` Postgres schema for a workspace id — mirrors the
 * metadata write-surface's derivation (underscores, not dashes). */
function engineSchemaFor(workspaceId: string): string {
	return `ws_${workspaceId.replaceAll("-", "_")}`;
}

/**
 * The engine's Temporal task queue for a workspace id (DAT-505) — one queue per
 * workspace, `engine-<workspace_id>`. Mirrors the engine's `task_queue_for`
 * (server/workspace.py): the worker polls this exact queue and asserts the match
 * at boot, so the cockpit drivers MUST route `workflow.start` here (not the bare
 * `config.temporalTaskQueue`) for the run to land on the right worker. The id is
 * kept verbatim (raw UUID with dashes), matching the workflow-ID convention.
 */
export function engineTaskQueueFor(workspaceId: string): string {
	return `engine-${workspaceId}`;
}

/** The active workspace as the drivers need it (DAT-505): the id, the engine
 * task queue to route workflows to, and the frame vertical. Read from the
 * registry — the source of truth — never re-derived from the env var. */
export interface ActiveWorkspace {
	id: string;
	taskQueue: string;
	vertical: string;
}

/** Idempotently seed the default actor + the env-designated workspace row.
 * Runs only on the cold path (first resolve after a fresh boot). */
async function ensureRegistry(workspaceId: string): Promise<void> {
	await cockpitDb
		.insert(actors)
		.values({ id: DEFAULT_ACTOR_ID, displayName: "Default user" })
		.onConflictDoNothing({ target: actors.id });
	await cockpitDb
		.insert(workspaces)
		.values({
			id: workspaceId,
			name: `Workspace ${workspaceId}`,
			engineSchema: engineSchemaFor(workspaceId),
			vertical: DEFAULT_VERTICAL,
		})
		.onConflictDoNothing({ target: workspaces.id });
}

/**
 * The active workspace ROW, read from the registry (seeding it on the cold path).
 * The single seam the drivers use to route a workflow: it carries the per-workspace
 * task queue (`engine-<id>`) and the frame `vertical` alongside the id, so nothing
 * re-derives routing from the bare env var. Proven to exist as a `workspaces` row,
 * so `sessions.workspaceId` FKs resolve.
 */
export async function resolveActiveWorkspaceRow(): Promise<ActiveWorkspace> {
	const workspaceId = config.dataraumWorkspaceId;
	const [row] = await cockpitDb
		.select({ id: workspaces.id, vertical: workspaces.vertical })
		.from(workspaces)
		.where(eq(workspaces.id, workspaceId))
		.limit(1);
	if (row) {
		return {
			id: row.id,
			taskQueue: engineTaskQueueFor(row.id),
			vertical: row.vertical,
		};
	}
	await ensureRegistry(workspaceId);
	return {
		id: workspaceId,
		taskQueue: engineTaskQueueFor(workspaceId),
		vertical: DEFAULT_VERTICAL,
	};
}

/**
 * The active workspace id, read from the registry (seeding it on the cold path).
 * Returns the `DATARAUM_WORKSPACE_ID` value in Phase 1 — but proven to exist as
 * a `workspaces` row, so `sessions.workspaceId` FKs resolve. Thin wrapper over
 * `resolveActiveWorkspaceRow` for call sites that need only the id.
 */
export async function resolveActiveWorkspace(): Promise<string> {
	return (await resolveActiveWorkspaceRow()).id;
}
