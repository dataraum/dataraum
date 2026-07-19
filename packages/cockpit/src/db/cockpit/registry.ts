// Workspace registry resolver (DAT-461, control plane DAT-817) — the source of
// truth for "which workspace", replacing scattered `config.dataraumWorkspaceId`
// env reads.
//
// The cockpit is PER-WORKSPACE (DD/51740673): each container boots with a single
// workspace identity and never resolves the workspace per request. cockpit_db,
// however, is ONE shared database across all cockpit containers of the
// installation — so isolation is by scoped queries: every accessor in this
// directory scopes reads/writes by `bootWorkspaceId()` (the sweep is DAT-817).
//
// Seeding is LAZY here rather than a boot step: the compose `cockpit-migrate`
// init service applies the SCHEMA only, and host dev (cockpit run outside docker)
// has no init step at all — so the registry self-populates on first resolve,
// idempotently, working identically everywhere. The seed runs ONCE per process
// (memoized) so the default user + membership also appear on an installation
// whose workspace row predates them — the old cold-path-only seed would skip a
// warm registry forever.

import { eq } from "drizzle-orm";
import { config } from "../../config";
import { cockpitDb } from "./client";
import { memberships, users, workspaces } from "./schema";

// The single coarse user (was the DAT-460 `actors` placeholder): no auth, no
// multi-user yet. Seeded so the portal's login/membership routing (Phase 6,
// DD/51740673) has a real identity row to start from.
export const DEFAULT_USER_ID = "default";

// The no-vertical placeholder a cold-start workspace is seeded with (DAT-505) —
// mirrors the engine's `_adhoc` and the schema column default. Vertical is a
// WORKSPACE property; the per-add_source channel retires in DAT-506.
const DEFAULT_VERTICAL = "_adhoc";

/** The provisioner lifecycle of a workspace (DAT-817, DD/51740673). The retired
 * `archived_at` timestamp folds into `archiving`/`archived`. */
export type WorkspaceState = "creating" | "ready" | "archiving" | "archived";

/** A user's role in a workspace (DAT-817). `member` is the only role in v1;
 * finer roles are a portal-phase (DAT-819) concern. */
export type MembershipRole = "member";

/**
 * The workspace id this cockpit process SERVES — the boot identity
 * (`DATARAUM_WORKSPACE_ID`), one per container (DD/51740673). THE scoping value
 * of the control plane: every cockpit_db accessor filters its reads and fences
 * its writes on this id (DAT-817), which is what keeps one shared cockpit_db
 * isolated between per-workspace cockpits without per-request resolution.
 */
export function bootWorkspaceId(): string {
	return config.dataraumWorkspaceId;
}

/**
 * Born-loud guard for accessors that take a `workspaceId` parameter (DAT-817):
 * in a per-workspace cockpit the only legal value is the boot workspace, so a
 * mismatch is a programming error (or a mis-routed Temporal activity) — throw,
 * never silently query another workspace's rows.
 */
export function assertBootWorkspace(workspaceId: string): void {
	const boot = bootWorkspaceId();
	if (workspaceId !== boot) {
		throw new Error(
			`[cockpit] cross-workspace query refused: ${workspaceId} is not the boot workspace ${boot}`,
		);
	}
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

/** Idempotently seed the default user, the env-designated workspace row (live:
 * `state = 'ready'`), and the user's membership in it (DAT-817). */
async function seedRegistry(workspaceId: string): Promise<void> {
	await cockpitDb
		.insert(users)
		.values({ id: DEFAULT_USER_ID, displayName: "Default user" })
		.onConflictDoNothing({ target: users.id });
	await cockpitDb
		.insert(workspaces)
		.values({
			id: workspaceId,
			name: `Workspace ${workspaceId}`,
			vertical: DEFAULT_VERTICAL,
			state: "ready" satisfies WorkspaceState,
		})
		.onConflictDoNothing({ target: workspaces.id });
	await cockpitDb
		.insert(memberships)
		.values({
			userId: DEFAULT_USER_ID,
			workspaceId,
			role: "member" satisfies MembershipRole,
		})
		.onConflictDoNothing({
			target: [memberships.userId, memberships.workspaceId],
		});
}

// Once per process: the inserts are idempotent, so memoizing is purely a cost
// cap (three no-op upserts per boot, not per resolve). Reset on failure so a
// transient DB blip retries on the next resolve instead of wedging the seam.
let seeded: Promise<void> | null = null;
function ensureRegistrySeeded(workspaceId: string): Promise<void> {
	seeded ??= seedRegistry(workspaceId).catch((err: unknown) => {
		seeded = null;
		throw err;
	});
	return seeded;
}

/**
 * The active workspace ROW, read from the registry (seeding user + workspace +
 * membership once per boot). The single seam the drivers use to route a
 * workflow: it carries the per-workspace task queue (`engine-<id>`) and the
 * frame `vertical` alongside the id, so nothing re-derives routing from the
 * bare env var. Proven to exist as a `workspaces` row, so `runs.workspaceId`
 * FKs resolve. Born-loud if the row is missing AFTER the idempotent seed —
 * that's a broken database, not a cold start.
 */
export async function resolveActiveWorkspaceRow(): Promise<ActiveWorkspace> {
	const workspaceId = bootWorkspaceId();
	await ensureRegistrySeeded(workspaceId);
	const [row] = await cockpitDb
		.select({ id: workspaces.id, vertical: workspaces.vertical })
		.from(workspaces)
		.where(eq(workspaces.id, workspaceId))
		.limit(1);
	if (!row) {
		throw new Error(
			`[cockpit] workspace ${workspaceId} missing from the registry after seeding`,
		);
	}
	return {
		id: row.id,
		taskQueue: engineTaskQueueFor(row.id),
		vertical: row.vertical,
	};
}

/**
 * The active workspace id, read from the registry (seeding it on the cold path).
 * Returns the `DATARAUM_WORKSPACE_ID` value — but proven to exist as a
 * `workspaces` row, so `runs.workspaceId` FKs resolve. Thin wrapper over
 * `resolveActiveWorkspaceRow` for call sites that need only the id.
 */
export async function resolveActiveWorkspace(): Promise<string> {
	return (await resolveActiveWorkspaceRow()).id;
}

/**
 * Persist the active workspace's declared vertical (DAT-523). The `frame` stage
 * calls this once it resolves a real, named vertical, so the Temporal drivers
 * read it onto the next workflow's `verticals[]` manifest — no hand-seeded
 * registry row. Upserts the workspace row (seeding it if frame somehow precedes
 * the first resolve), so the write is AUTHORITATIVE: the workspace IS this
 * vertical afterwards. Throws on DB failure by design — a framed-but-unpersisted
 * vertical would silently leave the workspace `_adhoc` and fail add_source later
 * with a misleading "run frame first". Never called with `_adhoc` (frame guards
 * the no-vertical default so it can't overwrite a previously-framed workspace).
 * Boot-scoped by construction: the target row is always the boot workspace.
 */
export async function setActiveWorkspaceVertical(
	vertical: string,
): Promise<void> {
	const workspaceId = bootWorkspaceId();
	await cockpitDb
		.insert(workspaces)
		.values({
			id: workspaceId,
			name: `Workspace ${workspaceId}`,
			vertical,
			state: "ready" satisfies WorkspaceState,
		})
		.onConflictDoUpdate({ target: workspaces.id, set: { vertical } });
}
