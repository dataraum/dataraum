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
// Seeding is idempotent and memoized ONCE per process; it runs at BOOT (the
// registry-seed Nitro plugin — the membership gate fronts every request, so
// the seed that creates the dev login user cannot wait for a request to
// arrive) and again lazily on first resolve as the retry fallback (the memo
// resets on failure, so a boot-time Postgres hiccup self-heals). The seed
// also runs on an installation whose workspace row predates it — a
// cold-path-only seed would skip a warm registry forever. Identity itself is
// better-auth's (DAT-819): the seed only provisions the DEV credential user
// when env asks for one; production users arrive through the portal's
// sign-up.

import { hashPassword } from "better-auth/crypto";
import { and, eq } from "drizzle-orm";
import { config } from "../../config";
import { baseConfig } from "../../config.base";
import { cockpitDb } from "./client";
import { accounts, memberships, users, workspaces } from "./schema";

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

/** The deterministic row ids of the env-seeded DEV credential user (DAT-819).
 * Exported for tests/smokes only — production users are better-auth-minted
 * with generated ids. */
export const DEV_USER_ID = "dev-user";

/**
 * Idempotently seed the env-designated workspace row (live: `state = 'ready'`,
 * `subdomain` from DATARAUM_WORKSPACE_SUBDOMAIN so the portal can route here)
 * and — when the dev credentials are configured (compose/host dev) — the dev
 * user with its membership in this workspace (DAT-819).
 *
 * The dev user is written DIRECTLY (users + credential-account rows, the exact
 * shape better-auth's own sign-up writes: providerId `credential`, accountId =
 * user id, scrypt hash) rather than through `auth.api.signUpEmail`: the seed
 * runs lazily inside whatever request first resolves the registry, and the
 * sign-up endpoint would issue a session + set the dev user's cookie on THAT
 * unrelated response. The env password is re-asserted on every seed (mirrors
 * the engine re-setting the reader role's password each boot) so a changed
 * DATARAUM_DEV_USER_PASSWORD takes effect without wiping the volume.
 */
async function seedRegistry(workspaceId: string): Promise<void> {
	const subdomain = config.dataraumWorkspaceSubdomain;
	const workspaceRow = cockpitDb.insert(workspaces).values({
		id: workspaceId,
		name: `Workspace ${workspaceId}`,
		vertical: DEFAULT_VERTICAL,
		state: "ready" satisfies WorkspaceState,
		...(subdomain ? { subdomain } : {}),
	});
	if (subdomain) {
		// The env-declared subdomain is truth for an env-seeded workspace (a
		// pre-DAT-819 row carries NULL and would otherwise stay unroutable
		// forever); everything else on a warm row stands.
		await workspaceRow.onConflictDoUpdate({
			target: workspaces.id,
			set: { subdomain },
		});
	} else {
		await workspaceRow.onConflictDoNothing({ target: workspaces.id });
	}

	const { devUserEmail, devUserPassword } = baseConfig;
	if (!devUserEmail || !devUserPassword) {
		return;
	}
	await cockpitDb
		.insert(users)
		.values({
			id: DEV_USER_ID,
			name: "Dev user",
			email: devUserEmail,
			emailVerified: true,
		})
		// No target: a manual sign-up may already own the EMAIL under a
		// better-auth-generated id — either conflict (id or email) means the
		// user exists and the row stands.
		.onConflictDoNothing();
	// Resolve the actual id for the dev email (ours, or the manual sign-up's).
	const [devUser] = await cockpitDb
		.select({ id: users.id })
		.from(users)
		.where(eq(users.email, devUserEmail))
		.limit(1);
	if (!devUser) {
		throw new Error(
			`[cockpit] dev user ${devUserEmail} missing after the idempotent seed`,
		);
	}
	const passwordHash = await hashPassword(devUserPassword);
	const [credentialAccount] = await cockpitDb
		.select({ id: accounts.id })
		.from(accounts)
		.where(
			and(
				eq(accounts.userId, devUser.id),
				eq(accounts.providerId, "credential"),
			),
		)
		.limit(1);
	if (credentialAccount) {
		await cockpitDb
			.update(accounts)
			.set({ password: passwordHash })
			.where(eq(accounts.id, credentialAccount.id));
	} else {
		await cockpitDb.insert(accounts).values({
			id: `${devUser.id}-credential`,
			accountId: devUser.id,
			providerId: "credential",
			userId: devUser.id,
			password: passwordHash,
		});
	}
	await cockpitDb
		.insert(memberships)
		.values({
			userId: devUser.id,
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
 * The active workspace ROW, read from the registry (seeding the workspace row
 * — plus the dev user + membership when configured — once per boot). The
 * single seam the drivers use to route a
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
