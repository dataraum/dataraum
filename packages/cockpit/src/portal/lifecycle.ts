// Workspace lifecycle — the DAT-820 provisioner core.
//
// Create and archive are DIRECT durable server-side operations in the portal
// role — deliberately NOT Temporal workflows. ADR-0020 pins workflows to
// Python on an engine worker, and the one worker that could naturally own a
// workspace's lifecycle does not exist until create finishes (hosting it on
// ANOTHER workspace's engine worker would make that workspace a hidden
// control-plane dependency — and its own archive impossible). Durability is
// ADR-0010 discipline transposed to the control plane instead: the registry
// `state` column is the durable cursor, EVERY step is idempotent and
// convergent, and re-running the operation for the same workspace id resumes
// where it died. A cockpit_db advisory lock serializes ops per workspace (the
// serialization `portal/caddy.ts` assumes).
//
// Failure contract: a mid-create crash leaves `state = 'creating'` and only
// resources derivable from the registry row (deterministic names, recorded
// up front) — nothing exists that the registry cannot see, so re-running
// create converges and archive can always sweep a half-workspace.
//
// This module is PURE orchestration over `LifecycleDeps` (Node-importable,
// unit-tested); the real adapters — Bun SQL, Bun.S3Client, the docker
// driver — live in ./lifecycle-deps.ts and are exercised by the lane smoke.

import type { WorkspaceRouteSpec } from "./caddy";
import type { ProvisioningDriver } from "./driver";

/** Mirrors registry.ts `WorkspaceState` (imported nowhere to keep this module
 * free of the workspace-config import chain). */
export type LifecycleState = "creating" | "ready" | "archiving" | "archived";

export interface WorkspaceRow {
	id: string;
	name: string;
	vertical: string;
	state: LifecycleState;
	subdomain: string | null;
	readerRole: string | null;
	writerRole: string | null;
	catalogSchema: string | null;
}

/** Registry writes the lifecycle needs — implemented over drizzle/cockpit_db
 * in lifecycle-deps.ts. All idempotent. */
export interface WorkspaceRegistryOps {
	get(workspaceId: string): Promise<WorkspaceRow | null>;
	/** Insert-or-update the row in `creating` with the full resource record.
	 * Must reject a subdomain already claimed by another live workspace
	 * (the partial unique index) with an error naming the subdomain. */
	upsertCreating(row: {
		id: string;
		name: string;
		vertical: string;
		subdomain: string;
		readerRole: string;
		writerRole: string;
		catalogSchema: string;
	}): Promise<void>;
	/** Grant memberships (idempotent; unknown user ids fail loud via FK). */
	addMembers(workspaceId: string, userIds: string[]): Promise<void>;
	setState(workspaceId: string, state: LifecycleState): Promise<void>;
}

/** One admin SQL session per database — statements of one lifecycle op run
 * in order on the SAME session (so `SET lock_timeout` scopes as written). */
export interface AdminSql {
	run(statement: string): Promise<Record<string, unknown>[]>;
}

export interface LifecycleDeps {
	registry: WorkspaceRegistryOps;
	/** Primary (engine-metadata) database, admin session. */
	primaryDb: AdminSql;
	/** Installation DuckLake catalog database, admin session. */
	catalogDb: AdminSql;
	driver: ProvisioningDriver;
	caddy: {
		addRoute(spec: WorkspaceRouteSpec): Promise<void>;
		removeRoute(workspaceId: string): Promise<void>;
	};
	s3: {
		/** Delete every object under `prefix` in the lake bucket. */
		deletePrefix(prefix: string): Promise<void>;
	};
	/** Per-workspace mutual exclusion (cockpit_db advisory lock). Throws when
	 * another lifecycle op for the same workspace is in flight. */
	withWorkspaceLock<T>(workspaceId: string, fn: () => Promise<T>): Promise<T>;
	/** The installation's parent domain (portal origin hostname) — the Caddy
	 * route is `<subdomain>.<parentDomain>`. */
	parentDomain: string;
	/** Readiness-wait tuning (tests inject fast values). */
	readyTimeoutMs?: number;
	readyPollMs?: number;
	sleep?: (ms: number) => Promise<void>;
	/** Secret mint (tests inject a deterministic one). */
	mintSecret?: () => string;
}

export interface CreateWorkspaceInput {
	/** Omit to mint a fresh uuid; PASS THE SAME ID to resume/converge a
	 * half-created workspace. */
	workspaceId?: string;
	name: string;
	/** The workspace's frame ontology (a real vertical, not `_adhoc`-only —
	 * the registry seeding requirement from the DAT-820 smoke). */
	vertical: string;
	subdomain: string;
	/** Users granted membership (must exist in cockpit_db `users`). */
	memberUserIds?: string[];
}

export interface LifecycleResult {
	workspaceId: string;
	state: LifecycleState;
	/** True when the op was a no-op because the workspace was already in the
	 * terminal state (idempotent re-run). */
	already?: boolean;
}

// ── Deterministic resource names (mirror engine server/workspace.py +
// storage/read_views.py — the registry RECORDS these; derivation authority
// stays engine-side) ─────────────────────────────────────────────────────────

const SCHEMA_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** `ws_<id-with-dashes-as-underscores>` — the engine's `schema_name_for`. */
export function workspaceSchemaName(workspaceId: string): string {
	const candidate = `ws_${workspaceId.replaceAll("-", "_")}`;
	// The WRITER role (`<schema>_writer`, the longer suffix) must also fit
	// Postgres's 63-char identifier limit — the engine enforces the same at
	// role mint; failing here keeps a too-long id out of the registry.
	if (candidate.length + "_writer".length > 63) {
		throw new Error(
			`workspace id '${workspaceId}' produces identifiers over Postgres's ` +
				"63-char limit — use a shorter id",
		);
	}
	if (!SCHEMA_NAME_PATTERN.test(candidate)) {
		throw new Error(
			`workspace id '${workspaceId}' is not a valid identifier stem — use a ` +
				"UUID or [A-Za-z0-9-]+",
		);
	}
	return candidate;
}

export function readerRoleName(schema: string): string {
	return `${schema}_reader`;
}

export function writerRoleName(schema: string): string {
	return `${schema}_writer`;
}

/** DNS label: lowercase alphanumerics + inner dashes, ≤63 chars. */
const SUBDOMAIN_PATTERN = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;

function validateSubdomain(subdomain: string): void {
	if (subdomain.length > 63 || !SUBDOMAIN_PATTERN.test(subdomain)) {
		throw new Error(
			`subdomain '${subdomain}' is not a valid DNS label (lowercase ` +
				"alphanumerics and inner dashes, max 63 chars)",
		);
	}
}

/** 32 random bytes, hex — the per-workspace role secret. Hex keeps it inert
 * inside SQL literals and connection URLs alike. */
function defaultMintSecret(): string {
	const bytes = new Uint8Array(32);
	crypto.getRandomValues(bytes);
	return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** Single-quoted SQL literal (identifiers are validated, secrets are hex —
 * this is belt for both). */
function sqlLiteral(value: string): string {
	return `'${value.replaceAll("'", "''")}'`;
}

// ── Create ──────────────────────────────────────────────────────────────────

/**
 * Provision a workspace end-to-end: registry row (`creating`) → catalog
 * schema → per-workspace role secrets → engine+cockpit pair (driver) → wait
 * for the engine's boot self-provisioning → Caddy route → `ready`.
 *
 * Every step converges on re-run; the engine/cockpit boot self-provisioning
 * (ws schemas, read views, grants, catalog ATTACH) is REUSED — the
 * provisioner only pre-creates what the registry must be able to account
 * for (catalog schema, login roles) and never duplicates a grant.
 */
export async function createWorkspace(
	input: CreateWorkspaceInput,
	deps: LifecycleDeps,
): Promise<LifecycleResult> {
	const name = input.name.trim();
	const vertical = input.vertical.trim();
	if (!name || !vertical) {
		throw new Error("workspace name and vertical must be non-empty");
	}
	validateSubdomain(input.subdomain);

	const workspaceId = input.workspaceId ?? crypto.randomUUID();
	const schema = workspaceSchemaName(workspaceId);
	const readerRole = readerRoleName(schema);
	const writerRole = writerRoleName(schema);

	return deps.withWorkspaceLock(workspaceId, async () => {
		const existing = await deps.registry.get(workspaceId);
		if (existing) {
			if (existing.state === "ready") {
				return { workspaceId, state: "ready", already: true };
			}
			if (existing.state === "archiving" || existing.state === "archived") {
				throw new Error(
					`workspace ${workspaceId} is ${existing.state} — archived ids ` +
						"are not reused; create with a fresh id",
				);
			}
			// `creating`: resume — fall through, converging the row to the
			// latest input below.
		}

		// 1. The registry row FIRST (ADR-0010: nothing may exist that the
		// registry cannot account for). The full resource record is
		// deterministic, so it is recorded before anything is created.
		await deps.registry.upsertCreating({
			id: workspaceId,
			name,
			vertical,
			subdomain: input.subdomain,
			readerRole,
			writerRole,
			catalogSchema: schema,
		});
		if (input.memberUserIds?.length) {
			await deps.registry.addMembers(workspaceId, input.memberUserIds);
		}

		// 2. Per-workspace DuckLake catalog schema (DAT-815: transactional SQL
		// is the whole allocation; the engine's ATTACH would also create it —
		// pre-creating makes the resource registry-visible even if boot fails).
		await deps.catalogDb.run(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);

		// 3. Mint per-workspace secrets and ensure the two LOGIN roles carry
		// them. Passwords only — search_path + grants are the engine boot's
		// self-provisioning (storage/read_views.py ensure_workspace_roles),
		// which re-asserts these same passwords from the pair's env. Secrets
		// rotate on every attempt; the pair below is (re)created to match.
		const mint = deps.mintSecret ?? defaultMintSecret;
		const readerSecret = mint();
		const writerSecret = mint();
		for (const [role, secret] of [
			[readerRole, readerSecret],
			[writerRole, writerSecret],
		] as const) {
			// Engine _ensure_role's race-safe shape: CREATE unconditionally,
			// swallow duplicate_object, then ALTER (re-run = password rotate).
			await deps.primaryDb.run(
				`DO $dataraum_role$ BEGIN ` +
					`CREATE ROLE ${role} LOGIN PASSWORD ${sqlLiteral(secret)}; ` +
					`EXCEPTION WHEN duplicate_object THEN NULL; ` +
					`END $dataraum_role$;`,
			);
			await deps.primaryDb.run(
				`ALTER ROLE ${role} WITH LOGIN PASSWORD ${sqlLiteral(secret)}`,
			);
		}

		// 4. The compute pair (deployment-specific, behind the driver seam).
		const { cockpitUpstream } = await deps.driver.startPair({
			workspaceId,
			subdomain: input.subdomain,
			readerRole,
			writerRole,
			readerSecret,
			writerSecret,
		});

		// 5. Wait for the pair: cockpit healthy AND the engine bootstrap's
		// read schema present (the engine self-provisions ws schemas, views,
		// grants at boot — its completion is what makes the workspace real).
		await waitForWorkspaceUp(workspaceId, schema, deps);

		// 6. Route, then flip the cursor — a `ready` workspace is reachable.
		await deps.caddy.addRoute({
			workspaceId,
			subdomain: input.subdomain,
			parentDomain: deps.parentDomain,
			upstream: cockpitUpstream,
		});
		await deps.registry.setState(workspaceId, "ready");
		return { workspaceId, state: "ready" };
	});
}

async function waitForWorkspaceUp(
	workspaceId: string,
	schema: string,
	deps: LifecycleDeps,
): Promise<void> {
	const timeoutMs = deps.readyTimeoutMs ?? 300_000;
	const pollMs = deps.readyPollMs ?? 2_000;
	const sleep =
		deps.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
	const deadline = Date.now() + timeoutMs;
	let lastState = "pair not up";
	for (;;) {
		if (await deps.driver.pairReady(workspaceId)) {
			const rows = await deps.primaryDb.run(
				`SELECT 1 AS ok FROM information_schema.schemata ` +
					`WHERE schema_name = ${sqlLiteral(`${schema}_read`)}`,
			);
			if (rows.length > 0) {
				return;
			}
			lastState = "pair up, engine bootstrap incomplete";
		}
		if (Date.now() >= deadline) {
			throw new Error(
				`workspace ${workspaceId} did not come up within ${timeoutMs}ms ` +
					`(${lastState}) — state stays 'creating'; re-running create ` +
					"converges (check the pair's container logs)",
			);
		}
		await sleep(pollMs);
	}
}

// ── Archive ─────────────────────────────────────────────────────────────────

/**
 * The `delete-workspace.sh` sweep as a durable operation (that script is
 * retired by this module): stop/remove the pair → remove the route → drop
 * `ws_<id>` + `ws_<id>_read` → drop the per-workspace roles → drop the
 * catalog schema → delete the S3 prefix → `state = 'archived'`. Control-plane
 * rows (registry, memberships, runs, conversations) REMAIN — `archived` is
 * the terminal record, and the portal's `state = 'ready'` filter hides the
 * workspace everywhere it lists.
 */
export async function archiveWorkspace(
	workspaceId: string,
	deps: LifecycleDeps,
): Promise<LifecycleResult> {
	const schema = workspaceSchemaName(workspaceId);

	return deps.withWorkspaceLock(workspaceId, async () => {
		const existing = await deps.registry.get(workspaceId);
		if (!existing) {
			throw new Error(`workspace ${workspaceId} is not in the registry`);
		}
		if (existing.state === "archived") {
			return { workspaceId, state: "archived", already: true };
		}
		await deps.registry.setState(workspaceId, "archiving");

		// 1. Stop producing/consuming before dropping state.
		await deps.driver.removePair(workspaceId);
		await deps.caddy.removeRoute(workspaceId);

		// 2. Engine Postgres schemas (raw + promoted-read). lock_timeout makes
		// a straggling reader fail the DROP loud after 30s instead of hanging
		// (re-run after stopping whatever holds the lock).
		await deps.primaryDb.run(`SET lock_timeout = '30s'`);
		await deps.primaryDb.run(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
		await deps.primaryDb.run(`DROP SCHEMA IF EXISTS "${schema}_read" CASCADE`);

		// 3. The per-workspace roles (registry-recorded names; derived names
		// are identical — belt for a row minted before roles landed). DROP
		// OWNED first: it revokes every grant + default-privilege the role
		// holds (the roles own no objects), which is what unblocks DROP ROLE.
		for (const role of [
			existing.readerRole ?? readerRoleName(schema),
			existing.writerRole ?? writerRoleName(schema),
		]) {
			await deps.primaryDb.run(
				`DO $dataraum_drop$ BEGIN ` +
					`IF EXISTS (SELECT FROM pg_roles WHERE rolname = ${sqlLiteral(role)}) THEN ` +
					`EXECUTE 'DROP OWNED BY ${role}'; ` +
					`EXECUTE 'DROP ROLE ${role}'; ` +
					`END IF; ` +
					`END $dataraum_drop$;`,
			);
		}

		// 4. The DuckLake catalog schema in the SHARED catalog DB (never the
		// database itself — every workspace lives in it). Same loud-lock
		// posture as the engine schemas.
		await deps.catalogDb.run(`SET lock_timeout = '30s'`);
		await deps.catalogDb.run(
			`DROP SCHEMA IF EXISTS "${existing.catalogSchema ?? schema}" CASCADE`,
		);

		// 5. The workspace's whole object-store prefix (lake + uploads).
		await deps.s3.deletePrefix(`${workspaceId}/`);

		await deps.registry.setState(workspaceId, "archived");
		return { workspaceId, state: "archived" };
	});
}
