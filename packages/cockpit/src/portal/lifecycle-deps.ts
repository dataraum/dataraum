// Real lifecycle adapters (DAT-820) — the Bun-only assembly behind
// lifecycle.ts's deps seam. SERVER-ONLY, portal role + the
// scripts/provision-workspace.ts trigger; DAT-821's portal server fns call
// `runLifecycle` too. Kept out of lifecycle.ts so the orchestration logic
// stays Node-importable for unit tests (vitest cannot import `bun`).

import "@tanstack/react-start/server-only";

import { SQL } from "bun";
import { eq } from "drizzle-orm";
import { baseConfig } from "../config.base";
import { cockpitDb } from "../db/cockpit/client";
import { memberships, workspaces } from "../db/cockpit/schema";
import { addWorkspaceRoute, removeWorkspaceRoute } from "./caddy";
import { ComposeDriver } from "./compose-driver";
import type {
	AdminSql,
	LifecycleDeps,
	LifecycleState,
	WorkspaceRegistryOps,
	WorkspaceRow,
} from "./lifecycle";
import { provisionerConfig } from "./provisioner-config";

// ── Registry ops (drizzle over shared cockpit_db) ───────────────────────────

const registry: WorkspaceRegistryOps = {
	async get(workspaceId): Promise<WorkspaceRow | null> {
		const [row] = await cockpitDb
			.select({
				id: workspaces.id,
				name: workspaces.name,
				vertical: workspaces.vertical,
				state: workspaces.state,
				subdomain: workspaces.subdomain,
				readerRole: workspaces.readerRole,
				writerRole: workspaces.writerRole,
				catalogSchema: workspaces.catalogSchema,
			})
			.from(workspaces)
			.where(eq(workspaces.id, workspaceId))
			.limit(1);
		return row ? { ...row, state: row.state as LifecycleState } : null;
	},

	async upsertCreating(row): Promise<void> {
		try {
			await cockpitDb
				.insert(workspaces)
				.values({ ...row, state: "creating" })
				.onConflictDoUpdate({
					target: workspaces.id,
					set: {
						name: row.name,
						vertical: row.vertical,
						subdomain: row.subdomain,
						readerRole: row.readerRole,
						writerRole: row.writerRole,
						catalogSchema: row.catalogSchema,
						state: "creating",
					},
				});
		} catch (err) {
			// The partial unique index (workspaces_subdomain_live_uq): another
			// LIVE workspace already claims the label. Drizzle wraps Bun's
			// PostgresError (which carries `constraint`) in a DrizzleQueryError
			// whose own message is just the params — walk the cause chain.
			let detail = "";
			for (
				let cursor: unknown = err;
				cursor instanceof Error;
				cursor = cursor.cause
			) {
				detail += ` ${cursor.message} ${
					(cursor as { constraint?: string }).constraint ?? ""
				}`;
			}
			if (detail.includes("workspaces_subdomain_live_uq")) {
				throw new Error(
					`subdomain '${row.subdomain}' is already claimed by a live ` +
						"workspace — pick another label",
					{ cause: err },
				);
			}
			throw err;
		}
	},

	async addMembers(workspaceId, userIds): Promise<void> {
		if (userIds.length === 0) {
			return;
		}
		await cockpitDb
			.insert(memberships)
			.values(
				userIds.map((userId) => ({ userId, workspaceId, role: "member" })),
			)
			.onConflictDoNothing({
				target: [memberships.userId, memberships.workspaceId],
			});
	},

	async setState(workspaceId, state): Promise<void> {
		await cockpitDb
			.update(workspaces)
			.set({ state })
			.where(eq(workspaces.id, workspaceId));
	},
};

// ── Admin SQL sessions ──────────────────────────────────────────────────────

/** One-connection session (`max: 1`) so an op's statements — including
 * `SET lock_timeout` — share a session, per the AdminSql contract. */
function adminSession(url: string): {
	sql: AdminSql;
	close: () => Promise<void>;
} {
	const session = new SQL(url, { max: 1 });
	return {
		sql: {
			async run(statement) {
				return (await session.unsafe(statement)) as Record<string, unknown>[];
			},
		},
		close: () => session.close(),
	};
}

// ── S3 prefix sweep ─────────────────────────────────────────────────────────

/** Delete EVERYTHING under `prefix` (lake parquet + uploads + directory
 * markers), paginating like upload/s3-upload.ts but without its marker
 * filter — the sweep wants the markers gone too. */
async function deletePrefix(prefix: string): Promise<void> {
	const cfg = provisionerConfig();
	const s3 = new Bun.S3Client({
		accessKeyId: cfg.s3AccessKeyId,
		secretAccessKey: cfg.s3SecretAccessKey,
		region: cfg.s3Region,
		endpoint: `${cfg.s3UseSsl ? "https" : "http"}://${cfg.s3Endpoint}`,
	});
	const bucket = cfg.s3Bucket;
	let continuationToken: string | undefined;
	do {
		const res = await s3.list(
			{ prefix, maxKeys: 1000, continuationToken },
			{ bucket },
		);
		const keys = (res.contents ?? []).map((obj) => obj.key);
		// Bounded fan-out: dev-scale prefixes are thousands of objects at most.
		for (let i = 0; i < keys.length; i += 50) {
			await Promise.all(
				keys.slice(i, i + 50).map((key) => s3.delete(key, { bucket })),
			);
		}
		continuationToken = res.isTruncated ? res.nextContinuationToken : undefined;
	} while (continuationToken);
}

// ── Per-workspace advisory lock (cockpit_db) ────────────────────────────────

/** Exported for the lock-contention integration test — production callers go
 * through `runLifecycle`, which wires this in as the deps seam. */
export async function withWorkspaceLock<T>(
	workspaceId: string,
	fn: () => Promise<T>,
): Promise<T> {
	// A dedicated single connection: session-level advisory locks live and die
	// with the connection, so a crash mid-op releases the lock automatically.
	const lock = new SQL(baseConfig.cockpitDatabaseUrl, { max: 1 });
	const key = `dataraum-provision:${workspaceId}`;
	try {
		const [row] = await lock`
			SELECT pg_try_advisory_lock(hashtextextended(${key}, 0)) AS locked
		`;
		if (!row?.locked) {
			throw new Error(
				`a lifecycle operation for workspace ${workspaceId} is already in ` +
					"flight — wait for it (or its crash) and re-run",
			);
		}
		return await fn();
	} finally {
		await lock.close();
	}
}

// ── Assembly ────────────────────────────────────────────────────────────────

/**
 * Build the real deps, run one lifecycle operation, and dispose the admin
 * sessions. The single entry point for the trigger script today and the
 * DAT-821 portal server fns tomorrow.
 */
export async function runLifecycle<T>(
	op: (deps: LifecycleDeps) => Promise<T>,
): Promise<T> {
	const cfg = provisionerConfig();
	const primary = adminSession(cfg.adminDatabaseUrl);
	const catalog = adminSession(cfg.catalogDatabaseUrl);
	try {
		return await op({
			registry,
			primaryDb: primary.sql,
			catalogDb: catalog.sql,
			driver: new ComposeDriver({
				socketPath: cfg.dockerSocketPath,
				project: cfg.composeProject,
				referenceCockpitService: cfg.referenceCockpitService,
				referenceEngineService: cfg.referenceEngineService,
			}),
			caddy: {
				addRoute: (spec) => addWorkspaceRoute(cfg.caddyAdminUrl, spec),
				removeRoute: (workspaceId) =>
					removeWorkspaceRoute(cfg.caddyAdminUrl, workspaceId),
			},
			s3: { deletePrefix },
			withWorkspaceLock,
			// The parent domain the subdomains hang off — the portal origin's
			// hostname (auth.ts derives the cookie domain the same way).
			parentDomain: new URL(baseConfig.portalOrigin).hostname,
		});
	} finally {
		// allSettled, not all: a close() failure must never mask the lifecycle
		// op's own error as the reason the CLI/server fn reports.
		await Promise.allSettled([primary.close(), catalog.close()]);
	}
}
