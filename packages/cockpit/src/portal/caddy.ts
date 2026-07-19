// Caddy route management (DAT-819) — THE provisioner seam for DAT-820.
//
// Caddy terminates every `<subdomain>.<parent domain>` of the installation
// (DD/51740673) and exposes its admin API on the compose network; a workspace
// becomes reachable by ADDING one `@id`-tagged route and unreachable by
// REMOVING it — no reload, no config file edit (the dev compose seeds the
// same shape statically in packages/infra/caddy/caddy.json). The provisioner
// (Phase 7) calls these two functions at create/archive; nothing else may
// talk to the Caddy admin API.
//
// Pure by design: explicit admin URL + injectable fetch, no config import —
// callable from any role and trivially unit-tested. Both operations are
// IDEMPOTENT (create/archive retries must converge): add replaces an existing
// route with the same id; remove treats an unknown id as already-removed.

/** Where a workspace's route lives in Caddy's config tree. `server` matches
 * the server key in caddy.json — one HTTP server for the installation. */
const DEFAULT_SERVER = "srv0";

export interface WorkspaceRouteSpec {
	workspaceId: string;
	/** The registry `subdomain` label, e.g. `ws1`. */
	subdomain: string;
	/** The installation's parent domain, e.g. `dataraum.localhost` (the
	 * portal origin's hostname — auth.ts `parentDomain`). */
	parentDomain: string;
	/** The workspace cockpit's dial address on the compose network,
	 * e.g. `cockpit-2:3000`. */
	upstream: string;
}

/** The `@id` tag a workspace's route carries — the stable admin-API handle
 * (`DELETE /id/ws-<workspaceId>`), derived from the workspace id only so
 * archive needs no route lookup. */
export function workspaceRouteId(workspaceId: string): string {
	return `ws-${workspaceId}`;
}

/** The Caddy route object for a workspace — host-matched reverse proxy,
 * terminal (no fall-through to the portal catch-all). */
export function workspaceRoute(spec: WorkspaceRouteSpec): object {
	return {
		"@id": workspaceRouteId(spec.workspaceId),
		match: [{ host: [`${spec.subdomain}.${spec.parentDomain}`] }],
		handle: [
			{
				handler: "reverse_proxy",
				upstreams: [{ dial: spec.upstream }],
			},
		],
		terminal: true,
	};
}

async function routeExists(
	adminUrl: string,
	id: string,
	fetchImpl: typeof fetch,
): Promise<boolean> {
	const res = await fetchImpl(`${adminUrl}/id/${id}`);
	if (res.ok) {
		return true;
	}
	await res.body?.cancel();
	return false;
}

/**
 * Make `<subdomain>.<parentDomain>` reach `upstream`: append the workspace's
 * `@id`-tagged route to the server's route array, replacing a pre-existing
 * route with the same id (idempotent re-provision).
 */
export async function addWorkspaceRoute(
	adminUrl: string,
	spec: WorkspaceRouteSpec,
	fetchImpl: typeof fetch = fetch,
	server: string = DEFAULT_SERVER,
): Promise<void> {
	const id = workspaceRouteId(spec.workspaceId);
	if (await routeExists(adminUrl, id, fetchImpl)) {
		await removeWorkspaceRoute(adminUrl, spec.workspaceId, fetchImpl);
	}
	const res = await fetchImpl(
		`${adminUrl}/config/apps/http/servers/${server}/routes`,
		{
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(workspaceRoute(spec)),
		},
	);
	if (!res.ok) {
		throw new Error(
			`[caddy] adding route ${id} failed (${res.status}): ${await res.text()}`,
		);
	}
	await res.body?.cancel();
}

/**
 * Make the workspace's subdomain unreachable: delete its route by `@id`.
 * An unknown id is already-removed — success (Caddy reports it as an
 * `unknown object id` error, its only signal for absent).
 */
export async function removeWorkspaceRoute(
	adminUrl: string,
	workspaceId: string,
	fetchImpl: typeof fetch = fetch,
): Promise<void> {
	const id = workspaceRouteId(workspaceId);
	const res = await fetchImpl(`${adminUrl}/id/${id}`, { method: "DELETE" });
	if (res.ok) {
		await res.body?.cancel();
		return;
	}
	const body = await res.text();
	if (body.includes("unknown object id")) {
		return;
	}
	throw new Error(
		`[caddy] removing route ${id} failed (${res.status}): ${body}`,
	);
}
