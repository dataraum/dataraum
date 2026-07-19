// Server functions for the `/` route (DAT-819) — modal on the image's role.
// Peeled out of the isomorphic route file so server-only reads (auth,
// cockpit_db) never ride into the client bundle. Everything here is
// base-config only — safe to evaluate in portal mode, which must never load
// the workspace config.

import { createServerFn } from "@tanstack/react-start";
import { getRequest } from "@tanstack/react-start/server";
import { and, eq, ne } from "drizzle-orm";
import { auth } from "#/auth/auth";
import { workspaceUrlFor } from "#/auth/workspace-url";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import type { WorkspaceState } from "#/db/cockpit/registry";
import { memberships, workspaces } from "#/db/cockpit/schema";

/** One row of the portal's workspace list: a membership joined onto the
 * registry. `url` is null when the workspace is not enterable — mid-lifecycle
 * (`creating`/`archiving`), or `ready` without a subdomain yet (bare host-dev
 * seed — reachable only on its direct port, not through Caddy). */
export interface PortalWorkspace {
	id: string;
	name: string;
	/** Lifecycle state (DAT-821): `ready` is enterable; `creating`/`archiving`
	 * render with their state instead of a link; `archived` never leaves the
	 * server. */
	state: Exclude<WorkspaceState, "archived">;
	url: string | null;
}

export type PortalHome =
	// A per-workspace cockpit: `/` redirects into the workspace UI.
	| { mode: "workspace" }
	// Portal, signed out: render the login screen.
	| { mode: "signin" }
	// Portal, signed in: the user's workspaces from `memberships`.
	| { mode: "list"; email: string; workspaces: PortalWorkspace[] };

/**
 * The portal home state (DAT-819): role, session, and — signed in — the
 * user's workspaces. Lists memberships joined onto the registry, respecting
 * the provisioner lifecycle end-to-end (DAT-821): `ready` workspaces link to
 * their subdomain; `creating`/`archiving` are shown with their state but are
 * not login targets; `archived` is hidden entirely.
 */
export const getPortalHome = createServerFn({ method: "GET" }).handler(
	async (): Promise<PortalHome> => {
		if (!baseConfig.portalMode) {
			return { mode: "workspace" };
		}
		const session = await auth.api.getSession({
			headers: getRequest().headers,
		});
		if (!session) {
			return { mode: "signin" };
		}
		const rows = await cockpitDb
			.select({
				id: workspaces.id,
				name: workspaces.name,
				state: workspaces.state,
				subdomain: workspaces.subdomain,
			})
			.from(memberships)
			.innerJoin(workspaces, eq(memberships.workspaceId, workspaces.id))
			.where(
				and(
					eq(memberships.userId, session.user.id),
					ne(workspaces.state, "archived"),
				),
			);
		return {
			mode: "list",
			email: session.user.email,
			workspaces: rows.map((row) => ({
				id: row.id,
				name: row.name,
				state: row.state as Exclude<WorkspaceState, "archived">,
				url:
					row.state === "ready" && row.subdomain
						? workspaceUrlFor(row.subdomain, baseConfig.portalOrigin)
						: null,
			})),
		};
	},
);
