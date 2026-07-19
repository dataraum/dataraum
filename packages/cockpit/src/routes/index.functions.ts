// Server functions for the `/` route (DAT-819) — modal on the image's role.
// Peeled out of the isomorphic route file so server-only reads (registry,
// auth, cockpit_db) never ride into the client bundle; workspace-only modules
// are additionally imported INSIDE handlers because loading them evaluates
// the workspace config, which throws in portal mode.

import { createServerFn } from "@tanstack/react-start";
import { getRequest } from "@tanstack/react-start/server";
import { and, eq } from "drizzle-orm";
import { auth } from "#/auth/auth";
import { workspaceUrlFor } from "#/auth/workspace-url";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { memberships, workspaces } from "#/db/cockpit/schema";

/** One row of the portal's workspace list: a membership joined onto the
 * registry. `url` is null when the workspace has no subdomain yet (bare
 * host-dev seed — reachable only on its direct port, not through Caddy). */
export interface PortalWorkspace {
	id: string;
	name: string;
	url: string | null;
}

export type PortalHome =
	// A per-workspace cockpit: `/` redirects into the workspace UI.
	| { mode: "workspace" }
	// Portal, signed out: render the login screen.
	| { mode: "signin" }
	// Portal, signed in: the user's workspaces from `memberships`.
	| { mode: "list"; email: string; workspaces: PortalWorkspace[] };

// `/` resolves the active workspace and sends the user straight to its
// cockpit (workspace role). The id comes from the cockpit_db workspace
// registry (DAT-461), seeded from DATARAUM_WORKSPACE_ID. Dynamic import: the
// registry evaluates the workspace config, which portal mode must never load.
export const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	async () => {
		const { resolveActiveWorkspace } = await import("#/db/cockpit/registry");
		return resolveActiveWorkspace();
	},
);

/**
 * The portal home state (DAT-819): role, session, and — signed in — the
 * user's workspaces. Lists memberships joined onto the registry, respecting
 * the provisioner lifecycle: only `state = 'ready'` workspaces are offered
 * (a creating/archiving/archived workspace is not a login target).
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
				subdomain: workspaces.subdomain,
			})
			.from(memberships)
			.innerJoin(workspaces, eq(memberships.workspaceId, workspaces.id))
			.where(
				and(
					eq(memberships.userId, session.user.id),
					eq(workspaces.state, "ready"),
				),
			);
		return {
			mode: "list",
			email: session.user.email,
			workspaces: rows.map((row) => ({
				id: row.id,
				name: row.name,
				url: row.subdomain
					? workspaceUrlFor(row.subdomain, baseConfig.portalOrigin)
					: null,
			})),
		};
	},
);
