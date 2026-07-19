// The workspace switcher's data (DAT-821). SERVER FN, workspace role.
//
// A per-workspace cockpit never resolves "which workspace" per request — the
// switcher does not either: it lists the SESSION USER's memberships from the
// shared cockpit_db (joined onto the registry, the multi-workspace source of
// truth) and marks this cockpit's boot workspace as current. Switching IS
// navigation: every other `ready` workspace is an absolute URL to its
// subdomain (DD/51740673); `creating`/`archiving` rows surface with their
// state but no URL; `archived` never leaves the server.
//
// Authz: the global membership gate (start.ts) already vetted this request —
// session + membership of the boot workspace — so the session read here is
// scoping (whose memberships), not the boundary. The portal-mode rejection is
// a fence: the portal serves the same server bundle but has no boot identity.

import { createServerFn } from "@tanstack/react-start";
import { getRequest } from "@tanstack/react-start/server";
import { and, eq, ne } from "drizzle-orm";
import { auth } from "#/auth/auth";
import { workspaceUrlFor } from "#/auth/workspace-url";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { bootWorkspaceId, type WorkspaceState } from "#/db/cockpit/registry";
import { memberships, workspaces } from "#/db/cockpit/schema";

export interface SwitcherWorkspace {
	id: string;
	name: string;
	state: Exclude<WorkspaceState, "archived">;
	/** Absolute subdomain URL — set only for `ready` workspaces with a routed
	 * subdomain (null on the current one too: it needs no link). */
	url: string | null;
	current: boolean;
}

export interface SwitcherData {
	/** The boot workspace's display name — the switcher target's label. */
	currentName: string;
	/** The user's non-archived workspaces, name-sorted. */
	workspaces: SwitcherWorkspace[];
	/** The portal's create flow — the switcher's "New workspace" entry. */
	createUrl: string;
}

export const getSwitcherWorkspaces = createServerFn({ method: "GET" }).handler(
	async (): Promise<SwitcherData> => {
		if (baseConfig.portalMode) {
			throw Response.json({ error: "workspace_only" }, { status: 403 });
		}
		const session = await auth.api.getSession({
			headers: getRequest().headers,
		});
		if (!session) {
			// The membership gate fronts every request; reaching this without a
			// session means the caller bypassed HTML navigation — same status.
			throw Response.json({ error: "unauthenticated" }, { status: 401 });
		}

		const currentId = bootWorkspaceId();
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

		const list: SwitcherWorkspace[] = rows
			.map((row) => ({
				id: row.id,
				name: row.name,
				state: row.state as Exclude<WorkspaceState, "archived">,
				url:
					row.id !== currentId && row.state === "ready" && row.subdomain
						? workspaceUrlFor(row.subdomain, baseConfig.portalOrigin)
						: null,
				current: row.id === currentId,
			}))
			.sort((a, b) => a.name.localeCompare(b.name));

		return {
			// The gate guarantees membership of the boot workspace, so it is in
			// the list; the id is the born-loud fallback for a broken registry.
			currentName:
				list.find((workspace) => workspace.current)?.name ?? currentId,
			workspaces: list,
			createUrl: `${baseConfig.portalOrigin}/create`,
		};
	},
);
