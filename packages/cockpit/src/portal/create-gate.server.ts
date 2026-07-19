// The DAT-821 authz gate for the portal's lifecycle server fns. SERVER-ONLY
// (the `.server.ts` split keeps these plain exports out of the isomorphic
// route graph — the import-protection build check rejects non-serverFn
// exports that reach `@tanstack/react-start/server`).
//
// v1 policy (decided on DAT-821): the portal role only, an authenticated
// session required, and ANY signed-in user of the installation may create —
// one installation = one tenant, every account arrived through this portal's
// sign-up, and finer roles are explicitly post-v1 (`MembershipRole` is
// `member`-only). The creator is the membership the new workspace gets; the
// client can never attach other users.

import "@tanstack/react-start/server-only";

import { getRequest } from "@tanstack/react-start/server";
import { and, eq } from "drizzle-orm";
import { auth } from "#/auth/auth";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { memberships } from "#/db/cockpit/schema";
import { createRunFor } from "#/portal/create-tracker";

/** Portal-role + session gate. Thrown Responses pass through the server-fn
 * handler verbatim (status-correct rejections for direct RPC callers; the
 * route's beforeLoad handles the human redirect). */
export async function requirePortalSession() {
	if (!baseConfig.portalMode) {
		// A workspace cockpit must never expose provisioning — its container
		// deliberately lacks the admin env, and the surface belongs to the portal.
		throw Response.json({ error: "portal_only" }, { status: 403 });
	}
	const session = await auth.api.getSession({ headers: getRequest().headers });
	if (!session) {
		throw Response.json({ error: "unauthenticated" }, { status: 401 });
	}
	return session;
}

/** Progress/retry visibility = membership (the creator's row lands with the
 * registry write) OR having started the tracked run (the pre-row window). */
export async function requireCreateVisibility(
	workspaceId: string,
	userId: string,
): Promise<void> {
	const [membership] = await cockpitDb
		.select({ userId: memberships.userId })
		.from(memberships)
		.where(
			and(
				eq(memberships.userId, userId),
				eq(memberships.workspaceId, workspaceId),
			),
		)
		.limit(1);
	if (!membership && createRunFor(workspaceId)?.userId !== userId) {
		throw Response.json({ error: "not_a_member" }, { status: 403 });
	}
}
