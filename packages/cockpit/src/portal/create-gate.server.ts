// The DAT-821 authz gate for the portal's lifecycle server fns. SERVER-ONLY
// (the `.server.ts` split keeps these plain exports out of the isomorphic
// route graph — the import-protection build check rejects non-serverFn
// exports that reach `@tanstack/react-start/server`).
//
// v1 policy (decided on DAT-821): the portal role only, an authenticated
// session required, and ANY signed-in user of the installation may create —
// one installation = one tenant, and finer roles are explicitly post-v1
// (`MembershipRole` is `member`-only). The creator is the membership the new
// workspace gets; the client can never attach other users.
//
// Read "ANY signed-in user" literally, and note what it composes with:
// better-auth's handler is mounted as a splat over /api/auth/* (routes/api/
// auth/$.ts) and the gate allow-lists that prefix as public (gate.server.ts),
// while auth.ts sets `emailAndPassword.enabled` with no `disableSignUp`. So
// POST /api/auth/sign-up/email is reachable UNAUTHENTICATED today — there is
// no sign-up *UI*, but the endpoint is open. Anyone who can reach the portal
// can therefore mint an account and provision a workspace, which spins
// containers on the host.
//
// That is the current posture, not a claim that it is the intended one. If it
// is not, the fix is `disableSignUp: true` (or an invite flow) in auth.ts —
// NOT a stricter comment here.

import "@tanstack/react-start/server-only";

import { getRequest } from "@tanstack/react-start/server";
import { and, eq } from "drizzle-orm";
import { auth } from "#/auth/auth";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { memberships } from "#/db/cockpit/schema";
import { createRunFor } from "#/portal/create-tracker";
import { serverFnError } from "#/server/server-fn-error";

/** Portal-role + session gate. Rejections go through `serverFnError` — a
 * status-carrying thrown Error that actually REJECTS the client call (a
 * thrown Response would resolve, see server-fn-error.ts); the route's
 * beforeLoad handles the human redirect. */
export async function requirePortalSession() {
	if (!baseConfig.portalMode) {
		// A workspace cockpit must never expose provisioning — its container
		// deliberately lacks the admin env, and the surface belongs to the portal.
		throw serverFnError(403, "portal_only");
	}
	const session = await auth.api.getSession({ headers: getRequest().headers });
	if (!session) {
		throw serverFnError(401, "unauthenticated");
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
		throw serverFnError(403, "not_a_member");
	}
}
