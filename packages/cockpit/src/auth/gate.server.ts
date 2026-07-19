// The workspace membership gate (DAT-819). SERVER-ONLY.
//
// THE data security boundary of a per-workspace cockpit: global request
// middleware (src/start.ts) runs this on EVERY server request — SSR documents,
// /api/* server routes, AND server-function RPCs — so a route-level guard is
// UX at most, never the boundary (route beforeLoad does not protect a
// server fn reachable by direct POST — TanStack auth guidance).
//
// Semantics (AC on DAT-819): a signed-out request is sent to the portal to
// log in; an authenticated user WITHOUT membership of THIS cockpit's boot
// workspace is rejected — HTML navigations bounce to the portal (which shows
// the denial and the user's real workspaces), API/RPC calls get the bare
// status. The portal role has no boot workspace and no gate (its only
// surfaces are login + the membership list, which guard themselves).
//
// Verification is LOCAL: the same better-auth instance reads the session row
// from shared cockpit_db (auth/auth.ts) — no call to the portal.

import "@tanstack/react-start/server-only";

import { and, eq } from "drizzle-orm";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { memberships } from "#/db/cockpit/schema";
import { auth } from "./auth";

// The auth handler itself is public by definition (sign-in posts from the
// portal UI, sign-out posts from a workspace cockpit, session reads).
const PUBLIC_PREFIXES = ["/api/auth/"];

/** An HTML navigation (redirect to the portal) vs an API/RPC caller (bare
 * status — a fetch following a 302 into portal HTML would only garble the
 * caller's JSON parse). */
function wantsHtml(request: Request): boolean {
	return (request.headers.get("accept") ?? "").includes("text/html");
}

function redirectTo(url: string): Response {
	return new Response(null, { status: 302, headers: { location: url } });
}

/**
 * Gate one request. Returns `null` to let it through, or the rejection
 * Response. Exercised by the global request middleware only — portal mode
 * passes everything (this module never loads there).
 */
export async function gateRequest(request: Request): Promise<Response | null> {
	const { pathname } = new URL(request.url);
	if (PUBLIC_PREFIXES.some((prefix) => pathname.startsWith(prefix))) {
		return null;
	}

	const session = await auth.api.getSession({ headers: request.headers });
	if (!session) {
		return wantsHtml(request)
			? redirectTo(baseConfig.portalOrigin)
			: Response.json({ error: "unauthenticated" }, { status: 401 });
	}

	// The boot workspace read lives behind a dynamic import: #/config is the
	// workspace config (it throws in portal mode), and this module is shared
	// through src/start.ts which loads in both roles.
	const { bootWorkspaceId } = await import("#/db/cockpit/registry");
	const workspaceId = bootWorkspaceId();
	const [membership] = await cockpitDb
		.select({ userId: memberships.userId })
		.from(memberships)
		.where(
			and(
				eq(memberships.userId, session.user.id),
				eq(memberships.workspaceId, workspaceId),
			),
		)
		.limit(1);
	if (!membership) {
		if (wantsHtml(request)) {
			const denied = new URL(baseConfig.portalOrigin);
			denied.searchParams.set("denied", workspaceId);
			return redirectTo(denied.toString());
		}
		return Response.json({ error: "not_a_member" }, { status: 403 });
	}

	return null;
}
