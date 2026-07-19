// The better-auth instance (DAT-819) — the ONE auth surface of the
// installation, shared by both roles of the cockpit image (DD/51740673):
//
//   - the PORTAL mounts the HTTP handler (routes/api/auth/$) and the login UI;
//     it ISSUES the session cookie on the parent domain,
//   - every per-workspace cockpit instantiates the same config against the
//     same shared cockpit_db and VERIFIES that cookie server-side
//     (auth.api.getSession) — no cross-service auth calls, the database is
//     the rendezvous.
//
// Version note: better-auth is pinned to the 1.7 rc line — the only line
// whose Drizzle adapter supports our deliberate drizzle-orm 1.0.0-rc pin
// (better-auth#6766); it advances on its own channel, never blanket-`latest`.
//
// Deliberately thin (auth decision on DAT-819): email+password only.
// Enterprise SSO (e.g. Azure AD) arrives by ADDING better-auth's OIDC/SSO
// plugin here WHEN a customer needs it — the seam is this plugins array, and
// nothing outside src/auth/ may depend on better-auth internals, so swapping
// the provider entirely (Kinde is the long-term managed-cloud direction)
// never touches workspace routing.

import "@tanstack/react-start/server-only";

import { betterAuth } from "better-auth";
import { drizzleAdapter } from "better-auth/adapters/drizzle";
import { tanstackStartCookies } from "better-auth/tanstack-start";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { accounts, sessions, users, verifications } from "#/db/cockpit/schema";

/** The parent domain — the portal origin's hostname. Workspace subdomains
 * hang off it (`ws1.dataraum.localhost`), and the session cookie is scoped to
 * it so one login reaches every workspace cockpit. */
export const parentDomain = new URL(baseConfig.portalOrigin).hostname;

// Subdomain cookie sharing needs a dotted parent (`dataraum.localhost`); on a
// bare host like `localhost` (host dev without Caddy) there are no subdomains
// and the cookie stays host-only.
const crossSubDomain = parentDomain.includes(".");

export const auth = betterAuth({
	baseURL: baseConfig.portalOrigin,
	secret: baseConfig.authSecret,
	database: drizzleAdapter(cockpitDb, {
		provider: "pg",
		// Our tables are the schema-wide plural snake_case ones; the adapter
		// resolves model `user` -> key `users` (usePlural) and fields by the
		// camelCase TS keys. Schema passed explicitly: cockpitDb binds none
		// (client.ts keeps the relational API off).
		usePlural: true,
		schema: { users, sessions, accounts, verifications },
	}),
	emailAndPassword: {
		enabled: true,
	},
	// POSTs to the auth handler (sign-out from a workspace cockpit) originate
	// from workspace subdomains — trust the whole parent-domain family, not
	// just the portal origin.
	trustedOrigins: crossSubDomain ? [`*.${parentDomain}`] : [],
	advanced: {
		crossSubDomainCookies: {
			enabled: crossSubDomain,
			domain: parentDomain,
		},
	},
	// Must stay last: rewrites Set-Cookie through TanStack Start's response
	// handling so server-side flows (sign-in, sign-out) actually set cookies.
	plugins: [tanstackStartCookies()],
});
