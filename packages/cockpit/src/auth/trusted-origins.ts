// The `trustedOrigins` wildcard pattern for an installation (DAT-819).
//
// Split out of auth.ts so it can be tested without instantiating better-auth
// (which needs a DB adapter and the whole config chain).
//
// The subtlety this exists to hold: better-auth matches a wildcard
// `trustedOrigins` entry against `new URL(url).host`
// (`dist/auth/trusted-origins.mjs` → `getHost`), and `host` INCLUDES a
// non-default port. A pattern built from the origin's *hostname* therefore
// never matches a request from `ws1.dataraum.localhost:8000`, so on any stack
// moved off :80 (CADDY_HTTP_PORT) every sign-out POST from a workspace
// subdomain fails 403 INVALID_ORIGIN. Building it from `host` is correct in
// both cases — it collapses to the bare hostname on the scheme's default port.
//
// Note this is deliberately NOT the same derivation as the session cookie's
// `domain`, which uses `hostname`: cookies are not port-scoped.

/** `*.<host>` for the portal origin — port included when non-default. */
export function trustedOriginPattern(portalOrigin: string): string {
	return `*.${new URL(portalOrigin).host}`;
}
