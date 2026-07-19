# ADR-0022 — Portal auth is better-auth, self-hosted in cockpit_db, behind a thin seam

- **Status:** Accepted
- **Date:** 2026-07-19
- **Ticket:** DAT-819 (Phase 6 of the multi-workspace epic DAT-813)
- **Design doc:** Confluence DD/51740673

## Context

The multi-workspace design puts a small portal on the installation's parent domain:
login, then routing to the user's workspaces by `memberships`, each workspace's cockpit
on its own Caddy-terminated subdomain. That needs a real identity/session system where
DAT-817 had a seeded placeholder user. Constraints: self-hosted (an installation is
self-contained), tables in `cockpit_db` (ADR-0003 — the cockpit owns its control plane),
enterprise SSO eventually but not now, and a managed-cloud future where auth is likely
outsourced (Kinde) — so the integration must stay swappable.

## Decision

**v1 identity is [better-auth](https://better-auth.com), self-hosted, its tables living
in `cockpit_db` via its Drizzle adapter.** Its `user` model IS the `users` table — one
identity model, no parallel user journals; `memberships` FKs onto it, and the DAT-817
placeholder row is retired. Sessions are DB rows in the shared `cockpit_db`, which is
what lets every per-workspace cockpit verify the portal-issued parent-domain cookie
locally (same instance config + secret, no cross-service auth calls).

**The organization plugin is deliberately NOT adopted.** The `workspaces` registry is
the workspace source of truth (ADR-0012), with a provisioner-owned lifecycle and
resource record; better-auth organizations would be a second workspace journal with
user-driven creation semantics the provisioner owns. Plain `memberships` carries
workspace access. If invitations/roles (DAT-821+) want the plugin, adopting it is a
clean cut that replaces `memberships`, not an overlay.

**The auth surface is one module wide** (`src/auth/` + the `/api/auth/$` mount): nothing
outside it may depend on better-auth internals beyond the table shapes. Enterprise SSO
(e.g. Azure AD) arrives by adding better-auth's OIDC/SSO plugin inside that module when
a customer needs it — the seam exists, the feature is not built. The long-term
managed-cloud direction is **Kinde**; swapping providers must not touch workspace
routing, the membership gate, or the registry.

Enforcement is server-side: TanStack Start **global request middleware** checks session
+ membership of the boot workspace on every server request (SSR, `/api/*`, server-fn
RPCs). Route guards are UX only, never the boundary.

## Consequences

- Version pins: better-auth rides the **1.7 rc channel** (exact pin) — the only line
  whose Drizzle adapter supports our drizzle-orm 1.0.0-rc pin — and carries a
  `kysely 0.28.17` override (its bundled, unused kysely adapter imports root exports
  kysely 0.29 removed). Both advance deliberately, never blanket-`latest`.
- The parent-domain cookie (`crossSubDomainCookies`) requires a dotted parent host;
  bare-host dev (`localhost:3000`) stays host-only-cookie and portal-less by design.
- A dev credential user is seeded by the workspace registry when
  `DATARAUM_DEV_USER_EMAIL/_PASSWORD` are set — written directly in better-auth's own
  sign-up row shape, since the lazy seed runs inside unrelated requests where the
  sign-up endpoint would set a stray session cookie.
