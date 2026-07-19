// Mode + role-shared configuration (DAT-819). SERVER-ONLY.
//
// The cockpit image serves two roles (DD/51740673): a per-WORKSPACE cockpit
// (the default) and the per-installation PORTAL — login + membership routing —
// when DATARAUM_PORTAL_MODE=1. This module carries exactly the fields BOTH
// roles need, parsed eagerly in either mode. Portal-mode boot must never
// evaluate ./config (the workspace config): that schema requires workspace-only
// env — metadata role URLs, S3, lake path — a portal container deliberately
// does not have, and it throws born-loud in portal mode for exactly that
// reason. Boot seams that run in both modes (the Nitro plugins, otel, the
// cockpit_db client, the auth module) read THIS module.

import "@tanstack/react-start/server-only";

import { z } from "zod";

const BaseConfigSchema = z.object({
	// Portal role flag — `DATARAUM_PORTAL_MODE=1` (same env contract style as
	// DUCKLAKE_SKIP_INSTALL); anything else is a workspace cockpit.
	portalMode: z.boolean(),

	// cockpit_db — the ONE shared control-plane database of the installation
	// (registry, memberships, auth tables, chat history). Both roles read it.
	cockpitDatabaseUrl: z.string().min(1),

	// better-auth signing secret — required in BOTH modes: the portal issues
	// the session cookie, every workspace cockpit verifies it, and they only
	// agree when they share this value (one installation, one secret).
	authSecret: z.string().min(1),

	// The portal's own origin (scheme + host [+ port]), e.g.
	// `http://dataraum.localhost`. Three derived duties: it is better-auth's
	// baseURL, its hostname is the parent domain the session cookie lives on
	// (workspace subdomains hang off it — `ws1.dataraum.localhost`), and it is
	// the redirect target a workspace cockpit sends signed-out users and
	// non-members to. Defaults to the bare host-dev address (no Caddy, no
	// subdomains — the cookie stays host-only, see auth/auth.ts).
	portalOrigin: z.url(),

	// Dev seed credentials (compose/host dev only): when BOTH are set, the
	// workspace registry seed creates this credential user via better-auth and
	// grants it membership in the boot workspace, so a fresh stack is
	// immediately log-in-able (reference_smoke_runbook). Unset in production —
	// real users arrive through the portal's sign-up (invitations are a
	// DAT-821 concern).
	devUserEmail: z.string().min(1).optional(),
	devUserPassword: z.string().min(1).optional(),

	// OTLP sink (ADR-0019/DAT-705) — installation-wide, meaningful in both
	// modes; unset/empty = telemetry off. Lives here (not ./config) because
	// the otel Nitro plugin bootstraps in portal mode too.
	otelExporterOtlpEndpoint: z.string().optional(),
});

export type BaseConfig = z.infer<typeof BaseConfigSchema>;

function loadBaseConfig(): BaseConfig {
	const parsed = BaseConfigSchema.safeParse({
		portalMode: (process.env.DATARAUM_PORTAL_MODE ?? "0") === "1",
		cockpitDatabaseUrl: process.env.COCKPIT_DATABASE_URL,
		authSecret: process.env.BETTER_AUTH_SECRET,
		portalOrigin: process.env.DATARAUM_PORTAL_ORIGIN || "http://localhost:3000",
		devUserEmail: process.env.DATARAUM_DEV_USER_EMAIL || undefined,
		devUserPassword: process.env.DATARAUM_DEV_USER_PASSWORD || undefined,
		// `|| undefined`: an empty string (compose interpolation of an unset
		// var) means OFF, never a half-configured exporter.
		otelExporterOtlpEndpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || undefined,
	});

	if (!parsed.success) {
		const details = parsed.error.issues
			.map((issue) => `  ${issue.path.join(".") || "(root)"}: ${issue.message}`)
			.join("\n");
		throw new Error(
			`Invalid cockpit base configuration — check your .env / environment:\n${details}`,
		);
	}

	return parsed.data;
}

export const baseConfig = loadBaseConfig();
