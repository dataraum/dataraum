// Server functions for the portal's create-workspace flow (DAT-821) — the
// FIRST HTTP-reachable trigger of the DAT-820 provisioner, so every handler
// runs the authz gate itself (a route beforeLoad is UX, never the boundary —
// server fns are reachable by direct POST). The gate and its v1 policy live
// in #/portal/create-gate.server.ts: portal role only, session required, any
// signed-in user may create, the creator is the membership the row gets.
//
// Create itself is fire-and-forget into the portal process: createWorkspace
// is durable against the registry `state` cursor (lifecycle.ts), the advisory
// lock serializes per-workspace ops, and the progress poll reads the registry
// + the in-process tracker. Retry re-runs the SAME workspace id from the
// registry row's own values — the convergence contract, not a new attempt.

import { createServerFn } from "@tanstack/react-start";
import { getRequest } from "@tanstack/react-start/server";
import { and, eq, ne } from "drizzle-orm";
import { z } from "zod";
import { auth } from "#/auth/auth";
import { workspaceUrlFor } from "#/auth/workspace-url";
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import type { WorkspaceState } from "#/db/cockpit/registry";
import { workspaces } from "#/db/cockpit/schema";
import {
	SUBDOMAIN_LABEL_MAX,
	SUBDOMAIN_LABEL_PATTERN,
} from "#/lib/subdomain-label";
import {
	requireCreateVisibility,
	requirePortalSession,
} from "#/portal/create-gate.server";
import { createRunFor, trackCreateRun } from "#/portal/create-tracker";
import { createWorkspace } from "#/portal/lifecycle";
import { runLifecycle } from "#/portal/lifecycle-deps";
import { type BuiltinVertical, listBuiltinVerticals } from "#/portal/verticals";

// ── Form context ────────────────────────────────────────────────────────────

export type CreateContext =
	// A per-workspace cockpit — the create flow lives on the portal.
	| { mode: "workspace" }
	// Portal, signed out — the `/` login screen is the answer.
	| { mode: "signin" }
	| {
			mode: "form";
			verticals: BuiltinVertical[];
			/** For the live `<label>.<host>` URL preview (workspaceUrlFor). */
			portalOrigin: string;
	  };

/** What the `/create` route needs: role/session (redirect decisions) and the
 * pickable verticals off the bind-mounted config tree. */
export const getCreateContext = createServerFn({ method: "GET" }).handler(
	async (): Promise<CreateContext> => {
		if (!baseConfig.portalMode) {
			return { mode: "workspace" };
		}
		const session = await auth.api.getSession({
			headers: getRequest().headers,
		});
		if (!session) {
			return { mode: "signin" };
		}
		return {
			mode: "form",
			verticals: await listBuiltinVerticals(),
			portalOrigin: baseConfig.portalOrigin,
		};
	},
);

// ── Start ───────────────────────────────────────────────────────────────────

const StartCreateInput = z.object({
	name: z.string().trim().min(1).max(120),
	vertical: z.string().min(1),
	subdomain: z
		.string()
		.max(SUBDOMAIN_LABEL_MAX)
		.regex(SUBDOMAIN_LABEL_PATTERN, "not a valid DNS label"),
});

/**
 * Kick off a workspace create and return its id immediately — the progress
 * page polls `getCreateProgress`. The subdomain pre-check turns the common
 * conflict into an inline 409 at submit; the partial unique index remains the
 * real guard (a race just surfaces later as the run's failure).
 */
export const startWorkspaceCreate = createServerFn({ method: "POST" })
	.inputValidator((input: z.infer<typeof StartCreateInput>) =>
		StartCreateInput.parse(input),
	)
	.handler(async ({ data }) => {
		const session = await requirePortalSession();

		const verticals = await listBuiltinVerticals();
		if (!verticals.some((vertical) => vertical.name === data.vertical)) {
			// The engine resolves phase config by this key — an unknown vertical
			// would mint a workspace that fails at first add_source.
			throw Response.json({ error: "unknown_vertical" }, { status: 400 });
		}

		const [claimed] = await cockpitDb
			.select({ id: workspaces.id })
			.from(workspaces)
			.where(
				and(
					eq(workspaces.subdomain, data.subdomain),
					ne(workspaces.state, "archived"),
				),
			)
			.limit(1);
		if (claimed) {
			throw Response.json(
				{
					error: "subdomain_taken",
					message: `subdomain '${data.subdomain}' is already claimed by a live workspace — pick another label`,
				},
				{ status: 409 },
			);
		}

		const workspaceId = crypto.randomUUID();
		trackCreateRun(
			workspaceId,
			session.user.id,
			runLifecycle((deps) =>
				createWorkspace(
					{
						workspaceId,
						name: data.name,
						vertical: data.vertical,
						subdomain: data.subdomain,
						memberUserIds: [session.user.id],
					},
					deps,
				),
			),
		);
		return { workspaceId };
	});

// ── Retry ───────────────────────────────────────────────────────────────────

const WorkspaceIdInput = z.object({ workspaceId: z.uuid() });

/**
 * Re-run a create that died (`state = 'creating'`, no run in flight) — the
 * same-id convergence createWorkspace guarantees. Inputs come from the
 * registry row itself (what the first attempt recorded), never re-submitted.
 */
export const retryWorkspaceCreate = createServerFn({ method: "POST" })
	.inputValidator((input: z.infer<typeof WorkspaceIdInput>) =>
		WorkspaceIdInput.parse(input),
	)
	.handler(async ({ data }) => {
		const session = await requirePortalSession();
		const workspaceId = data.workspaceId;
		await requireCreateVisibility(workspaceId, session.user.id);

		if (createRunFor(workspaceId)?.status === "running") {
			// Idempotent re-submit: the run is already in flight here — starting
			// another would only trip the advisory lock.
			return { workspaceId };
		}
		const [row] = await cockpitDb
			.select({
				name: workspaces.name,
				vertical: workspaces.vertical,
				subdomain: workspaces.subdomain,
				state: workspaces.state,
			})
			.from(workspaces)
			.where(eq(workspaces.id, workspaceId))
			.limit(1);
		const subdomain = row?.subdomain;
		if (row?.state !== "creating" || !subdomain) {
			// Nothing to converge: the first attempt never wrote the row (e.g. a
			// subdomain conflict) or the workspace has moved on.
			throw Response.json({ error: "not_retryable" }, { status: 409 });
		}
		trackCreateRun(
			workspaceId,
			session.user.id,
			runLifecycle((deps) =>
				createWorkspace(
					{
						workspaceId,
						name: row.name,
						vertical: row.vertical,
						subdomain,
						memberUserIds: [session.user.id],
					},
					deps,
				),
			),
		);
		return { workspaceId };
	});

// ── Progress ────────────────────────────────────────────────────────────────

export interface CreateProgress {
	/** Registry state, or null while the run has not written the row yet. */
	state: WorkspaceState | null;
	name: string | null;
	subdomain: string | null;
	/** The workspace URL once `ready` (the redirect target). */
	url: string | null;
	/** A create op for this id is running in THIS portal process. False for a
	 * bare `creating` row (crashed/restarted portal) — the retry case. */
	inFlight: boolean;
	/** The last failure in this process, verbatim from the lifecycle. */
	error: string | null;
}

export const getCreateProgress = createServerFn({ method: "GET" })
	.inputValidator((input: z.infer<typeof WorkspaceIdInput>) =>
		WorkspaceIdInput.parse(input),
	)
	.handler(async ({ data }): Promise<CreateProgress> => {
		const session = await requirePortalSession();
		await requireCreateVisibility(data.workspaceId, session.user.id);

		const run = createRunFor(data.workspaceId);
		const [row] = await cockpitDb
			.select({
				name: workspaces.name,
				subdomain: workspaces.subdomain,
				state: workspaces.state,
			})
			.from(workspaces)
			.where(eq(workspaces.id, data.workspaceId))
			.limit(1);
		return {
			state: row ? (row.state as WorkspaceState) : null,
			name: row?.name ?? null,
			subdomain: row?.subdomain ?? null,
			url:
				row?.state === "ready" && row.subdomain
					? workspaceUrlFor(row.subdomain, baseConfig.portalOrigin)
					: null,
			inFlight: run?.status === "running",
			error: run?.status === "failed" ? (run.error ?? "create failed") : null,
		};
	});
