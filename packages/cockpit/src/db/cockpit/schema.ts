// cockpit_db schema — the cockpit's control plane (DAT-461).
//
// Owned by TanStack Start via Drizzle ORM. Lives in its own Postgres database
// (`cockpit_db`) inside the shared Postgres instance, separate from the engine's
// `dataraum` / `dataraum_lake_catalog` databases and from the engine-owned
// `ws_<id>` analytical schema (which the cockpit reads through ../metadata/).
//
// Control plane vs data plane (DD/32538626): the engine owns analytical data +
// its `investigation_sessions` run anchor; cockpit_db owns *who / which-workspace
// / which-session / which-runs*. These tables are PURELY ADDITIVE — `sessions`
// references the engine's session id (`engineSessionId`), it does not replace it;
// the driver tools keep seeding `investigation_sessions` unchanged.
//
// Source of truth: this file. Migrations land in ../../../drizzle/cockpit/ via
// `bun run db:generate:cockpit`, applied by `bun run db:migrate:cockpit` (the
// compose `cockpit-migrate` init service on the stack; manual for host dev).
//
// Still to land here (later phases of DAT-460): conversations /
// conversation_messages (chat persistence, DAT-462) · ui_state (DAT-462).

import {
	index,
	pgTable,
	timestamp,
	uniqueIndex,
	varchar,
} from "drizzle-orm/pg-core";

/**
 * Who triggered control-plane work. A coarse identity seam (DAT-460): a single
 * seeded `default` row for now — NO auth, NO multi-user. Real actors/auth are
 * Phase 3 (DAT-357). `sessions.createdBy` references this so attribution exists
 * from day one without threading `actor_id` through the engine (the retired
 * DAT-365 approach — identity is a control-plane concern, kept out of the data
 * plane).
 */
export const actors = pgTable("actors", {
	id: varchar("id").primaryKey(),
	displayName: varchar("display_name").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
});

/**
 * The workspace registry — the source of truth for "which workspace", replacing
 * the bare `DATARAUM_WORKSPACE_ID` env read. Seeded from that env var on first
 * resolve (registry.ts). `id` is the workspace key (the same value the engine is
 * bootstrapped with, e.g. `00000000-…-001`); `engineSchema` is the derived
 * `ws_<id>` Postgres schema the metadata client reads. Phase 1 is single-active-
 * workspace; the switcher + lifecycle are Phase 3 (DAT-357 — hence `archivedAt`).
 */
export const workspaces = pgTable("workspaces", {
	id: varchar("id").primaryKey(),
	name: varchar("name").notNull(),
	engineSchema: varchar("engine_schema").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
	archivedAt: timestamp("archived_at", { mode: "date" }),
});

/**
 * A control-plane session — the cockpit's own record of an analytical session,
 * keyed to the engine's session by `engineSessionId` (UNIQUE, the join into the
 * engine's `investigation_sessions`). Additive: begin_session/add_source/replay
 * each create one; operating_model REUSES begin_session's (looked up by
 * `engineSessionId`). `kind` mirrors the engine seed intent
 * (onboarding | begin_session | replay); `status` carries the lifecycle
 * (active | ended | archived) that DAT-404 will drive.
 */
export const sessions = pgTable(
	"sessions",
	{
		id: varchar("id").primaryKey(),
		workspaceId: varchar("workspace_id")
			.notNull()
			.references(() => workspaces.id),
		engineSessionId: varchar("engine_session_id").notNull(),
		kind: varchar("kind").notNull(),
		status: varchar("status").notNull().default("active"),
		createdBy: varchar("created_by")
			.notNull()
			.references(() => actors.id),
		createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
		endedAt: timestamp("ended_at", { mode: "date" }),
	},
	(t) => [
		// One cockpit session per engine session — the upsert key the tools use so
		// operating_model's append (and any re-run) reuses the row instead of
		// duplicating it.
		uniqueIndex("sessions_engine_session_uq").on(t.engineSessionId),
		index("sessions_workspace_idx").on(t.workspaceId),
	],
);

/**
 * One Temporal run under a session — the reload-recovery substrate (DAT-462
 * reads non-terminal rows to re-attach progress). A session has 1:N runs:
 * re-runs / teach-replays append a new row. `stage` is the workflow that ran
 * (add_source | begin_session | operating_model); `(workflowId, runId)` is the
 * Temporal identity the progress widget polls, UNIQUE so an idempotent record
 * call can't double-insert.
 */
export const sessionRuns = pgTable(
	"session_runs",
	{
		id: varchar("id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => sessions.id),
		stage: varchar("stage").notNull(),
		workflowId: varchar("workflow_id").notNull(),
		runId: varchar("run_id").notNull(),
		status: varchar("status").notNull().default("running"),
		startedAt: timestamp("started_at", { mode: "date" }).notNull().defaultNow(),
	},
	(t) => [
		uniqueIndex("session_runs_workflow_run_uq").on(t.workflowId, t.runId),
		index("session_runs_session_idx").on(t.sessionId),
	],
);
