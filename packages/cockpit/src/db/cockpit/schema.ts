// cockpit_db schema — the cockpit's control plane (DAT-461).
//
// Owned by TanStack Start via Drizzle ORM. Lives in its own Postgres database
// (`cockpit_db`) inside the shared Postgres instance, separate from the engine's
// `dataraum` / `dataraum_lake_catalog` databases and from the engine-owned
// `ws_<id>` analytical schema (which the cockpit reads through ../metadata/).
//
// Control plane vs data plane (DD/32538626): the engine owns analytical data;
// cockpit_db owns *who / which-workspace / which-session / which-runs*. Sessions
// live HERE now (DAT-506) — the engine dropped `investigation_sessions`; the run's
// table set is anchored engine-side by `run_tables` (keyed by `run_id`). `sessions`
// is the session-of-record, keyed to a run-correlation id (`engineSessionId`).
//
// Source of truth: this file. Migrations land in ../../../drizzle/cockpit/ via
// `bun run db:generate:cockpit`, applied by `bun run db:migrate:cockpit` (the
// compose `cockpit-migrate` init service on the stack; manual for host dev).

import type { UIMessage } from "@tanstack/ai-react";
import {
	boolean,
	index,
	integer,
	jsonb,
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
 *
 * `vertical` is the workspace's frame ontology (DAT-505): vertical is a WORKSPACE
 * property, not a per-add_source pick. The capability lands here (the column + the
 * boot-read via `resolveActiveWorkspaceRow`); the per-add_source vertical channel
 * (select.ts / the workflow payload) is retired in DAT-506 (Phase 5), which deletes
 * the session row that carries it today. Defaults to `_adhoc` (the no-vertical
 * placeholder) so a freshly-seeded workspace is always valid.
 */
export const workspaces = pgTable("workspaces", {
	id: varchar("id").primaryKey(),
	name: varchar("name").notNull(),
	engineSchema: varchar("engine_schema").notNull(),
	vertical: varchar("vertical").notNull().default("_adhoc"),
	createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
	archivedAt: timestamp("archived_at", { mode: "date" }),
});

/**
 * A control-plane session — the cockpit's record of an analytical session, the
 * session-of-record (DAT-506: the engine no longer has `investigation_sessions`).
 * `engineSessionId` (UNIQUE) is the run-correlation id the workflow ids + the
 * engine identity header key on. begin_session/add_source/replay each create one;
 * operating_model REUSES begin_session's (looked up by `engineSessionId`). `kind`
 * is the run origin (onboarding | begin_session | replay); `status` carries the
 * lifecycle (active | ended | archived) that DAT-404 will drive.
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
 *
 * `runId` is Temporal's EXECUTION runId (`firstExecutionRunId`) — what
 * `getHandle(workflowId, runId)` pins for the progress poll / reconcile. The engine
 * mints its OWN internal metadata `run_id` (the version axis, DAT-413) and resolves
 * replay from the generation heads, so the cockpit does not store it (DAT-506).
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
		// The atomic claim for the run-completion narration (Phase 2A): the server
		// watcher sets this (conditional UPDATE … WHERE … IS NULL) the first time it
		// narrates a run's completion, so the agent narrates EXACTLY once even with
		// several watchers for one conversation (multi-tab — each open
		// /api/chat-stream hosts its own watcher). Distinct from `status`: the
		// terminal-status writers (the progress poll / reconcile) don't touch it, so
		// the claim never races them. NULL = not yet narrated.
		completionNarratedAt: timestamp("completion_narrated_at", { mode: "date" }),
	},
	(t) => [
		uniqueIndex("session_runs_workflow_run_uq").on(t.workflowId, t.runId),
		index("session_runs_session_idx").on(t.sessionId),
	],
);

/**
 * A server-owned chat thread — the DAT-462 flip. The conversation belongs to a
 * WORKSPACE, not a single session: one thread spans many workflow sessions (you
 * chat, trigger add_source → session 1, chat more, trigger begin_session →
 * session 2, all in one transcript). Its `id` is the AG-UI `threadId` the client
 * hydrates on reload. cockpit_db is the source of truth; the client is a view
 * seeded via `initialMessages` and updated by the stream. One active conversation
 * per workspace for now (multi-conversation history is a later concern).
 */
export const conversations = pgTable(
	"conversations",
	{
		id: varchar("id").primaryKey(),
		workspaceId: varchar("workspace_id")
			.notNull()
			.references(() => workspaces.id),
		createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
		updatedAt: timestamp("updated_at", { mode: "date" }).notNull().defaultNow(),
	},
	(t) => [index("conversations_workspace_idx").on(t.workspaceId)],
);

/**
 * One persisted message per row (server appends; no blob rewrite). `message` is
 * the `UIMessage` verbatim so the transcript restores exactly; `id` is the
 * message's own id (PK → idempotent append by message id). `seq` orders within
 * the conversation. `modelOnly` rows are the refs channel (DAT-452 flip): fed to
 * the model via `buildModelMessages` but NEVER returned to the display transcript
 * — the leak the `agent-refs` text-marker convention used to prevent, now
 * impossible by construction. `role` is denormalized off `message` for filtering.
 */
export const conversationMessages = pgTable(
	"conversation_messages",
	{
		id: varchar("id").primaryKey(),
		conversationId: varchar("conversation_id")
			.notNull()
			.references(() => conversations.id),
		seq: integer("seq").notNull(),
		role: varchar("role").notNull(),
		message: jsonb("message").$type<UIMessage>().notNull(),
		modelOnly: boolean("model_only").notNull().default(false),
		createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
	},
	(t) => [
		index("conversation_messages_conversation_idx").on(t.conversationId, t.seq),
	],
);

/**
 * Per-conversation UI state restored on reload — the canvas "viewing history"
 * pin (DAT-354 `pinnedCallId`) so a refresh returns to the same focus rather
 * than snapping back to live. 1:1 with a conversation (its id is the PK). Kept
 * deliberately thin; more prefs join as columns when a surface needs them.
 */
export const uiState = pgTable("ui_state", {
	conversationId: varchar("conversation_id")
		.primaryKey()
		.references(() => conversations.id),
	pinnedCallId: varchar("pinned_call_id"),
	updatedAt: timestamp("updated_at", { mode: "date" }).notNull().defaultNow(),
});
