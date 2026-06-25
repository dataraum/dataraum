// cockpit_db schema — the cockpit's control plane (DAT-461).
//
// Owned by TanStack Start via Drizzle ORM. Lives in its own Postgres database
// (`cockpit_db`) inside the shared Postgres instance, separate from the engine's
// `dataraum` / `dataraum_lake_catalog` databases and from the engine-owned
// `ws_<id>` analytical schema (which the cockpit reads through ../metadata/).
//
// Control plane vs data plane (DD/32538626): the engine owns analytical data;
// cockpit_db owns *who / which-workspace / which-runs*. Post-DAT-506 the analytical
// truth is the engine's per-table / catalog GENERATION heads; the cockpit's job is
// purely run-grouping. DAT-562 RETIRED the `sessions` table: a cockpit "session" no
// longer scoped anything (the id never went on the wire, no engine table carried it,
// every stage resolves what it operates on from the heads — workspace-current), and
// minting one per import only fragmented run-grouping. Runs now group by WORKSPACE
// directly (`runs.workspaceId`), and workflow ids drop the session segment
// (`addsource-<ws>` — see temporal/workflow-id.ts).
//
// Source of truth: this file. Migrations land in ../../../drizzle/cockpit/ via
// `bun run db:generate:cockpit`, applied by `bun run db:migrate:cockpit` (the
// compose `cockpit-migrate` init service on the stack; manual for host dev).

import type { UIMessage } from "@tanstack/ai-react";
import {
	type AnyPgColumn,
	boolean,
	index,
	integer,
	jsonb,
	pgTable,
	text,
	timestamp,
	uniqueIndex,
	varchar,
} from "drizzle-orm/pg-core";
import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";

/**
 * Who triggered control-plane work. A coarse identity seam (DAT-460): a single
 * seeded `default` row for now — NO auth, NO multi-user. Real actors/auth are
 * Phase 3 (DAT-357). The registry seeds the `default` row; run attribution
 * (`createdBy`) was carried by the retired `sessions` table (DAT-562) and had no
 * reader, so it is reintroduced on `runs` only when auth lands.
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
 * One Temporal run in a workspace — the reload-recovery substrate (DAT-462 reads
 * non-terminal rows to re-attach progress) and the native monitor's row (DAT-550).
 * Runs group by WORKSPACE directly (DAT-562 retired the per-import `sessions` row):
 * every add_source / begin_session / operating_model run — fresh or a teach-replay —
 * is one row, attributed to its workspace.
 *
 * `kind` is the run's ORIGIN (onboarding | begin_session | replay) — formerly the
 * `sessions` row's origin, now the run's own truth (a run has exactly one origin).
 * `stage` is the workflow that ran (add_source | begin_session | operating_model);
 * `(workflowId, runId)` is the Temporal identity the progress widget polls, UNIQUE
 * so an idempotent record call can't double-insert.
 *
 * `runId` is Temporal's EXECUTION runId (`firstExecutionRunId`) — what
 * `getHandle(workflowId, runId)` pins for the progress poll / reconcile. The engine
 * mints its OWN internal metadata `run_id` (the version axis, DAT-413) and resolves
 * replay from the generation heads, so the cockpit does not store it (DAT-506).
 */
export const runs = pgTable(
	"runs",
	{
		id: varchar("id").primaryKey(),
		workspaceId: varchar("workspace_id")
			.notNull()
			.references(() => workspaces.id),
		// The run's origin (onboarding | begin_session | replay) — drives the
		// monitor's label. Was the retired `sessions.kind`; a run has one origin.
		kind: varchar("kind").notNull(),
		stage: varchar("stage").notNull(),
		workflowId: varchar("workflow_id").notNull(),
		runId: varchar("run_id").notNull(),
		// The conversation that STARTED this run (DAT-528) — the run-routing key.
		// The completion-watcher + reconcile filter on it so a run narrates into the
		// chat that triggered it, not whichever workspace watcher claims it first
		// (the old order-dependent bug). NULLABLE by design: a legacy run (pre-528)
		// or a future auto-orchestrated run has no originating
		// chat — it simply doesn't narrate. Stamped in `recordRun` from the
		// request-scoped ALS context (lib/run-context).
		conversationId: varchar("conversation_id").references(
			() => conversations.id,
		),
		status: varchar("status").notNull().default("running"),
		startedAt: timestamp("started_at", { mode: "date" }).notNull().defaultNow(),
		// Why the run is parked in `status='awaiting_input'` (DAT-551 P3c): the
		// grounding-teach agent fixed what it mechanically could and a human-judgement
		// gap remains (a concept/relationship the agent must not auto-apply), or it hit
		// its attempt limit. One sentence the surface shows + deep-links a Stage chat
		// from. NULL for every other run. Written by the grounding-loop workflow's markRunAwaitingInput.
		awaitingNote: text("awaiting_note"),
	},
	(t) => [
		uniqueIndex("runs_workflow_run_uq").on(t.workflowId, t.runId),
		index("runs_workspace_idx").on(t.workspaceId),
		// The run-routing filter (DAT-528): the watcher/reconcile scope by it.
		index("runs_conversation_idx").on(t.conversationId),
	],
);

/**
 * A server-owned chat thread — the DAT-462 flip. The conversation belongs to a
 * WORKSPACE, not a single session: one thread spans many workflow sessions (you
 * chat, trigger add_source → session 1, chat more, trigger begin_session →
 * session 2, all in one transcript). Its `id` is the AG-UI `threadId` the client
 * hydrates on reload. cockpit_db is the source of truth; the client is a view
 * seeded via `initialMessages` and updated by the stream.
 *
 * Typed, resumable chat-sessions (DAT-528): a workspace has MANY conversations,
 * each with an immutable `kind` (connect | stage | analyse) that binds its
 * toolstack + system prompt ("skill" — the binding itself is S2). They are listed
 * (bounded recent) + resumable by id; `lastActiveAt` is the recency axis the
 * history list orders on.
 */
export const conversations = pgTable(
	"conversations",
	{
		id: varchar("id").primaryKey(),
		workspaceId: varchar("workspace_id")
			.notNull()
			.references(() => workspaces.id),
		// The immutable chat type (DAT-528). NOT NULL + never updated after create —
		// a chat cannot change type, and the user cannot jump types within one chat.
		// S1 stores + displays it and routes runs by conversation; S2 fences the
		// toolstack on it.
		kind: varchar("kind").notNull(),
		// A short human label for the history list — the first user message, sliced
		// (a Haiku summary is deferred, S4). Null until the first turn names it.
		title: varchar("title"),
		createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
		updatedAt: timestamp("updated_at", { mode: "date" }).notNull().defaultNow(),
		// Last chat ACTIVITY — the recency axis the bounded history list orders on,
		// bumped on each message append. Distinct in MEANING from `updatedAt` (any
		// row mutation, e.g. a future title edit), though the two coincide today.
		lastActiveAt: timestamp("last_active_at", { mode: "date" })
			.notNull()
			.defaultNow(),
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

/**
 * A minted report (DAT-624) — a frozen { SQL + summary (+ chart config) } widget
 * over LIVE data. Created from an `answer`: the composed CTE, the answer narrative,
 * and the confidence are captured at mint; the report RE-RUNS the SQL on every open
 * (no result snapshot, no run_id / catalog pin — the standard BI model, so numbers
 * stay current). Workspace-owned and session-independent: it outlives the chat it
 * was minted from, which is why `conversationId` / `messageId` are NULLABLE
 * provenance, not owners — deleting the chat must never orphan the report.
 *
 * `parentId` is the evolve-lineage self-reference (DAT-627): null for a freshly
 * minted report, set when re-minted from a drilled-down answer. Deletion is SOFT
 * (`deletedAt`) — a deleted parent keeps its children. `summaryFingerprint`
 * (DAT-625 staleness) and `chartConfig` (DAT-626 charts) are reserved here so those
 * phases need no migration; both stay null until then.
 */
export const reports = pgTable(
	"reports",
	{
		id: varchar("id").primaryKey(),
		workspaceId: varchar("workspace_id")
			.notNull()
			.references(() => workspaces.id),
		// Provenance (nullable): the chat the report was minted from. Mirrors
		// `runs.conversationId` — a report outlives the chat, so the chat doesn't own
		// it. `messageId` is a plain pointer (no FK): pure provenance, and report
		// lifetime must not couple to message-row lifetime.
		conversationId: varchar("conversation_id").references(
			() => conversations.id,
		),
		messageId: varchar("message_id"),
		// Evolve lineage (DAT-627): the report this one was drilled-down from. Self-FK
		// is safe under soft-delete (the parent row is never physically removed).
		parentId: varchar("parent_id").references((): AnyPgColumn => reports.id),
		// The ONLY editable field — a human label, defaulted from the answer at mint.
		title: varchar("title").notNull(),
		// The frozen answer narrative. Immutable text; the DAT-625 staleness pass
		// regenerates it via Haiku when the result fingerprint drifts.
		summary: text("summary").notNull(),
		// Result fingerprint at last summary-gen — drives the DAT-625 outdated flag.
		// Null until that phase populates it.
		summaryFingerprint: varchar("summary_fingerprint"),
		// The frozen composed CTE (stable lake names) — re-run live on every open.
		sql: text("sql").notNull(),
		// Frozen chart config (DAT-626) — null = table-only report (first-class).
		chartConfig: jsonb("chart_config"),
		// The answer's confidence at mint (band / grounded ratio / reuse) — colored
		// in the gallery + detail, never recomputed.
		confidence: jsonb("confidence").$type<AnswerConfidence>().notNull(),
		createdAt: timestamp("created_at", { mode: "date" }).notNull().defaultNow(),
		// Soft delete — a deleted report drops out of the gallery; its children
		// (parentId) remain. Null = live.
		deletedAt: timestamp("deleted_at", { mode: "date" }),
	},
	(t) => [
		// The gallery list: a workspace's reports, newest first.
		index("reports_workspace_idx").on(t.workspaceId, t.createdAt),
	],
);
