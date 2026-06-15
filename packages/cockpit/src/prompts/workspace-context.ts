// Per-turn WORKSPACE CONTEXT for the agent — session-awareness (DAT-506).
//
// The agent learns state ONLY from what reaches its message context (tool
// results + the structured handoff parts), and the chat messages are
// in-memory, so a reload wipes them. Result: replay / teach / look_relationships
// / why_relationship had no way to know which session the user is in, and asked
// for an id.
//
// Fix: each turn, read the workspace's sessions from cockpit_db (`sessions` — the
// session-of-record post-DAT-506; the engine no longer has `investigation_sessions`)
// and hand the agent the CURRENT session (most recent) plus the recent ones. The
// workspace's imported tables come from the live per-table GENERATION heads (the
// engine mints its own `run_id` the cockpit never sees, and no engine table carries
// the cockpit `session_id`, so a session→run_tables join is impossible at the
// cockpit edge — the generation heads ARE the workspace's current typed-table set,
// which in single-active-workspace equals the session's tables). A session is
// ambient state, like the workspace: the user is always in exactly one, and
// add_source / begin_session are the transitions into a new one.

import { desc, eq } from "drizzle-orm";
import { cockpitDb } from "../db/cockpit/client";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { sessions } from "../db/cockpit/schema";
import { metadataDb } from "../db/metadata/client";
import { GENERATION_STAGE } from "../db/metadata/relationship-target";
import { metadataSnapshotHead, runTables, sources, tables } from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

const RECENT_LIMIT = 5;

/** A session as the agent should see it: its (engine) id, framed vertical, and
 * the human-named tables the workspace spans (de-prefixed filenames). */
export interface SessionSummary {
	sessionId: string;
	vertical: string | null;
	tableNames: string[];
}

/** The engine session ids of a workspace's sessions, most recent first. cockpit_db
 * is the session-of-record (DAT-506); the engine session id is the value the tools
 * + workflow ids key on. */
async function recentSessions(
	workspaceId: string,
	limit: number,
): Promise<string[]> {
	const rows = await cockpitDb
		.select({ engineSessionId: sessions.engineSessionId })
		.from(sessions)
		.where(eq(sessions.workspaceId, workspaceId))
		.orderBy(desc(sessions.createdAt))
		.limit(limit);
	return rows.map((r) => r.engineSessionId);
}

/** The workspace's currently-imported tables (de-prefixed display names) — the
 * tables at the live per-table GENERATION heads (DAT-506). Empty until an
 * add_source run promotes. */
async function workspaceTableNames(): Promise<string[]> {
	const rows = await metadataDb
		.select({ tableName: tables.tableName, sourceName: sources.name })
		.from(metadataSnapshotHead)
		.innerJoin(runTables, eq(runTables.runId, metadataSnapshotHead.runId))
		.innerJoin(tables, eq(tables.tableId, runTables.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(eq(metadataSnapshotHead.stage, GENERATION_STAGE));
	const names = new Set<string>();
	for (const r of rows) {
		names.add(displayTableName(r.tableName ?? "", r.sourceName ?? ""));
	}
	return [...names].sort();
}

/**
 * The most recent session — the CURRENT session the user is in — when the
 * workspace has imported tables to act on; null otherwise. The "with tables" gate
 * keeps a bare "replay" right after onboarding from running before anything is
 * imported. (DAT-506: the table set is workspace-current, not per-session — the
 * engine run_id the cockpit can't see makes a per-session join impossible.)
 *
 * `replay` defaults to this so "replay" re-runs without an id.
 */
export async function currentSessionId(): Promise<string | null> {
	const workspaceId = (await resolveActiveWorkspaceRow()).id;
	const recent = await recentSessions(workspaceId, 1);
	if (recent.length === 0) return null;
	const tableNames = await workspaceTableNames();
	return tableNames.length > 0 ? recent[0] : null;
}

/**
 * Format the WORKSPACE CONTEXT block from the recent sessions (most-recent
 * first). Pure — the DB read lives in `buildWorkspaceContext`, so the wording +
 * the CURRENT tag are unit-testable. Null when there are no sessions (nothing to
 * tell the agent).
 */
export function formatWorkspaceContext(
	recent: SessionSummary[],
): string | null {
	if (recent.length === 0) return null;
	const lines = recent.map((s, i) => {
		const span =
			s.tableNames.length > 0 ? s.tableNames.join(", ") : "no tables yet";
		const tag = i === 0 ? " ← CURRENT (the session the user is in)" : "";
		return `- ${s.sessionId} — ${span} · vertical ${s.vertical ?? "_adhoc"}${tag}`;
	});
	return (
		"WORKSPACE CONTEXT — the analytical sessions in this workspace, most recent " +
		"first. The user is always in ONE session; the most recent is the CURRENT " +
		"one. For replay, teach, look_relationships, why_relationship and any " +
		"session-scoped action, use the CURRENT session's id (or the one the user " +
		"names) — never ask the user for a session id when this block has one. " +
		"`replay` with no session_id re-runs the CURRENT session.\n" +
		lines.join("\n")
	);
}

/** Read the recent sessions + the workspace's imported tables, most-recent first.
 * Only emitted when the workspace HAS imported tables (a freshly-recorded session
 * with nothing imported isn't something the agent can act on). The vertical is the
 * WORKSPACE's (DAT-506: a workspace property, not a per-session pick); the table
 * set is the workspace-current set (the generation heads), shown against each
 * recent session. */
export async function buildWorkspaceContext(): Promise<string | null> {
	const workspace = await resolveActiveWorkspaceRow();
	const [recent, tableNames] = await Promise.all([
		recentSessions(workspace.id, RECENT_LIMIT),
		workspaceTableNames(),
	]);
	if (recent.length === 0 || tableNames.length === 0) return null;

	return formatWorkspaceContext(
		recent.map((sessionId, i) => ({
			sessionId,
			// Vertical is a workspace property now (DAT-506) — same on every session.
			vertical: workspace.vertical,
			// The workspace-current tables belong to the CURRENT session; older
			// sessions list "no tables yet" (their own run's tables aren't separable
			// at the cockpit edge — see the module header).
			tableNames: i === 0 ? tableNames : [],
		})),
	);
}
