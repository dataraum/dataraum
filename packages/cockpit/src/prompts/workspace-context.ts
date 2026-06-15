// Per-turn WORKSPACE CONTEXT for the agent — session-awareness (DAT-506).
//
// The agent learns state ONLY from what reaches its message context (tool
// results + the structured handoff parts), and the chat messages are
// in-memory, so a reload wipes them. Result: replay / teach / look_relationships
// / why_relationship had no way to know which session the user is in, and asked
// for an id.
//
// Fix: each turn, read the workspace's sessions from cockpit_db (`sessions` —
// the session-of-record post-DAT-506; the engine no longer has an
// `investigation_sessions` table) and hand the agent the CURRENT session (most
// recent — the honest proxy until the active-session state machine lands) plus
// the recent ones. The tables each session spans are resolved through its runs:
// cockpit `session_runs.run_id` → the engine `run_tables` view → `tables` →
// `sources`. A session is ambient state, like the workspace: the user is always
// in exactly one, and add_source / begin_session are the transitions into a new one.

import { desc, eq, inArray } from "drizzle-orm";
import { cockpitDb } from "../db/cockpit/client";
import {
	resolveActiveWorkspace,
	resolveActiveWorkspaceRow,
} from "../db/cockpit/registry";
import { sessionRuns, sessions } from "../db/cockpit/schema";
import { metadataDb } from "../db/metadata/client";
import { runTables, sources, tables } from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

const RECENT_LIMIT = 5;

/** A session as the agent should see it: its (engine) id, framed vertical, and
 * the human-named tables it spans (de-prefixed filenames). */
export interface SessionSummary {
	sessionId: string;
	vertical: string | null;
	tableNames: string[];
}

/** The engine session ids of a workspace's sessions, most recent first, with the
 * union of run ids each spans. cockpit_db is the session-of-record (DAT-506);
 * the engine session id is the value the tools + workflow ids key on. */
async function recentSessionsWithRuns(
	workspaceId: string,
	limit: number,
): Promise<Array<{ engineSessionId: string; runIds: string[] }>> {
	const rows = await cockpitDb
		.select({
			engineSessionId: sessions.engineSessionId,
			createdAt: sessions.createdAt,
			runId: sessionRuns.runId,
		})
		.from(sessions)
		.leftJoin(sessionRuns, eq(sessionRuns.sessionId, sessions.id))
		.where(eq(sessions.workspaceId, workspaceId))
		.orderBy(desc(sessions.createdAt));

	// Fold runs under their session, preserving most-recent-first session order.
	const order: string[] = [];
	const runsBySession = new Map<string, Set<string>>();
	for (const r of rows) {
		const set = runsBySession.get(r.engineSessionId);
		if (set === undefined) {
			order.push(r.engineSessionId);
			runsBySession.set(
				r.engineSessionId,
				new Set(r.runId ? [r.runId] : []),
			);
		} else if (r.runId) {
			set.add(r.runId);
		}
	}
	return order
		.slice(0, limit)
		.map((engineSessionId) => ({
			engineSessionId,
			runIds: [...(runsBySession.get(engineSessionId) ?? [])],
		}));
}

/**
 * The most recent session that actually HAS linked tables — the CURRENT session
 * the user is in, until the app-state active-session machine replaces this proxy.
 * Null when the workspace has no usable session yet.
 *
 * Why "with linked tables", not just "most recent": a freshly-recorded session has
 * no `run_tables` rows for a window (the engine links them mid-typing). So treating
 * it as "current" would (a) make a bare "replay" right after replaying throw "no
 * sources", and (b) let an in-flight run hijack "current" mid-conversation.
 * Filtering to sessions-with-tables keeps "current" on the last USABLE session —
 * and means "replay" re-runs the one you taught (the original, which has tables),
 * not the in-flight replay seed.
 *
 * `replay` defaults to this so "replay" re-runs the taught session without an id.
 */
export async function currentSessionId(): Promise<string | null> {
	const workspaceId = await resolveActiveWorkspace();
	const recent = await recentSessionsWithRuns(workspaceId, RECENT_LIMIT);
	for (const s of recent) {
		if (s.runIds.length === 0) continue;
		const [row] = await metadataDb
			.select({ tableId: runTables.tableId })
			.from(runTables)
			.where(inArray(runTables.runId, s.runIds))
			.limit(1);
		if (row) return s.engineSessionId;
	}
	return null;
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

/** Read the recent sessions + the tables each spans, most-recent first. Only
 * sessions WITH linked tables (see `currentSessionId` — a freshly-recorded,
 * still in-flight session has none yet and isn't something the agent can act on).
 * The vertical is the WORKSPACE's (DAT-506: it's a workspace property, not a
 * per-session pick). */
export async function buildWorkspaceContext(): Promise<string | null> {
	const workspace = await resolveActiveWorkspaceRow();
	const recent = await recentSessionsWithRuns(workspace.id, RECENT_LIMIT);
	if (recent.length === 0) return null;

	// All run ids across the recent sessions, mapped back to their session so the
	// table names group correctly.
	const sessionByRun = new Map<string, string>();
	for (const s of recent) {
		for (const runId of s.runIds) sessionByRun.set(runId, s.engineSessionId);
	}
	const runIds = [...sessionByRun.keys()];

	const links =
		runIds.length === 0
			? []
			: await metadataDb
					.select({
						runId: runTables.runId,
						tableName: tables.tableName,
						sourceName: sources.name,
					})
					.from(runTables)
					.innerJoin(tables, eq(tables.tableId, runTables.tableId))
					.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
					.where(inArray(runTables.runId, runIds));

	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the identity fields the underlying tables guarantee.
	const namesBySession = new Map<string, Set<string>>();
	for (const l of links) {
		const sessionId = sessionByRun.get(l.runId ?? "") ?? "";
		const set = namesBySession.get(sessionId) ?? new Set<string>();
		set.add(displayTableName(l.tableName ?? "", l.sourceName ?? ""));
		namesBySession.set(sessionId, set);
	}

	// Only sessions that resolved at least one table (the usable ones).
	const usable = recent.filter(
		(s) => (namesBySession.get(s.engineSessionId)?.size ?? 0) > 0,
	);
	if (usable.length === 0) return null;

	return formatWorkspaceContext(
		usable.map((s) => ({
			sessionId: s.engineSessionId,
			// Vertical is a workspace property now (DAT-506) — the same value on
			// every session of this workspace.
			vertical: workspace.vertical,
			tableNames: [
				...(namesBySession.get(s.engineSessionId) ?? []),
			].sort(),
		})),
	);
}
