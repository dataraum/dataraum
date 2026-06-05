// Per-turn WORKSPACE CONTEXT for the agent — session-awareness.
//
// The agent learns state ONLY from what reaches its message context (tool
// results + the structured handoff parts). The "Add source" button bypasses the
// agent entirely — it never sees the session it creates — and the chat messages
// are in-memory, so a reload wipes them. Result: replay / teach / look_relationships
// / why_relationship had no way to know which session the user is in, and asked
// for an id.
//
// Fix: each turn, read the workspace's sessions from `investigation_sessions` and
// hand the agent the CURRENT session (most recent — the honest proxy until the
// active-session state machine lands) plus the recent ones. A session is ambient
// state, like the workspace: the user is always in exactly one, and
// add_source / begin_session are the transitions into a new one.

import { desc, eq, inArray } from "drizzle-orm";
import { metadataDb } from "../db/metadata/client";
import {
	investigationSessions,
	sessionTables,
	sources,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

const RECENT_LIMIT = 5;

/** A session as the agent should see it: its id, framed vertical, and the
 * human-named tables it spans (de-prefixed filenames). */
export interface SessionSummary {
	sessionId: string;
	vertical: string | null;
	tableNames: string[];
}

/**
 * The most recent session that actually HAS linked tables — the CURRENT session
 * the user is in, until the app-state active-session machine replaces this proxy.
 * Null when the workspace has no usable session yet.
 *
 * Why "with linked tables", not just "most recent": add_source / begin_session /
 * replay all SEED the `investigation_sessions` row BEFORE the engine's async
 * typing phase links its tables. So a freshly-started session has no tables for a
 * window — and treating it as "current" would (a) make a bare "replay" right
 * after replaying throw "no sources", and (b) let an in-flight run hijack
 * "current" mid-conversation. Filtering to sessions-with-tables keeps "current"
 * on the last USABLE session — and means "replay" re-runs the one you taught (the
 * original, which has tables), not the in-flight replay seed. (Tiebreak by id so
 * two sessions sharing a `started_at` millisecond resolve deterministically.)
 *
 * `replay` defaults to this so "replay" re-runs the taught session without an id.
 */
export async function currentSessionId(): Promise<string | null> {
	const [row] = await metadataDb
		.selectDistinct({
			sessionId: investigationSessions.sessionId,
			startedAt: investigationSessions.startedAt,
		})
		.from(investigationSessions)
		.innerJoin(
			sessionTables,
			eq(sessionTables.sessionId, investigationSessions.sessionId),
		)
		.orderBy(
			desc(investigationSessions.startedAt),
			desc(investigationSessions.sessionId),
		)
		.limit(1);
	return row?.sessionId ?? null;
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
 * sessions WITH linked tables (see `currentSessionId` — a freshly-seeded, still
 * in-flight session has none yet and isn't something the agent can act on). */
export async function buildWorkspaceContext(): Promise<string | null> {
	const recent = await metadataDb
		.selectDistinct({
			sessionId: investigationSessions.sessionId,
			vertical: investigationSessions.vertical,
			startedAt: investigationSessions.startedAt,
		})
		.from(investigationSessions)
		.innerJoin(
			sessionTables,
			eq(sessionTables.sessionId, investigationSessions.sessionId),
		)
		.orderBy(
			desc(investigationSessions.startedAt),
			desc(investigationSessions.sessionId),
		)
		.limit(RECENT_LIMIT);
	if (recent.length === 0) return null;

	const ids = recent.map((s) => s.sessionId);
	const links = await metadataDb
		.select({
			sessionId: sessionTables.sessionId,
			tableName: tables.tableName,
			sourceName: sources.name,
		})
		.from(sessionTables)
		.innerJoin(tables, eq(tables.tableId, sessionTables.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(inArray(sessionTables.sessionId, ids));

	const namesBySession = new Map<string, Set<string>>();
	for (const l of links) {
		const set = namesBySession.get(l.sessionId) ?? new Set<string>();
		set.add(displayTableName(l.tableName, l.sourceName));
		namesBySession.set(l.sessionId, set);
	}

	return formatWorkspaceContext(
		recent.map((s) => ({
			sessionId: s.sessionId,
			vertical: s.vertical,
			tableNames: [...(namesBySession.get(s.sessionId) ?? [])].sort(),
		})),
	);
}
