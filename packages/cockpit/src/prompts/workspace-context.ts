// Per-turn WORKSPACE CONTEXT for the agent — workspace-awareness (DAT-506, DAT-562).
//
// The agent learns state ONLY from what reaches its message context (tool results +
// the structured handoff parts), and the chat messages are in-memory, so a reload
// wipes them. Without a hint, replay / teach / look_relationships had no idea whether
// the workspace even has imported data, and asked the user for ids.
//
// Fix: each turn, tell the agent what the workspace HAS — its framed vertical and the
// imported tables — so session-scoped actions (replay / teach / look_relationships)
// just work without asking. There is no session id to surface: DAT-562 retired the
// cockpit "session" (it scoped nothing post-DAT-506 — the engine resolves every stage
// from the workspace-current generation heads), so replay/operating_model take no id
// and the context block names the workspace, not a session.

import { eq } from "drizzle-orm";
import type { ConversationKind } from "../db/cockpit/conversations";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import {
	buildWorkspaceBriefing,
	formatBriefingDigest,
} from "../db/metadata/briefing";
import { metadataDb } from "../db/metadata/client";
import { GENERATION_STAGE } from "../db/metadata/relationship-target";
import {
	metadataSnapshotHead,
	runTables,
	sources,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

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
 * Format the WORKSPACE CONTEXT block. Pure — the DB read lives in
 * `buildWorkspaceContext`, so the wording is unit-testable. Null when the workspace
 * has no imported tables (nothing the agent can act on yet).
 */
export function formatWorkspaceContext(
	vertical: string | null,
	tableNames: string[],
): string | null {
	if (tableNames.length === 0) return null;
	return (
		"WORKSPACE CONTEXT — the user's current workspace. Imported tables: " +
		`${tableNames.join(", ")} · vertical ${vertical ?? "_adhoc"}. For replay, ` +
		"teach, look_relationships, why_relationship and any session-scoped action, " +
		"operate on this workspace directly — never ask the user for a session id " +
		"(there is none). `replay` re-runs the workspace's imported sources; " +
		"`operating_model` re-runs over the workspace's begin_session result."
	);
}

/**
 * Read the workspace's vertical + imported tables and format the context block.
 * Only emitted when the workspace HAS imported tables (a freshly-created workspace
 * with nothing imported isn't something the agent can act on). The vertical is the
 * WORKSPACE's (DAT-506: a workspace property); the table set is the workspace-current
 * set (the generation heads).
 */
export async function buildWorkspaceContext(
	kind: ConversationKind,
): Promise<string | null> {
	const workspace = await resolveActiveWorkspaceRow();
	const tableNames = await workspaceTableNames();
	if (tableNames.length === 0) return null;
	const block = formatWorkspaceContext(workspace.vertical, tableNames);

	// Append a compact, PROJECTED readiness digest (DAT-634) so the agent can speak
	// to "what's blocked / what to do next" for this chat's kind without a tool
	// round-trip. Soft: a briefing read blip just drops the digest, keeping the base
	// block. TODO(DAT-634): cache candidate — buildWorkspaceBriefing runs ~9 queries
	// per turn here; a short-TTL / invalidate-on-promote cache would cut it.
	// Intentionally UNCACHED for now (correctness first; DB cost is negligible beside
	// the LLM call). Do NOT add a cache without measuring.
	const briefing = await buildWorkspaceBriefing().catch(() => null);
	const digest = briefing ? formatBriefingDigest(briefing, kind) : null;
	return digest && block ? `${block}\n\n${digest}` : block;
}
