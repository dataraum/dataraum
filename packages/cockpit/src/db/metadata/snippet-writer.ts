// Snippet WRITE surface (DAT-486) — save-on-clean persists a learned
// `query:` snippet so the library grows from real questions and reuse compounds.
//
// Mirrors the semantics of the engine's `snippet_library.py` `save_snippet`:
// concept-keyed, first-writer-wins. (The engine's own `query`-type producer is
// gone — DAT-487 removed the query agent — so the cockpit answer tool is now the
// only writer of `query` snippets.) The
// snippet identity is the 6-col `uq_snippet_semantic_key`; for a `query` snippet
// statement/aggregation/parameter_value are all NULL, so dedup CANNOT be a DB
// `ON CONFLICT` — the unique key is NULLS DISTINCT, so a null-bearing key never
// conflicts and every save would duplicate. We replicate the engine's app-level
// `_find_by_key_any`: SELECT by key with explicit IS-NULL matching, then INSERT
// only if absent. The cockpit writes the raw `sql_snippets` table through the
// control-plane write surface (write-surface.ts), granted SELECT,INSERT to the
// per-workspace writer role (storage/read_views.py, DAT-816).
//
// Scope: this writes ONLY `query`-type snippets (the cockpit answer tool's
// learned SQL). Cross-`snippet_type` reconciliation/promotion against the
// graph-minted extract/formula snippets is a separate layer (DAT-493).

import { randomUUID } from "node:crypto";
import { and, eq, isNull, type SQL } from "drizzle-orm";

import { metadataWriteDb } from "./client";
import { sqlSnippetsWrite } from "./write-surface";

/**
 * The concept-key a learned `query:` snippet populates — the subset of
 * `uq_snippet_semantic_key` that matters for this path. snippet_type is always
 * `query`; statement/aggregation/parameter_value are always NULL.
 */
export interface QuerySnippetKey {
	/** The dashed-UUID workspace_id VALUE (`config.dataraumWorkspaceId`). */
	schemaMappingId: string;
	/** The concept the step computes (the answer tool's `Component.name`). */
	standardField: string;
}

export interface SaveQuerySnippetInput extends QuerySnippetKey {
	/** The dashed-UUID workspace_id VALUE (the NOT-NULL `workspace_id` column,
	 * DAT-506 — snippets are workspace-scoped, no session FK). */
	workspaceId: string;
	sql: string;
	description: string;
	/** Provenance, e.g. `query:<runId>`. */
	source: string;
}

export interface SaveQuerySnippetResult {
	snippetId: string;
	/** true when a snippet with the same key already existed and was KEPT (first-writer-wins). */
	deduped: boolean;
}

/**
 * The full semantic key for a `query` snippet, IS-NULL-aware. statement /
 * aggregation / parameter_value are NULL for query snippets and the unique
 * constraint is NULLS DISTINCT, so these MUST be `isNull(...)` — `eq(col, null)`
 * renders `col = NULL`, which never matches, and dedup would silently fail.
 * Filters `snippet_type = 'query'`: a query save dedups only against other query
 * snippets, never the graph-minted extract/formula rows (mirrors the engine's
 * `_find_by_key_any`, which keys by the same snippet_type).
 */
export function queryKeyConditions(key: QuerySnippetKey): SQL {
	return and(
		eq(sqlSnippetsWrite.snippetType, "query"),
		eq(sqlSnippetsWrite.standardField, key.standardField),
		isNull(sqlSnippetsWrite.statement),
		isNull(sqlSnippetsWrite.aggregation),
		eq(sqlSnippetsWrite.schemaMappingId, key.schemaMappingId),
		isNull(sqlSnippetsWrite.parameterValue),
	) as SQL;
}

/**
 * Save a learned `query:` snippet, first-writer-wins. If a snippet with the same
 * concept key already exists it is KEPT and its id returned (`deduped: true`) —
 * the first clean computation of a concept becomes the reusable one, matching the
 * engine's healthy-existing branch. Otherwise a new row is inserted.
 *
 * The caller gates WHICH steps reach here (only `fresh`/`adapted` components, not
 * `exact_reuse` — saving a reused step would re-write the curated `graph:` row it
 * came from).
 *
 * Concurrency: the dedup is SELECT-then-INSERT with NO DB backstop — the
 * `uq_snippet_semantic_key` constraint is NULLS DISTINCT, so a null-bearing
 * `query:` key (statement/aggregation/parameter_value all NULL) never raises a
 * unique violation. Two CONCURRENT saves of the same concept can therefore both
 * miss the SELECT and both insert. This is low-harm (a duplicate `query:` row;
 * both are reusable, the next sequential save dedups, and the cross-type
 * reconciliation DAT-493 collapses them) and acceptable for this best-effort
 * path. The proper backstop — `NULLS NOT DISTINCT` on the engine constraint, so
 * the DB enforces what the app-level IS-NULL match intends — is deferred.
 *
 * Failure-replacement: the engine's `save_snippet` REPLACES a row with
 * `failure_count > 0`; this path always keeps the existing row. No cockpit path
 * sets `failure_count` yet (usage/quarantine is P2b/DAT-488) and the grant has no
 * UPDATE, so it cannot bite — folded into P2b when failure tracking lands.
 */
export async function saveQuerySnippet(
	input: SaveQuerySnippetInput,
): Promise<SaveQuerySnippetResult> {
	const existing = await metadataWriteDb
		.select({ snippetId: sqlSnippetsWrite.snippetId })
		.from(sqlSnippetsWrite)
		.where(queryKeyConditions(input))
		.limit(1);
	if (existing.length > 0) {
		return { snippetId: existing[0].snippetId, deduped: true };
	}

	const snippetId = randomUUID();
	const now = new Date();
	await metadataWriteDb.insert(sqlSnippetsWrite).values({
		snippetId,
		workspaceId: input.workspaceId,
		snippetType: "query",
		standardField: input.standardField,
		statement: null,
		aggregation: null,
		schemaMappingId: input.schemaMappingId,
		parameterValue: null,
		sql: input.sql,
		description: input.description,
		source: input.source,
		executionCount: 0,
		failureCount: 0,
		createdAt: now,
		updatedAt: now,
	});
	return { snippetId, deduped: false };
}
