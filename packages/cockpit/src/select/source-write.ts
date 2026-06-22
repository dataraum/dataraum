// Shared source-row write primitives (DAT-592) — the cross-schema UPSERT into
// the engine-owned `ws_<id>.sources` table + the import-witness read. The sole
// caller is the staging hub's import server fn (`server/import-sources.ts`, one
// source per probed query / uploaded file); the agent `select` tool that used to
// share this was removed when acquisition moved fully to the hub (DAT-597).
//
// The cockpit OWNS the INSERT (the documented metadata-client policy break — the
// engine's import phase assumes the cockpit wrote the source row before
// triggering addSourceWorkflow). These two helpers ARE that write seam; nothing
// here decides the row SHAPE (that's `select/mappers.ts`), only how it lands.

import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import { sources } from "../db/metadata/schema";
import { sourcesWrite } from "../db/metadata/write-surface";

// The onboarding stage a freshly registered source sits at. The staging hub
// assembles → frames → imports; the row is written already at `add_source`, the
// cursor the journey readiness reads.
export const STAGE_AFTER_SELECT = "add_source";

// Initial source status — registered by the cockpit but not yet imported reads
// `configured` (mirrors the integration seed in scripts/smoke-add-source.ts).
export const INITIAL_STATUS = "configured";

/**
 * UPSERT one `sources` row (on the UNIQUE name) and return its source_id.
 *
 * A fresh name INSERTs a new source_id; re-selecting the same name re-points its
 * `connection_config` / `source_type` / `backend` / `stage` (an idempotent
 * re-select, not a duplicate-name error). `created_at` is only set on insert; the
 * update touches `updated_at`. Workspace scope is implicit in the ws_<id> schema
 * the client targets (no workspace_id column post-DAT-343).
 */
export async function upsertSource(values: {
	name: string;
	sourceType: string;
	backend: string | null;
	connectionConfig: Record<string, unknown>;
	now: Date;
}): Promise<string> {
	const [row] = await metadataDb
		.insert(sourcesWrite)
		.values({
			sourceId: randomUUID(),
			name: values.name,
			sourceType: values.sourceType,
			connectionConfig: values.connectionConfig,
			status: INITIAL_STATUS,
			stage: STAGE_AFTER_SELECT,
			backend: values.backend,
			createdAt: values.now,
			updatedAt: values.now,
		})
		.onConflictDoUpdate({
			target: sourcesWrite.name,
			set: {
				sourceType: values.sourceType,
				connectionConfig: values.connectionConfig,
				status: INITIAL_STATUS,
				stage: STAGE_AFTER_SELECT,
				backend: values.backend,
				updatedAt: values.now,
			},
		})
		.returning({ sourceId: sourcesWrite.sourceId });
	return row.sourceId;
}

/**
 * The engine-stamped `imported_recipe_hash` witness on an existing source row,
 * if any (DAT-430).
 *
 * At import success the engine copies the recipe's `recipe_hash` into
 * `connection_config.imported_recipe_hash` — the record of WHICH recipe the
 * source's raw tables were materialized from. A db-source upsert REPLACES the
 * whole `connection_config` JSON, so the caller must carry that engine-owned key
 * forward: preserving it is what lets the engine skip an idempotent re-select
 * (current hash == witness) and fail loud on a re-pointed recipe (mismatch)
 * instead of silently serving stale raw tables. A fresh name (or a never-imported
 * source) has no witness — returns null, and the key is simply absent.
 */
export async function importedRecipeHash(name: string): Promise<string | null> {
	const rows = await metadataDb
		.select({ connectionConfig: sources.connectionConfig })
		.from(sources)
		.where(eq(sources.name, name))
		.limit(1);
	const cc = rows[0]?.connectionConfig as Record<string, unknown> | null;
	const witness = cc?.imported_recipe_hash;
	return typeof witness === "string" && witness.length > 0 ? witness : null;
}
