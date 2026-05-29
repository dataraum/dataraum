// list_sources tool (DAT-353) — read the workspace's registered sources.
//
// Pure read via the Drizzle metadata client (ws_<id>.sources). No approval:
// reads are unattended. The DB query is covered by the gated integration test
// (skips without METADATA_DATABASE_URL), mirroring teach's split — no mocking.

import { toolDefinition } from "@tanstack/ai";
import { isNull } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { sources } from "../db/metadata/schema";

const SourceSummary = z.object({
	source_id: z.string(),
	name: z.string(),
	source_type: z.string(),
	status: z.string().nullable(),
	backend: z.string().nullable(),
	created_at: z.string(),
});
export type SourceSummary = z.infer<typeof SourceSummary>;

/** All non-archived sources in the active workspace, oldest first. */
export async function listSources(): Promise<SourceSummary[]> {
	const rows = await metadataDb
		.select({
			sourceId: sources.sourceId,
			name: sources.name,
			sourceType: sources.sourceType,
			status: sources.status,
			backend: sources.backend,
			createdAt: sources.createdAt,
		})
		.from(sources)
		.where(isNull(sources.archivedAt))
		.orderBy(sources.createdAt);

	return rows.map((r) => ({
		source_id: r.sourceId,
		name: r.name,
		source_type: r.sourceType,
		status: r.status,
		backend: r.backend,
		created_at: r.createdAt.toISOString(),
	}));
}

export const listSourcesTool = toolDefinition({
	name: "list_sources",
	description:
		"List the data sources registered in the workspace (excludes archived). Returns each source's id, name, type, status, and backend.",
	inputSchema: z.object({}),
	outputSchema: z.array(SourceSummary),
}).server(() => listSources());
