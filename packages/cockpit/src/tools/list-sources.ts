// list_sources tool — the workspace's AVAILABLE inputs (the pre-`select`
// inventory), unified across configured databases and uploaded files.
//
// This is what a user (and the agent) means by "what data do I have to work
// with": the configured DB sources (`DATARAUM_<NAME>_URL`) plus the files staged
// in the bucket's `uploads/` prefix — BEFORE any of them is registered or
// imported. It is deliberately NOT the post-`select` `sources` table: once a
// source is selected + imported it materializes as tables, which `list_tables`
// reports. (Earlier this tool read the post-`select` rows, so a freshly uploaded
// file showed up nowhere until the whole connect→frame→select dance completed —
// the agent's natural first call, `list_sources`, came back empty.)
//
// Pure reads (env scan + an S3 prefix list), no approval. No secret is exposed:
// a database entry carries only its name + scheme-inferred backend, never the
// connection URL.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { listConfiguredDatabases } from "../duckdb/credentials";
import { UPLOAD_PREFIX } from "../upload/policy";
import { listPrefixObjects } from "../upload/s3-upload";

const AvailableSource = z.object({
	// "database" = a configured DB source; "file" = a staged uploaded file.
	kind: z.enum(["database", "file"]),
	// database: the source name (the key for connect/select); file: the filename.
	name: z.string(),
	// database: backend kind (postgres/mysql/sqlite/mssql); file: null.
	backend: z.string().nullable(),
	// file: the s3:// handle to pass to connect/select; database: null.
	uri: z.string().nullable(),
	// file: object size in bytes; database: null.
	size_bytes: z.number().nullable(),
});
export type AvailableSource = z.infer<typeof AvailableSource>;

/** The configured database sources, as `AvailableSource` rows. */
function databaseSources(): AvailableSource[] {
	return listConfiguredDatabases().map((d) => ({
		kind: "database" as const,
		name: d.name,
		backend: d.backend,
		uri: null,
		size_bytes: null,
	}));
}

/** The staged uploaded files (under the bucket's `uploads/` prefix). */
async function uploadedFileSources(): Promise<AvailableSource[]> {
	const objects = await listPrefixObjects(config.s3Bucket, `${UPLOAD_PREFIX}/`);
	return objects.map((o) => ({
		kind: "file" as const,
		// Key is `uploads/<digest>/<filename>`: the leaf is the original filename.
		name: o.key.split("/").pop() ?? o.key,
		backend: null,
		uri: `s3://${config.s3Bucket}/${o.key}`,
		size_bytes: o.size,
	}));
}

/** All available inputs in the workspace: configured databases + uploaded files. */
export async function listSources(): Promise<AvailableSource[]> {
	const files = await uploadedFileSources();
	return [...databaseSources(), ...files];
}

export const listSourcesTool = toolDefinition({
	name: "list_sources",
	description:
		"List the data inputs AVAILABLE to import into the workspace — configured " +
		"databases and uploaded files — i.e. the pre-`select` inventory of what the " +
		"user has to work with. Use it to see what's available, then `connect` to " +
		"preview one and `select` to register it. Each entry has a `kind` " +
		"(database | file), a `name`, the `backend` (databases) and the s3:// `uri` " +
		"(files). Data that has already been imported appears as TABLES via " +
		"`list_tables`, not here.",
	inputSchema: z.object({}),
	outputSchema: z.array(AvailableSource),
}).server(() => listSources());
