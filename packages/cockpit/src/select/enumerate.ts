// select-stage prefix enumeration (DAT-378) — turn an `s3://<bucket>/<prefix>/`
// into an EXPLICIT, immutable list of `s3://<bucket>/<key>` URIs.
//
// This is the cockpit-side multi-file capability the engine deliberately lacks:
// the engine NEVER globs (its `validate_source_uri` forbids glob metacharacters
// and requires exactly one object per URI), so the cockpit lists the prefix
// (ListObjectsV2) and hands the engine the resulting concrete URI list. That
// list is persisted into the Source row's `connection_config` under the DISTINCT
// `file_uris` key (NOT the db_recipe `tables` key) and is the authoritative,
// frozen pre-trigger artifact the import phase loops over (docs/architecture/pipeline.md).
//
// Why select and not connect: `connect(file)` sniffs exactly ONE object (it
// cannot enumerate — a glob char is rejected there too). `connect(database)`
// already returns multiple tables, so a DB-source subset reuses connect's
// per-table output and needs no enumeration. Only the file-prefix case needs
// this ListObjectsV2 step, so it lives here in the select stage.

import {
	ALLOWED_EXTENSIONS,
	buildUploadUri,
	fileExtension,
} from "../upload/policy";
import { listPrefixKeys } from "../upload/s3-upload";

/** The `file_uris` connection_config the select stage persists into a Source.
 *
 * A DISTINCT key from db_recipe `connection_config.tables` (a list of
 * `{name, sql}` query dicts), so a file source's URI list can never be confused
 * with a recipe's query list. The engine's `ImportPhase._resolve_file_uris`
 * reads exactly this key.
 */
export interface FileUrisConnectionConfig {
	file_uris: string[];
}

/** True when `key` is a real object key (not a zero-byte folder marker) whose
 * extension the loaders can read. The enumeration drops everything else so a
 * README / image / nested folder marker under the prefix never becomes a URI the
 * engine would fail to load. */
export function isLoadableKey(key: string): boolean {
	if (key.endsWith("/")) return false;
	const ext = fileExtension(key);
	return (
		ext !== null && (ALLOWED_EXTENSIONS as readonly string[]).includes(ext)
	);
}

/**
 * Map listed object keys under a prefix to a sorted, explicit `s3://` URI list.
 *
 * Pure (no I/O): the driver `enumeratePrefixUris` does the ListObjectsV2 call
 * and passes the raw keys here. Keys are filtered to loadable data files, mapped
 * to `s3://<bucket>/<key>`, and sorted for a deterministic, stable order (the
 * engine names raw tables `<source_name>__<file_stem>`, so order only affects
 * fan-out scheduling, not the table set — but determinism keeps the persisted
 * artifact reproducible).
 */
export function keysToUris(bucket: string, keys: string[]): string[] {
	return keys
		.filter(isLoadableKey)
		.map((key) => buildUploadUri(bucket, key))
		.sort();
}

/**
 * Enumerate `s3://<bucket>/<prefix>` into an explicit `file_uris` list.
 *
 * Lists the prefix via ListObjectsV2, keeps the loadable data files, and maps
 * them to concrete `s3://` URIs. Throws when the prefix is empty of loadable
 * objects — a select with nothing to import is a loud error, not a silent
 * zero-URI source the import phase would reject downstream.
 *
 * `list` is injected so the unit test can assert the mapping with a mocked
 * ListObjectsV2 without a live SeaweedFS; the default is the real @aws-lite
 * `listPrefixKeys`.
 */
export async function enumeratePrefixUris(
	bucket: string,
	prefix: string,
	list: (bucket: string, prefix: string) => Promise<string[]> = listPrefixKeys,
): Promise<string[]> {
	const keys = await list(bucket, prefix);
	const uris = keysToUris(bucket, keys);
	if (uris.length === 0) {
		throw new Error(
			`No loadable objects found under s3://${bucket}/${prefix} ` +
				`(supported: ${ALLOWED_EXTENSIONS.join(", ")}).`,
		);
	}
	return uris;
}

/** Build the `file_uris` connection_config a Source row stores for a multi-file
 * source. Thin, but it pins the DISTINCT key name in one place so the engine
 * mirror (`file_uris`) and the cockpit write can't drift. */
export function buildFileUrisConfig(uris: string[]): FileUrisConnectionConfig {
	return { file_uris: uris };
}
