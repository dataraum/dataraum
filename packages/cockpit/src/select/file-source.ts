// Persist staged file uploads as content-keyed sources (DAT-594) — one
// content-keyed `src_<digest>` source PER uploaded file, the file analog of
// `recipe-source.ts`'s one-query = one-source path.
//
// This is the file-source persistence extracted OUT of the agent `select` tool
// (tools/select.ts) so BOTH the agent and the probe surface's staging hub
// (server/import-sources.ts) write file sources the same way. The model is locked
// (DAT-422): one file = one content-keyed source, keyed by its upload digest, so
// identical bytes (same digest) UPSERT one row and two distinct files never
// collide on a raw table even with matching basenames.
//
// NO trigger here: persistence + validation only, mirroring `persistRecipeSources`.
// `server/import-sources.ts` composes this with `persistRecipeSources` and ONE
// `triggerAddSource` over the union so a heterogeneous import set (files + queries)
// imports as one coherent run.

import { sourceTypeForUri } from "./mappers";
import { contentKeyedSourceName } from "./source-content-hash";
import { upsertSource } from "./source-write";

/** One staged upload the user chose to import — its `s3://` URI. */
export interface FileSourceSpec {
	/** The staged `s3://<bucket>/<ws>/uploads/<digest>/<filename>` upload URI. */
	file_uri: string;
}

/** One persisted file source: its id + the content-keyed name + URI it landed as. */
export interface PersistedFileSource {
	source_id: string;
	source_name: string;
	source_type: string;
	file_uri: string;
}

/**
 * UPSERT one content-keyed `src_<digest>` source per staged upload URI and return
 * their ids + names. Persistence ONLY — the caller triggers the batched import.
 *
 * Dedup by content key so a repeated URI UPSERTs once; `contentKeyedSourceName`
 * fails loud on a non-upload URI (content identity requires the upload digest), so
 * a bad batch fails before any write. The write loop is NOT a single transaction
 * (matching the sibling recipe loop): a transient mid-loop failure leaves the
 * earlier sources upserted while the trigger never fires — recoverable, not
 * corrupting (every upsert is idempotent on the content-keyed name).
 */
export async function persistFileSources(
	specs: FileSourceSpec[],
): Promise<PersistedFileSource[]> {
	const now = new Date();
	// Dedup by content key — a repeated URI (same digest → same name) UPSERTs once.
	// `contentKeyedSourceName` fails loud on a non-upload URI BEFORE any write.
	const byName = new Map<string, { uri: string; sourceType: string }>();
	for (const spec of specs) {
		const name = contentKeyedSourceName(spec.file_uri);
		if (!byName.has(name)) {
			byName.set(name, {
				uri: spec.file_uri,
				sourceType: sourceTypeForUri(spec.file_uri),
			});
		}
	}

	const persisted: PersistedFileSource[] = [];
	for (const [name, { uri, sourceType }] of byName) {
		const sourceId = await upsertSource({
			name,
			sourceType,
			backend: null,
			// DISTINCT key from the db_recipe `tables` key — never folded together.
			connectionConfig: { file_uris: [uri] },
			now,
		});
		persisted.push({
			source_id: sourceId,
			source_name: name,
			source_type: sourceType,
			file_uri: uri,
		});
	}
	return persisted;
}
