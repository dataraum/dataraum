// Multi-file upload batch validation (DAT-391) — the CLIENT-SIDE UX gate the
// dropzone runs before staging anything. Pure, no I/O.
//
// Three rules, all UX (the upload route itself stays one-file-per-request and
// uncapped — these bound what one multi-select composes into ONE `file_uris`
// source, not what the API accepts):
//   1. count ≤ MAX_UPLOAD_FILES,
//   2. every file an extension `connect` can sniff,
//   3. HOMOGENEOUS source_type — a `file_uris` source is one `source_type`, so a
//      batch must be all-csv / all-parquet / all-json (csv+tsv+txt all map to
//      csv, etc.). We reuse the select stage's `sourceTypeForUri` so this gate
//      and the eventual `select` write agree on the ext→type mapping (no drift).

import { sourceTypeForUri } from "../select/mappers";
import {
	ALLOWED_EXTENSIONS,
	isAllowedExtension,
	MAX_UPLOAD_FILES,
} from "./policy";

/**
 * Validate a multi-file selection. Returns a human-readable error string for the
 * first violation, or `null` when the batch is OK to upload. Order: empty →
 * over-cap → unsupported type → mixed kinds.
 */
export function validateUploadBatch(filenames: string[]): string | null {
	if (filenames.length === 0) {
		return "Pick at least one file to upload.";
	}
	if (filenames.length > MAX_UPLOAD_FILES) {
		return `Up to ${MAX_UPLOAD_FILES} files at once — you selected ${filenames.length}. Remove a few and try again.`;
	}
	const unsupported = filenames.filter((f) => !isAllowedExtension(f));
	if (unsupported.length > 0) {
		return `Unsupported file type: ${unsupported.join(", ")}. Allowed: ${ALLOWED_EXTENSIONS.join(", ")}.`;
	}
	// Extensions are validated above, so sourceTypeForUri won't throw here.
	const kinds = new Set(filenames.map(sourceTypeForUri));
	if (kinds.size > 1) {
		return `All files must be the same kind (got ${[...kinds].sort().join(" + ")}). Upload one type per source.`;
	}
	return null;
}
