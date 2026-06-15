// Upload policy + handle shape for the connect upload entry-mode (DAT-386).
//
// PURE, no I/O: the size/extension limits, the object KEY layout, and the
// returned `s3://` handle are all derived here so the route stays a thin I/O
// shell and the contract is unit-testable without SeaweedFS or @aws-lite.
//
// The handle shape `s3://<bucket>/<ws>/uploads/<digest>/<filename>` is a DE-FACTO
// CONTRACT that DAT-389 (ingest + cleanup) reads to find the staged file. It is
// locked, not improvised: everything for a workspace lives under its `<ws>/`
// prefix (DAT-505: per-workspace isolation on the object store — uploads sit
// beside that workspace's `<ws>/lake/`), the directory segment is the file's
// CONTENT DIGEST (workspace-scoped, see upload/digest.ts) so identical bytes land
// at one key — re-uploads dedup instead of accumulating — while distinct files
// never clobber, and the original filename is preserved as the leaf so the
// extension drives the DuckDB reader on the connect sniff (read_csv_auto /
// read_parquet / …).

// Object-key prefix uploads land under, WITHIN the workspace's `<ws>/` prefix
// (sibling to that workspace's `<ws>/lake/`). DAT-389 lists this prefix to find
// staged files.
export const UPLOAD_PREFIX = "uploads";

/** The per-workspace object-key prefix uploads stage under (DAT-505):
 * `<workspace_id>/uploads`. Everything for a workspace lives under its own
 * `<ws>/` prefix on the shared bucket, so two workspaces never collide and a
 * workspace's whole footprint deletes by dropping one prefix (the deletion
 * sweep). The workspace segment is sanitized to a single safe segment, same as
 * the digest/filename, so a stray id can't escape the prefix. */
export function workspaceUploadPrefix(workspaceId: string): string {
	return `${sanitizeFilename(workspaceId)}/${UPLOAD_PREFIX}`;
}

// Max upload size. Sized for an interactive schema-peek staging file, not a bulk
// load: the sniff only DESCRIBEs + samples the first rows, and the dev SeaweedFS
// is ephemeral. Generous enough for a real CSV/Parquet a user drags in.
export const MAX_UPLOAD_BYTES = 100 * 1024 * 1024; // 100 MiB

// Max files in ONE drag-drop/select batch (DAT-391). A CLIENT-SIDE UX gate only:
// the upload route stays one-file-per-request and uncapped — this bounds how many
// files a single multi-select composes into one source, not what the API accepts.
export const MAX_UPLOAD_FILES = 12;

// Extensions connect can sniff (mirrors duckdb/connect.ts FILE_READERS). The
// upload gate rejects anything else BEFORE it touches the bucket, so an
// unsupported file never stages a dead object DAT-389 would have to clean up.
export const ALLOWED_EXTENSIONS = [
	"csv",
	"tsv",
	"txt",
	"parquet",
	"pq",
	"json",
	"ndjson",
	"jsonl",
] as const;

/** Lowercased extension (no dot) of `filename`, or null when it has none. */
export function fileExtension(filename: string): string | null {
	const dot = filename.lastIndexOf(".");
	if (dot <= 0 || dot === filename.length - 1) return null;
	return filename.slice(dot + 1).toLowerCase();
}

/** True when `filename`'s extension is one connect can sniff. */
export function isAllowedExtension(filename: string): boolean {
	const ext = fileExtension(filename);
	return (
		ext !== null && (ALLOWED_EXTENSIONS as readonly string[]).includes(ext)
	);
}

// Extension → `file_uris` source_type (csv/tsv/txt → csv, parquet/pq → parquet,
// json/jsonl/ndjson → json). A file source's `source_type` is this derived value,
// NEVER the literal "file" — the engine import dispatch keys off it. THE single
// authority both the upload batch gate (upload/batch.ts) and the select write
// (select/mappers.ts re-exports `sourceTypeForUri`) key off, so the two can't
// drift. Lives HERE (the pure, crypto-free upload policy) rather than in
// select/mappers so the CLIENT dropzone can import it without dragging
// select/mappers' `node:crypto` into the browser bundle.
const EXTENSION_TO_SOURCE_TYPE: Record<string, "csv" | "parquet" | "json"> = {
	csv: "csv",
	tsv: "csv",
	txt: "csv",
	parquet: "parquet",
	pq: "parquet",
	json: "json",
	jsonl: "json",
	ndjson: "json",
};

/**
 * The engine `source_type` for a single file URI, derived from its suffix.
 *
 * Throws on an unsupported / extensionless URI — a select that can't name the
 * source_type is a loud error, not a silently-mislabelled row the engine import
 * would reject.
 */
export function sourceTypeForUri(uri: string): "csv" | "parquet" | "json" {
	const ext = fileExtension(uri);
	const type = ext ? EXTENSION_TO_SOURCE_TYPE[ext] : undefined;
	if (!type) {
		throw new Error(
			`Cannot derive source_type for '${uri}' — unsupported or missing extension ` +
				`(supported: ${ALLOWED_EXTENSIONS.join(", ")}).`,
		);
	}
	return type;
}

// A filename can carry path separators or control bytes (a malicious or sloppy
// client); the object key must be a single safe leaf so it can't escape the
// `uploads/<uuid>/` directory or break the S3 path. Strip directory parts, then
// allowlist a conservative leaf charset — everything else (spaces, control
// bytes, parens) collapses to `_`, which also covers control bytes without a
// separate raw-control-char strip. Finally drop leading dots so the leaf can
// never be hidden/relative (`.`, `..`).
export function sanitizeFilename(filename: string): string {
	const leaf = filename.split(/[/\\]/).pop() ?? "";
	const cleaned = leaf.replace(/[^A-Za-z0-9._-]/g, "_").replace(/^\.+/, "");
	return cleaned || "upload";
}

/**
 * The object key a staged upload lands at:
 * `<workspace_id>/uploads/<digest>/<safe-filename>` (DAT-505).
 *
 * `workspaceId` scopes the key to the active workspace's `<ws>/` prefix; `digest`
 * is the file's content digest (upload/digest.ts) — the content-address directory
 * that makes identical bytes dedup; `filename` is sanitized to a single safe leaf.
 * Pure — the route resolves the workspace + computes the digest and passes them in
 * so this stays deterministic/testable.
 *
 * Every interpolated segment is sanitized to a single safe segment: a value that
 * somehow carried `/` or `..` must not be able to re-point the key at another
 * upload, another workspace, the `lake/` prefix, or the bucket root.
 */
export function buildUploadKey(
	workspaceId: string,
	digest: string,
	filename: string,
): string {
	return `${workspaceUploadPrefix(workspaceId)}/${sanitizeFilename(digest)}/${sanitizeFilename(filename)}`;
}

/**
 * The `s3://<bucket>/<key>` handle returned to the client and read by DAT-389.
 *
 * This is the locked contract surface — keep it exactly this shape.
 */
export function buildUploadUri(bucket: string, key: string): string {
	return `s3://${bucket}/${key}`;
}
