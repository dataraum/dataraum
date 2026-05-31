// Upload policy + handle shape for the connect upload entry-mode (DAT-386).
//
// PURE, no I/O: the size/extension limits, the object KEY layout, and the
// returned `s3://` handle are all derived here so the route stays a thin I/O
// shell and the contract is unit-testable without SeaweedFS or @aws-lite.
//
// The handle shape `s3://<bucket>/uploads/<uuid>/<filename>` is a DE-FACTO
// CONTRACT that DAT-389 (ingest + cleanup) reads to find the staged file. It is
// locked, not improvised: the `uploads/` prefix is sibling to the lake's
// `lake/` prefix in the SAME bucket (the lake stays at `lake/`), one upload =
// one `<uuid>/` directory so a filename collision across uploads never clobbers,
// and the original filename is preserved as the leaf so the extension drives the
// DuckDB reader on the connect sniff (read_csv_auto / read_parquet / …).

// Object-key prefix uploads land under, sibling to the lake's `lake/` prefix in
// the same bucket. DAT-389 lists this prefix to find staged files.
export const UPLOAD_PREFIX = "uploads";

// Max upload size. Sized for an interactive schema-peek staging file, not a bulk
// load: the sniff only DESCRIBEs + samples the first rows, and the dev SeaweedFS
// is ephemeral. Generous enough for a real CSV/Parquet a user drags in.
export const MAX_UPLOAD_BYTES = 100 * 1024 * 1024; // 100 MiB

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
 * The object key a staged upload lands at: `uploads/<uuid>/<safe-filename>`.
 *
 * `uuid` is the caller-supplied collision-free directory (one per upload);
 * `filename` is sanitized to a single safe leaf. Pure — the route generates the
 * uuid (crypto.randomUUID) and passes it in so this stays deterministic/testable.
 */
export function buildUploadKey(uuid: string, filename: string): string {
	return `${UPLOAD_PREFIX}/${uuid}/${sanitizeFilename(filename)}`;
}

/**
 * The `s3://<bucket>/<key>` handle returned to the client and read by DAT-389.
 *
 * This is the locked contract surface — keep it exactly this shape.
 */
export function buildUploadUri(bucket: string, key: string): string {
	return `s3://${bucket}/${key}`;
}
