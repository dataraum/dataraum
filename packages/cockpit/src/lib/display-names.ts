// Display + sanitization helpers that turn engine-internal identifiers into the
// names a person (or the agent) reads. Post-DAT-639 physical table names are
// NARROW and workspace-unique (the per-workspace DuckLake catalog is the
// namespace, so `orders` is just `orders` — no `<source>__` / `src_<digest>__`
// prefix), so a stored table name IS already its display name. What this module
// still owns is the agent-facing LEAK BARRIER (DAT-431/DAT-433): an uploaded
// file's SOURCE is content-keyed `src_<40-hex sha1>` and that digest lives on
// `sources.name` + in the upload's s3 URI — it must never reach LLM context, so
// the tool projections strip it from free text via `stripSrcDigests`. Pure (no
// React/DB) so each is unit-testable in isolation.

// The content-keyed source NAME a file upload mints (`src_` + the upload's
// 40-char sha-1 content digest — see select/mappers.ts `contentKeyedSourceName`).
// This is a SOURCE name, not a table name (DAT-639 dropped the digest from
// physical table names); it still appears bare in engine free text (failure
// messages, the source's own name). `(?![0-9a-f])` pins the digest to EXACTLY 40
// hex chars — `src_` + 41 hex is NOT a digest, and stripping its first 40 chars
// would mid-word mangle the token, so it must not match. The `i` flag is free
// insurance (the engine emits lowercase digests today).
const SRC_DIGEST = /src_[0-9a-f]{40}(?![0-9a-f])/i;
const SRC_DIGEST_G = /src_[0-9a-f]{40}(?![0-9a-f])/gi;
// A staged-upload object path (`uploads/<digest>/<file>`, upload/policy.ts
// buildUploadKey) — the one place a BARE digest (no `src_` prefix) reaches
// engine-built text, e.g. an import-failure message quoting the s3 URI.
// Matched up to and including the digest segment's trailing `/` so a replace
// leaves only the filename. Deliberately NOT a blanket bare-40-hex strip:
// evidence values can legitimately carry hex (git SHAs in user data); the
// uploads-path shape is the distinctive safe target.
const UPLOAD_URI_PREFIX =
	/(?:s3:\/\/[^\s"']+\/)?uploads\/[0-9a-f]{40}(?![0-9a-f])\//i;
const UPLOAD_URI_PREFIX_G = new RegExp(UPLOAD_URI_PREFIX.source, "gi");

/**
 * The display name for a physical table. Post-DAT-639 this is the identity: a
 * stored table name is already NARROW and workspace-unique (no `<source>__` /
 * `src_<digest>__` prefix, no `enriched_<source>__` digest form, and slices —
 * which were the one underscore-collapsed `slice_src_<digest>_…` family — have
 * been removed). Retained as the single stable seam every tool/widget routes
 * table-name display through, so any future divergence between physical and
 * display naming has one home; the `sourceName` arg is kept for that same
 * call-site stability though it's no longer consulted.
 */
export function displayTableName(
	tableName: string,
	_sourceName?: string,
): string {
	return tableName;
}

/**
 * Backstop strip for engine-built FREE TEXT (failure messages, serialized
 * evidence) that can embed the content-keyed SOURCE name or its upload URI. Two
 * rules, most-specific first:
 * - a staged-upload URI (`s3://<bucket>/uploads/<digest>/<file>` — the one shape
 *   where the BARE digest appears with no `src_` prefix, e.g. an import failure
 *   quoting the source URI) drops down to its trailing filename;
 * - a remaining bare `src_<digest>` (the source name itself) reads as `upload` —
 *   a neutral, digest-free stand-in.
 * The `upload` collapse is LOSSY: distinct digests all read `upload`, so a
 * message naming two uploads loses which is which — the structured
 * `failure.table_id` projected alongside is the disambiguator. This is the
 * LAST line of defense; explicit name keys should be display-mapped before
 * text ever gets here.
 */
export function stripSrcDigests(text: string): string {
	if (!SRC_DIGEST.test(text) && !UPLOAD_URI_PREFIX.test(text)) return text;
	return text.replace(UPLOAD_URI_PREFIX_G, "").replace(SRC_DIGEST_G, "upload");
}

/** Recursively sanitize one evidence node: drop engine-internal `_`-prefixed
 * keys (plumbing the agent never needs — `entropy/detectors/base.py` stamps
 * `_table_name`/`_column_name` into every persisted evidence dict) and recurse
 * into nested arrays/objects. Table-name values need no per-key display-mapping
 * post-DAT-639 (they're already narrow); the bare-digest backstop runs once over
 * the whole serialized blob in `renderEvidenceDetail`. */
function sanitizeEvidenceNode(node: unknown): unknown {
	if (Array.isArray(node)) return node.map(sanitizeEvidenceNode);
	if (node !== null && typeof node === "object") {
		const out: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(node)) {
			if (key.startsWith("_")) continue;
			out[key] = sanitizeEvidenceNode(value);
		}
		return out;
	}
	return node;
}

/**
 * Compact, agent-safe rendering of a detector's persisted evidence blob
 * (DAT-433). The why_* tools put this string in `evidence[].detail`, which
 * reaches BOTH the agent's tool result and the synthesis prompt — so the
 * engine-internal `_`-keys are dropped and `stripSrcDigests` backstops any
 * content-keyed source name/URI a detector's evidence carries. Engine evidence
 * shape itself is deliberately untouched.
 */
export function renderEvidenceDetail(evidence: unknown): string {
	if (evidence === null || evidence === undefined) return "";
	return stripSrcDigests(JSON.stringify(sanitizeEvidenceNode(evidence)));
}

/**
 * Humanize a snake_case / dotted identifier into a readable label: split on `_`
 * and `.`, then sentence-case the whole thing (only the first word is
 * capitalized). `semantic.business_meaning.naming_clarity` → "Semantic business
 * meaning naming clarity"; `null_ratio` → "Null ratio". An empty/garbage input
 * returns "" so the caller can fall back to the raw token.
 */
export function humanizeIdentifier(token: string): string {
	const words = token
		.split(/[._]+/)
		.map((w) => w.trim())
		.filter(Boolean);
	if (words.length === 0) return "";
	const joined = words.join(" ");
	return joined.charAt(0).toUpperCase() + joined.slice(1);
}
