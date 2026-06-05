// Display + sanitization helpers that turn engine-internal identifiers into the
// names a person (or the agent) reads. The engine stores physical tables as
// `<source>__<table>` and reports dimensions/detectors as dotted snake_case
// paths; widgets render the human form while the raw value stays in the
// underlying tool JSON. This module is ALSO the agent-facing leak barrier
// (DAT-431/DAT-433): an uploaded file's source is content-keyed `src_<40-hex
// sha1>`, and that digest must never reach LLM context — the tool projections
// strip it via these helpers. Pure (no React/DB) so each is unit-testable in
// isolation.

// The content-keyed source name a file upload mints (`src_` + the upload's
// 40-char sha-1 content digest — see select/mappers.ts `contentKeyedSourceName`).
// Physical table names derived from it embed `src_<digest>__` (raw/typed
// layers), `enriched_src_<digest>__` (enriched views), or — underscore-collapsed
// by the engine's slice sanitizer — `slice_src_<digest>_` (slice tables).
// `(?![0-9a-f])` pins the digest to EXACTLY 40 hex chars — `src_` + 41 hex is
// NOT a digest, and stripping its first 40 chars would mid-word mangle the
// token, so it must not match. The `i` flag is free insurance (the engine
// emits lowercase digests today).
const SRC_DIGEST = /src_[0-9a-f]{40}(?![0-9a-f])/i;
const SRC_DIGEST_PREFIX_G = /src_[0-9a-f]{40}(?![0-9a-f])__/gi;
const SRC_DIGEST_G = /src_[0-9a-f]{40}(?![0-9a-f])/gi;
const SLICE_FAMILY = /^slice_src_[0-9a-f]{40}(?![0-9a-f])_(.+)$/i;
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
 * Drop the engine's `<source>__` physical-table prefix for display, e.g.
 * `finance_data__trial_balance` → `trial_balance`. When the source name is known
 * we try to strip exactly that prefix; otherwise (or if it doesn't match — the
 * stored prefix is the *sanitized* source name, so a raw name with spaces/caps
 * won't match) we fall back to dropping everything up to and including the first
 * `__`.
 *
 * INVARIANT the fallback rests on (cite both sides before changing either): a
 * physical name has exactly one `__` separator because BOTH writers collapse
 * underscore runs inside each segment — the engine's `sanitize_identifier`
 * (`packages/engine/src/dataraum/core/duckdb_naming.py`) and the cockpit's
 * `sanitizeRecipeName` (`select/mappers.ts`). A change to either sanitizer that
 * lets `__` survive inside a segment breaks this contract.
 *
 * Two derived-table families DON'T fit the one-`__` shape (DAT-433) and get
 * deliberate prefix-aware handling before the generic rules:
 * - Enriched views are `enriched_<source>__<table>` (engine
 *   `pipeline/phases/enriched_views_phase.py`). The generic fallback would strip
 *   to the bare `<table>`, colliding with the base table's display name — so the
 *   family keeps its prefix: `enriched_<table>`.
 * - Slice tables are `slice_<sanitized source table>_<col>_<value>` with NO `__`
 *   (the engine's slice `_sanitize_name` collapses `__` → `_`, see
 *   `analysis/slicing/slice_runner.py`), so for a content-keyed source the
 *   digest would survive the fallback. The digest family maps
 *   `slice_src_<digest>_<rest>` → `slice_<rest>`. A human-named source's
 *   boundary is unrecoverable after the collapse (`slice_finance_journal_…`) —
 *   left as-is, which is fine: a human-chosen name is not a leak.
 *
 * SOUNDNESS of the family rules rests on the name reservation in select
 * (select/mappers.ts `RESERVED_SOURCE_NAME_PREFIXES`, enforced by the db-source
 * branch of tools/select.ts): a user-chosen source name can never start with
 * `src_`/`enriched_`/`slice_`, so a physical name carrying one of those
 * prefixes can ONLY be the corresponding derived family — never a plain table
 * of a source that happens to share the prefix. Without it, callers that pass
 * no sourceName (the why_column / why_table / why_relationship /
 * look_relationships projections) would family-map a db source named
 * `enriched_data` differently from the sourceName-passing callers AND collide
 * it with the real enriched view of a source named `data`.
 */
export function displayTableName(
	tableName: string,
	sourceName?: string,
): string {
	// Exact source-prefix strip first: an actual enriched/slice name never
	// starts with `<sourceName>__` (it starts with its family prefix), so this
	// only fires for a plain physical table. Family-prefixed SOURCE names can't
	// exist (reserved at select validation — see the doc above); this branch
	// stays as defense-in-depth for any pre-reservation row.
	if (sourceName) {
		const prefix = `${sourceName}__`;
		if (tableName.startsWith(prefix)) return tableName.slice(prefix.length);
	}
	if (tableName.startsWith("enriched_")) {
		const rest = tableName.slice("enriched_".length);
		const j = rest.indexOf("__");
		if (j >= 0) return `enriched_${rest.slice(j + 2)}`;
		return tableName;
	}
	const slice = SLICE_FAMILY.exec(tableName);
	if (slice) return `slice_${slice[1]}`;
	const i = tableName.indexOf("__");
	return i >= 0 ? tableName.slice(i + 2) : tableName;
}

/**
 * Backstop strip for engine-built FREE TEXT (failure messages, serialized
 * evidence) that can embed content-keyed names this module's structured
 * handling didn't see. Three rules, most-specific first:
 * - a staged-upload URI (`s3://<bucket>/uploads/<digest>/<file>` — the one
 *   shape where the BARE digest appears with no `src_` prefix, e.g. an import
 *   failure quoting the source URI) drops down to its trailing filename;
 * - `src_<digest>__` physical-name prefixes drop (leaving the table stem);
 * - a remaining bare `src_<digest>` (the source name itself, or a digest
 *   inside an underscore-collapsed slice name) reads as `upload` — a neutral,
 *   digest-free stand-in.
 * The `upload` collapse is LOSSY: distinct digests all read `upload`, so a
 * message naming two uploads loses which is which — the structured
 * `failure.table_id` projected alongside is the disambiguator. This is the
 * LAST line of defense; explicit name keys should be display-mapped before
 * text ever gets here.
 */
export function stripSrcDigests(text: string): string {
	if (!SRC_DIGEST.test(text) && !UPLOAD_URI_PREFIX.test(text)) return text;
	return text
		.replace(UPLOAD_URI_PREFIX_G, "")
		.replace(SRC_DIGEST_PREFIX_G, "")
		.replace(SRC_DIGEST_G, "upload");
}

// Evidence keys that carry a PHYSICAL table name by engine convention:
// relationship detectors stamp `from_table`/`to_table`
// (`entropy/detectors/structural/relations.py`), slice detectors
// `slice_table_name` (`entropy/detectors/value/slice_variance.py`). These get
// display-mapped rather than dropped — the name is meaningful, the digest isn't.
//
// KNOWN LIMITATION: evidence carries no sourceName context, so the mapping is
// displayTableName's no-sourceName form — two same-stem tables from DIFFERENT
// sources (two distinct "orders.csv" uploads → src_<d1>__orders /
// src_<d2>__orders) collapse to the same display name ("orders") inside an
// evidence detail. The top-level from_table_name/to_table_name on the tool
// result remain the more reliable identifiers. Deliberately NOT "fixed" by
// threading source context through every evidence node — the plumbing cost
// outweighs the cross-source same-stem corner case.
const EVIDENCE_TABLE_NAME_KEYS = new Set([
	"from_table",
	"to_table",
	"slice_table_name",
]);

/** Recursively sanitize one evidence node: drop `_`-prefixed keys, display-map
 * known table-name keys, recurse into nested arrays/objects. */
function sanitizeEvidenceNode(node: unknown): unknown {
	if (Array.isArray(node)) return node.map(sanitizeEvidenceNode);
	if (node !== null && typeof node === "object") {
		const out: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(node)) {
			// Engine-internal self-identification keys (`_table_name`,
			// `_column_name` — stamped into EVERY persisted evidence dict by
			// `entropy/detectors/base.py` create_entropy_object) are plumbing the
			// agent never needs; `_table_name` carries the raw digest name.
			if (key.startsWith("_")) continue;
			out[key] =
				EVIDENCE_TABLE_NAME_KEYS.has(key) && typeof value === "string"
					? displayTableName(value)
					: sanitizeEvidenceNode(value);
		}
		return out;
	}
	return node;
}

/**
 * Compact, agent-safe rendering of a detector's persisted evidence blob
 * (DAT-433). The why_* tools put this string in `evidence[].detail`, which
 * reaches BOTH the agent's tool result and the synthesis prompt — so the
 * engine-internal `_`-keys are dropped, explicit table-name keys are
 * display-mapped, and `stripSrcDigests` backstops whatever shape a future
 * detector invents. Engine evidence shape itself is deliberately untouched.
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
