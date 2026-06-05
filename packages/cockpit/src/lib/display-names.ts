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
const SRC_DIGEST = /src_[0-9a-f]{40}/;
const SRC_DIGEST_PREFIX_G = /src_[0-9a-f]{40}__/g;
const SRC_DIGEST_G = /src_[0-9a-f]{40}/g;
const SLICE_FAMILY = /^slice_src_[0-9a-f]{40}_(.+)$/;

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
 */
export function displayTableName(
	tableName: string,
	sourceName?: string,
): string {
	// Exact source-prefix strip first: an actual enriched/slice name never
	// starts with `<sourceName>__` (it starts with its family prefix), so this
	// only fires for a plain physical table — including one whose SOURCE is
	// legitimately named `enriched_*` and must not be mistaken for the family.
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
 * handling didn't see. `src_<digest>__` physical-name prefixes drop (leaving
 * the table stem); a remaining bare `src_<digest>` (the source name itself, or
 * a digest inside an underscore-collapsed slice name) reads as `upload` — a
 * neutral, digest-free stand-in. This is the LAST line of defense; explicit
 * name keys should be display-mapped before text ever gets here.
 */
export function stripSrcDigests(text: string): string {
	if (!SRC_DIGEST.test(text)) return text;
	return text.replace(SRC_DIGEST_PREFIX_G, "").replace(SRC_DIGEST_G, "upload");
}

// Evidence keys that carry a PHYSICAL table name by engine convention:
// relationship detectors stamp `from_table`/`to_table`
// (`entropy/detectors/structural/relations.py`), slice detectors
// `slice_table_name` (`entropy/detectors/value/slice_variance.py`). These get
// display-mapped rather than dropped — the name is meaningful, the digest isn't.
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

/**
 * Pretty-print a compact JSON string (a detector's evidence blob) with 2-space
 * indentation. Returns the original string unchanged when it isn't valid JSON —
 * detectors are free to emit a plain string, and a parse failure must never blank
 * the cell.
 */
export function prettyJson(raw: string): string {
	if (!raw) return "";
	try {
		return JSON.stringify(JSON.parse(raw), null, 2);
	} catch {
		return raw;
	}
}
