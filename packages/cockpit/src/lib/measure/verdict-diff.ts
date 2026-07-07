// Grounding-verdict set diff — the pure half of scripts/measure-stability.ts
// (ADR-0019, KPI `rerun_stability_flips`). Compares two verdict snapshots taken
// from the promoted surface after consecutive begin_session → operating_model
// passes on unchanged data; every difference is a flip.
//
// Cross-run identity (the run-versioned-model gotcha): rows are keyed by their
// CONTENT identity, never by per-run uuids —
//   - surrogate intents by `intent_digest` (deterministic in the component
//     column ids and direction-neutral — engine relationships/db_models.py;
//     column ids are stable across begin_session re-runs because the typed
//     tables don't change);
//   - metric artifacts by `artifact_key` (the graph id).
// `relationship_id` / `intent_id` / `artifact_id` are per-run and never appear
// here.

/** One composite-key ruling: confirmed or declined, keyed by content digest. */
export interface IntentVerdict {
	digest: string;
	status: string;
}

/** One metric artifact's verdict: lifecycle state, the reason it stopped short
 * (or the low-confidence caveat on an executed one — DAT-631), and a digest of
 * the effective definition it was assembled from (the observable lineage on
 * the promoted surface). */
export interface MetricVerdict {
	key: string;
	state: string | null;
	stateReason: string | null;
	lineageDigest: string | null;
}

export interface VerdictSnapshot {
	intents: IntentVerdict[];
	metrics: MetricVerdict[];
}

export interface VerdictFlip {
	kind:
		| "intent-status"
		| "intent-membership"
		| "metric-membership"
		| "metric-state"
		| "metric-lineage"
		| "metric-reason";
	key: string;
	before: string | null;
	after: string | null;
}

/**
 * Deterministic content digest for lineage comparison: canonical JSON (object
 * keys sorted recursively) hashed with FNV-1a. Two structurally-equal payloads
 * digest identically regardless of key order.
 */
export function canonicalDigest(value: unknown): string {
	let hash = 0x811c9dc5;
	for (const char of canonicalJson(value)) {
		hash ^= char.charCodeAt(0);
		// FNV-1a 32-bit prime multiply, kept in uint32 via Math.imul.
		hash = Math.imul(hash, 0x01000193) >>> 0;
	}
	return hash.toString(16).padStart(8, "0");
}

function canonicalJson(value: unknown): string {
	if (value === null || typeof value !== "object") {
		return JSON.stringify(value) ?? "undefined";
	}
	if (Array.isArray(value)) {
		return `[${value.map(canonicalJson).join(",")}]`;
	}
	const entries = Object.entries(value as Record<string, unknown>)
		.filter(([, v]) => v !== undefined)
		.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
		.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`);
	return `{${entries.join(",")}}`;
}

/**
 * Every verdict difference between two snapshots, deterministically ordered by
 * key. One changed aspect = one flip; a metric whose state changed reports the
 * state flip only (its reason follows the state — reporting both would count
 * one verdict change twice), while a reason change on an UNCHANGED state (e.g.
 * the low-confidence caveat appearing on a still-executed metric) is its own
 * flip. A lineage change is independent of state and always reported.
 */
export function diffVerdicts(
	before: VerdictSnapshot,
	after: VerdictSnapshot,
): VerdictFlip[] {
	const flips: VerdictFlip[] = [];

	const intentsBefore = new Map(before.intents.map((i) => [i.digest, i]));
	const intentsAfter = new Map(after.intents.map((i) => [i.digest, i]));
	for (const digest of sortedKeys(intentsBefore, intentsAfter)) {
		const a = intentsBefore.get(digest);
		const b = intentsAfter.get(digest);
		if (a === undefined || b === undefined) {
			// Membership variance: the judge ruled on this composite in one run
			// and never saw it in the other — a flip, not a neutral absence.
			flips.push({
				kind: "intent-membership",
				key: digest,
				before: a?.status ?? null,
				after: b?.status ?? null,
			});
		} else if (a.status !== b.status) {
			flips.push({
				kind: "intent-status",
				key: digest,
				before: a.status,
				after: b.status,
			});
		}
	}

	const metricsBefore = new Map(before.metrics.map((m) => [m.key, m]));
	const metricsAfter = new Map(after.metrics.map((m) => [m.key, m]));
	for (const key of sortedKeys(metricsBefore, metricsAfter)) {
		const a = metricsBefore.get(key);
		const b = metricsAfter.get(key);
		if (a === undefined || b === undefined) {
			flips.push({
				kind: "metric-membership",
				key,
				before: a?.state ?? null,
				after: b?.state ?? null,
			});
			continue;
		}
		if (a.state !== b.state) {
			flips.push({
				kind: "metric-state",
				key,
				before: a.state,
				after: b.state,
			});
		} else if (a.stateReason !== b.stateReason) {
			flips.push({
				kind: "metric-reason",
				key,
				before: a.stateReason,
				after: b.stateReason,
			});
		}
		if (a.lineageDigest !== b.lineageDigest) {
			flips.push({
				kind: "metric-lineage",
				key,
				before: a.lineageDigest,
				after: b.lineageDigest,
			});
		}
	}

	return flips;
}

function sortedKeys(
	a: ReadonlyMap<string, unknown>,
	b: ReadonlyMap<string, unknown>,
): string[] {
	return [...new Set([...a.keys(), ...b.keys()])].sort();
}
