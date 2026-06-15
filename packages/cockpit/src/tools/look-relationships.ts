// look_relationships tool (DAT-409) — a session's per-relationship readiness
// overview. The relationship analog of look_table: where look_table grids a
// table's columns, this grids a begin_session session's detected relationships.
//
// Pure read via the Drizzle metadata client. begin_session's terminal detect
// writes relationship-granularity `entropy_readiness` rows keyed by a
// `relationship:{from_col}::{to_col}` target (DAT-408) and seals the run under a
// `session:{id}` head (DAT-408). This resolves that head, reads the promoted
// run's relationship rows, and surfaces — per relationship — the calibrated band
// (ready/investigate/blocked) across the query/aggregation/reporting intents plus
// the top quality drivers. It reads the PERSISTED band, never re-deriving it (the
// engine owns the rollup). Read-only → no approval.
//
// DAT-478 — the readiness bands say HOW READY each relationship is; the
// relationship *catalog* (`current_relationships`) says WHAT each one is
// (type/cardinality/confidence/detection-method/confirmed). This joins the
// catalog facts onto the band rows by the directional column-pair so the agent
// gets what + how-ready in ONE call, at the same workspace catalog grain. The
// catalog view is already sealed to the promoted run by its own workspace
// `catalog` head EXISTS clause (DAT-506), so both reads see the same run. The
// union is full-outer by column-pair key: a catalog relationship with no readiness row
// surfaces catalog-only (bands null), and a readiness row with no catalog match
// surfaces bands-only (catalog facts null) — neither side is dropped.
//
// The DB join is browser-smoke-covered; the pure row→shape projection + union
// are unit-tested here via `projectRelationshipReadiness` / `unionRelationships`.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, desc, eq, inArray, like } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import {
	catalogHeadTarget,
	parseRelationshipTarget,
	relationshipTargetKey,
} from "../db/metadata/relationship-target";
import {
	columns,
	currentEntropyReadiness,
	currentRelationships,
	metadataSnapshotHead,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

// --- The tool's output: per-relationship bands + a few top driver labels each.

const IntentBand = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
});

const TopDriver = z.object({
	label: z.string(),
	state: z.string(),
	impact_delta: z.number(),
});

const RelationshipReadiness = z.object({
	// The directional column pair identifies the relationship (the target's parts)
	// — these feed why_relationship for the drill-down.
	from_column_id: z.string(),
	to_column_id: z.string(),
	// Endpoint names for display — table names in DISPLAY form (`src_<digest>__`
	// prefix stripped, DAT-431; the drill-down round-trip keys on the column ids);
	// null when a column id no longer resolves (a dropped column on a stale row),
	// so the grid degrades rather than omitting it.
	from_table_name: z.string().nullable(),
	from_column_name: z.string().nullable(),
	to_table_name: z.string().nullable(),
	to_column_name: z.string().nullable(),
	band: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	intents: z.array(IntentBand),
	top_drivers: z.array(TopDriver),
	// Catalog facts (DAT-478) — WHAT the relationship is, from `current_relationships`,
	// joined by the directional column pair. Null when no catalog row matches this
	// pair (a bands-only row), the same way the bands above go null on a catalog-only
	// relationship that has no readiness row yet.
	relationship_type: z.string().nullable(),
	cardinality: z.string().nullable(),
	confidence: z.number().nullable(),
	detection_method: z.string().nullable(),
	is_confirmed: z.boolean().nullable(),
});
export type RelationshipReadiness = z.infer<typeof RelationshipReadiness>;

const LookRelationshipsResult = z.object({
	session_id: z.string(),
	// False when the session has no promoted relationship-readiness run yet (no
	// begin_session detect sealed) — the grid should say "not analyzed" rather
	// than imply the session has no relationships.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	relationships: z.array(RelationshipReadiness),
});
export type LookRelationshipsResult = z.infer<typeof LookRelationshipsResult>;

const TOP_DRIVERS_SHOWN = 3;

/** One entropy_readiness relationship row, as Drizzle returns it. */
export interface RelationshipReadinessRow {
	target: string;
	band: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
	topDrivers: unknown;
}

/** One `current_relationships` catalog row (the facts join), as Drizzle returns it. */
export interface RelationshipCatalogRow {
	fromColumnId: string | null;
	toColumnId: string | null;
	relationshipType: string | null;
	cardinality: string | null;
	confidence: number | null;
	detectionMethod: string | null;
	isConfirmed: boolean | null;
}

/** The catalog facts as they ride on the tool's output shape (null when no match). */
type CatalogFacts = Pick<
	RelationshipReadiness,
	| "relationship_type"
	| "cardinality"
	| "confidence"
	| "detection_method"
	| "is_confirmed"
>;

const NO_CATALOG_FACTS: CatalogFacts = {
	relationship_type: null,
	cardinality: null,
	confidence: null,
	detection_method: null,
	is_confirmed: null,
};

/** Endpoint name lookup: column_id → its column + owning table name. */
export type ColumnNameLookup = Map<
	string,
	{ columnName: string; tableName: string }
>;

/** Resolve the from/to endpoint name fields for a column pair (display-form, DAT-431). */
function endpointNames(
	fromColumnId: string,
	toColumnId: string,
	names: ColumnNameLookup,
): Pick<
	RelationshipReadiness,
	"from_table_name" | "from_column_name" | "to_table_name" | "to_column_name"
> {
	const from = names.get(fromColumnId);
	const to = names.get(toColumnId);
	return {
		// This result goes back to the agent — strip the content-keyed
		// `src_<digest>__` prefix so no hash name reaches LLM context (DAT-431).
		from_table_name: from ? displayTableName(from.tableName) : null,
		from_column_name: from?.columnName ?? null,
		to_table_name: to ? displayTableName(to.tableName) : null,
		to_column_name: to?.columnName ?? null,
	};
}

/** The catalog facts a row carries, defaulting every field to null on no match. */
function catalogFacts(row: RelationshipCatalogRow | undefined): CatalogFacts {
	if (!row) return NO_CATALOG_FACTS;
	return {
		relationship_type: row.relationshipType ?? null,
		cardinality: row.cardinality ?? null,
		confidence: row.confidence ?? null,
		detection_method: row.detectionMethod ?? null,
		is_confirmed: row.isConfirmed ?? null,
	};
}

/**
 * Project one relationship-readiness row to the tool's shape, joining the matching
 * catalog row's facts (or nulls when none). Pure (no DB) so the target-parsing +
 * JSONB-parsing + name-resolution is unit-testable. Returns null for a row whose
 * target isn't a parseable relationship key (a defensive guard — the query already
 * filters to `relationship:%`). A malformed JSONB blob degrades to empty
 * intents/drivers rather than throwing.
 */
export function projectRelationshipReadiness(
	row: RelationshipReadinessRow,
	names: ColumnNameLookup,
	catalog?: RelationshipCatalogRow,
): RelationshipReadiness | null {
	const pair = parseRelationshipTarget(row.target);
	if (!pair) return null;

	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);

	return {
		from_column_id: pair.fromColumnId,
		to_column_id: pair.toColumnId,
		...endpointNames(pair.fromColumnId, pair.toColumnId, names),
		band: row.band ?? null,
		worst_intent_risk: row.worstIntentRisk ?? null,
		intents: intents.success
			? intents.data.map((i) => ({
					intent: i.intent,
					band: i.band,
					risk: i.risk,
				}))
			: [],
		top_drivers: drivers.success
			? drivers.data.slice(0, TOP_DRIVERS_SHOWN).map((d) => ({
					label: d.label,
					state: d.state,
					impact_delta: d.impact_delta,
				}))
			: [],
		...catalogFacts(catalog),
	};
}

/** Project a catalog-only relationship (no readiness row) to the tool's shape:
 * the facts + endpoints, with every band/intent field null. Skipped when either
 * endpoint id is missing (it can't form a stable pair key for the drill-down). */
function projectCatalogOnly(
	row: RelationshipCatalogRow,
	names: ColumnNameLookup,
): RelationshipReadiness | null {
	if (!row.fromColumnId || !row.toColumnId) return null;
	return {
		from_column_id: row.fromColumnId,
		to_column_id: row.toColumnId,
		...endpointNames(row.fromColumnId, row.toColumnId, names),
		band: null,
		worst_intent_risk: null,
		intents: [],
		top_drivers: [],
		...catalogFacts(row),
	};
}

// Representative-row precedence for a directional column pair (DAT-408), mirrored
// from the engine: a user teach (manual) wins over an LLM confirmation, which wins
// over a structural candidate; keeper sits between manual and llm. This is the SAME
// map the engine's readiness pass uses to pick the representative relationship it
// measures (`entropy/detectors/loaders.py` `load_relationship_for_pair`,
// docstring: "that is the relationship the readiness measures"). Higher wins.
// `look_relationships` joins catalog facts onto that readiness band, so the facts it
// surfaces MUST come from the same representative row — selection by precedence, NOT
// confidence, or `manual(0.7)+llm(0.9)` surfaces `llm` while the engine measured
// `manual`, and the facts contradict the band on the same grid row.
const REL_METHOD_PRECEDENCE: Record<string, number> = {
	manual: 4,
	keeper: 3,
	llm: 2,
	candidate: 1,
};

const relMethodRank = (method: string | null): number =>
	REL_METHOD_PRECEDENCE[method ?? ""] ?? 0;

/**
 * The per-pair catalog winner: a confirmed FK carries MULTIPLE catalog rows in one
 * promoted run — a structural `candidate` row AND an `llm`/`manual`/`keeper` row
 * (uniqueness is `(session_id, run_id, from_column_id, to_column_id, detection_method)`).
 * The facts we surface (`relationship_type` / `is_confirmed` / …) must match the
 * representative row the engine's readiness pass measured, so per pair we pick by the
 * engine's METHOD PRECEDENCE (`manual > keeper > llm > candidate`), NOT confidence —
 * else the surfaced facts can contradict the band on the same row. Confidence is only
 * the final tiebreak among rows of the SAME `detection_method`. The candidate-only
 * exclusion (no band → drop) stays in {@link unionRelationships}: candidate is the
 * lowest precedence, so a pair with candidate + any other method already resolves to
 * the other method here. Returns true when `next` should replace the current `best`.
 */
function catalogRowBeats(
	next: RelationshipCatalogRow,
	best: RelationshipCatalogRow,
): boolean {
	const nextRank = relMethodRank(next.detectionMethod);
	const bestRank = relMethodRank(best.detectionMethod);
	if (nextRank !== bestRank) return nextRank > bestRank;
	// Same detection_method — confidence breaks the tie.
	return (next.confidence ?? -1) > (best.confidence ?? -1);
}

/**
 * Full-outer union of readiness bands and catalog facts, keyed by the directional
 * column pair (`relationship:{from}::{to}`). Pure + unit-testable. Order: readiness
 * rows first (their query order is preserved — they carry the bands the grid sorts
 * around), then any catalog-only relationships the readiness pass didn't cover.
 * Neither side is dropped except by the candidate-only policy below: a bands-only row
 * keeps null catalog facts, a catalog-only relationship keeps null bands/intents.
 *
 * Two determinism guarantees (DAT-478 review): (1) per pair the winning catalog row
 * is chosen by {@link catalogRowBeats} — the engine's method precedence
 * (`manual > keeper > llm > candidate`), confidence only as the same-method tiebreak —
 * so the facts surface from the SAME representative row the engine's readiness pass
 * measured, never depending on row order; (2) a pair whose ONLY catalog rows are
 * `candidate` AND that has no readiness band row is dropped, mirroring the readiness
 * contract (`detection_method != 'candidate'`): a bare structural candidate the LLM
 * never confirmed is not a catalog relationship.
 */
export function unionRelationships(
	readinessRows: RelationshipReadinessRow[],
	catalogRows: RelationshipCatalogRow[],
	names: ColumnNameLookup,
): RelationshipReadiness[] {
	const catalogByPair = new Map<string, RelationshipCatalogRow>();
	for (const c of catalogRows) {
		if (!c.fromColumnId || !c.toColumnId) continue;
		const key = relationshipTargetKey(c.fromColumnId, c.toColumnId);
		const best = catalogByPair.get(key);
		if (!best || catalogRowBeats(c, best)) catalogByPair.set(key, c);
	}

	const out: RelationshipReadiness[] = [];
	const matchedPairs = new Set<string>();
	for (const r of readinessRows) {
		const pair = parseRelationshipTarget(r.target);
		const key = pair
			? relationshipTargetKey(pair.fromColumnId, pair.toColumnId)
			: null;
		const projected = projectRelationshipReadiness(
			r,
			names,
			key ? catalogByPair.get(key) : undefined,
		);
		if (!projected) continue;
		out.push(projected);
		if (key) matchedPairs.add(key);
	}

	for (const [key, c] of catalogByPair) {
		if (matchedPairs.has(key)) continue;
		// Candidate-only policy: a pair the readiness pass didn't score (no band row)
		// whose winning catalog row is still a bare structural `candidate` is NOT a
		// catalog relationship — drop it (the engine's `detection_method != 'candidate'`
		// contract). Only confirmed/manual/keeper rows surface catalog-only.
		if (c.detectionMethod === "candidate") continue;
		const projected = projectCatalogOnly(c, names);
		if (projected) out.push(projected);
	}

	return out;
}

export interface LookRelationshipsInput {
	session_id: string;
}

/** Per-relationship readiness for one session's promoted detect run. */
export async function lookRelationships(
	input: LookRelationshipsInput,
): Promise<LookRelationshipsResult> {
	// `analyzed` = a begin_session catalog run SEALED — distinct from "sealed but
	// zero relationships" (single-table workspace), which must not read as
	// never-ran. The head pass-through stays on the read surface for exactly
	// this check; the rows themselves come from the current_* view. Resolved at
	// the workspace `catalog` head (DAT-506), so it carries no session.
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, catalogHeadTarget()),
				eq(metadataSnapshotHead.stage, "catalog"),
			),
		)
		.limit(1);
	if (!head?.runId) {
		return {
			session_id: input.session_id,
			analyzed: false,
			pending_teaches: 0,
			relationships: [],
		};
	}

	// Two independent reads off the same promoted run — the relationship-readiness
	// bands and the relationship catalog. Fire them in parallel: neither depends on
	// the other (both key off sessionId; the union joins them in memory afterward).
	//
	// The current_* view IS the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database. `target` carries the identity (relationship rows
	// have null table_id/column_id), so filter by the `relationship:%` prefix.
	const readinessQuery = metadataDb
		.select({
			target: currentEntropyReadiness.target,
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			topDrivers: currentEntropyReadiness.topDrivers,
		})
		.from(currentEntropyReadiness)
		.where(like(currentEntropyReadiness.target, "relationship:%"))
		.orderBy(asc(currentEntropyReadiness.target));

	// The relationship catalog (DAT-478) — WHAT each relationship is. The view is
	// already sealed to the promoted begin_session run by its own workspace
	// `catalog` head EXISTS clause (DAT-506), so it reads the same run as the
	// readiness rows above. Joined onto the bands by the directional column pair.
	const catalogQuery = metadataDb
		.select({
			fromColumnId: currentRelationships.fromColumnId,
			toColumnId: currentRelationships.toColumnId,
			relationshipType: currentRelationships.relationshipType,
			cardinality: currentRelationships.cardinality,
			confidence: currentRelationships.confidence,
			detectionMethod: currentRelationships.detectionMethod,
			isConfirmed: currentRelationships.isConfirmed,
		})
		.from(currentRelationships)
		// The in-memory fold owns winner selection — `unionRelationships` /
		// `catalogRowBeats` pick the per-pair representative by the engine's method
		// precedence (`manual > keeper > llm > candidate`), confidence only as the
		// same-method tiebreak. So this ORDER BY does NOT decide the winner; it only
		// stabilizes the INPUT order the fold sees so two reads never disagree on the
		// rare exact-tie (same method AND same confidence). Note Postgres `DESC`
		// defaults to NULLS FIRST, so a NULL-confidence row sorts BEFORE a real one
		// here — harmless, since the fold compares confidence (NULL → -1) itself.
		.orderBy(
			asc(currentRelationships.fromColumnId),
			asc(currentRelationships.toColumnId),
			desc(currentRelationships.confidence),
		);

	const [rawRows, catalogRows] = await Promise.all([
		readinessQuery,
		catalogQuery,
	]);

	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the identity fields the underlying table guarantees.
	const readinessRows: RelationshipReadinessRow[] = rawRows.map((r) => ({
		...r,
		target: r.target ?? "",
	}));

	// Batch-resolve endpoint names across BOTH sides of the union — every column id
	// a readiness target or a catalog row references — then one join to
	// columns⟕tables. Avoids an N+1 over relationships.
	const names = await loadColumnNames(readinessRows, catalogRows);

	const relationships = unionRelationships(readinessRows, catalogRows, names);

	const pending = await getPendingOverlays();

	return {
		session_id: input.session_id,
		analyzed: true,
		pending_teaches: pending.length,
		relationships,
	};
}

/** Resolve the from/to column + table names referenced across both union sides —
 * the readiness targets AND the catalog rows (catalog-only relationships need
 * endpoint names too). */
async function loadColumnNames(
	rows: RelationshipReadinessRow[],
	catalogRows: RelationshipCatalogRow[],
): Promise<ColumnNameLookup> {
	const ids = new Set<string>();
	for (const r of rows) {
		const pair = parseRelationshipTarget(r.target);
		if (pair) {
			ids.add(pair.fromColumnId);
			ids.add(pair.toColumnId);
		}
	}
	for (const c of catalogRows) {
		if (c.fromColumnId) ids.add(c.fromColumnId);
		if (c.toColumnId) ids.add(c.toColumnId);
	}
	const lookup: ColumnNameLookup = new Map();
	if (ids.size === 0) return lookup;

	const nameRows = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			tableName: tables.tableName,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.where(inArray(columns.columnId, [...ids]));

	for (const n of nameRows) {
		// View columns type as nullable — the underlying tables guarantee these.
		lookup.set(n.columnId ?? "", {
			columnName: n.columnName ?? "",
			tableName: n.tableName ?? "",
		});
	}
	return lookup;
}

export const lookRelationshipsTool = toolDefinition({
	name: "look_relationships",
	description:
		"Show a begin_session session's per-relationship readiness — ready/investigate/" +
		"blocked across the query, aggregation, and reporting intents — with the top " +
		"quality drivers per relationship, identified by its directional column pair " +
		"(from_column_id → to_column_id). Each relationship also carries its catalog " +
		"facts (relationship_type, cardinality, confidence, detection_method, " +
		"is_confirmed) — WHAT it is alongside HOW READY it is. Read-only; reflects the " +
		"promoted detect run for the session. pending_teaches counts un-applied teaches " +
		"across the workspace; if > 0, suggest a `replay` before trusting the bands. Use " +
		"`why_relationship` to explain a specific relationship's band.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe("The begin_session session to inspect (its session_id)."),
	}),
	outputSchema: LookRelationshipsResult,
}).server((input) => lookRelationships(input));
