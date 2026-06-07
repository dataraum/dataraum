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
// The DB join is browser-smoke-covered; the pure row→shape projection is
// unit-tested here via `projectRelationshipReadiness`.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, eq, inArray, like } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import { parseRelationshipTarget } from "../db/metadata/relationship-target";
import {
	columns,
	currentEntropyReadiness,
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

/** Endpoint name lookup: column_id → its column + owning table name. */
export type ColumnNameLookup = Map<
	string,
	{ columnName: string; tableName: string }
>;

/**
 * Project one relationship-readiness row to the tool's shape. Pure (no DB) so the
 * target-parsing + JSONB-parsing + name-resolution is unit-testable. Returns null
 * for a row whose target isn't a parseable relationship key (a defensive guard —
 * the query already filters to `relationship:%`). A malformed JSONB blob degrades
 * to empty intents/drivers rather than throwing.
 */
export function projectRelationshipReadiness(
	row: RelationshipReadinessRow,
	names: ColumnNameLookup,
): RelationshipReadiness | null {
	const pair = parseRelationshipTarget(row.target);
	if (!pair) return null;

	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);
	const from = names.get(pair.fromColumnId);
	const to = names.get(pair.toColumnId);

	return {
		from_column_id: pair.fromColumnId,
		to_column_id: pair.toColumnId,
		// This result goes back to the agent — strip the content-keyed
		// `src_<digest>__` prefix so no hash name reaches LLM context (DAT-431).
		from_table_name: from ? displayTableName(from.tableName) : null,
		from_column_name: from?.columnName ?? null,
		to_table_name: to ? displayTableName(to.tableName) : null,
		to_column_name: to?.columnName ?? null,
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
	};
}

export interface LookRelationshipsInput {
	session_id: string;
}

/** Per-relationship readiness for one session's promoted detect run. */
export async function lookRelationships(
	input: LookRelationshipsInput,
): Promise<LookRelationshipsResult> {
	// The current_* view IS the promoted run (ADR-0008/DAT-453): the head join
	// lives in the database. `target` carries the identity (relationship rows
	// have null table_id/column_id), so filter by the `relationship:%` prefix.
	// No promoted run → empty view → analyzed=false below.
	const rawRows = await metadataDb
		.select({
			target: currentEntropyReadiness.target,
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			topDrivers: currentEntropyReadiness.topDrivers,
		})
		.from(currentEntropyReadiness)
		.where(
			and(
				eq(currentEntropyReadiness.sessionId, input.session_id),
				like(currentEntropyReadiness.target, "relationship:%"),
			),
		)
		.orderBy(asc(currentEntropyReadiness.target));
	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the identity fields the underlying table guarantees.
	const readinessRows: RelationshipReadinessRow[] = rawRows.map((r) => ({
		...r,
		target: r.target ?? "",
	}));

	if (readinessRows.length === 0) {
		return {
			session_id: input.session_id,
			analyzed: false,
			pending_teaches: 0,
			relationships: [],
		};
	}

	// Batch-resolve endpoint names: collect every column id the targets reference,
	// then one join to columns⟕tables. Avoids an N+1 over relationships.
	const names = await loadColumnNames(readinessRows);

	const relationships = readinessRows
		.map((r) => projectRelationshipReadiness(r, names))
		.filter((r): r is RelationshipReadiness => r !== null);

	const pending = await getPendingOverlays();

	return {
		session_id: input.session_id,
		analyzed: true,
		pending_teaches: pending.length,
		relationships,
	};
}

/** Resolve the from/to column + table names referenced by the readiness targets. */
async function loadColumnNames(
	rows: RelationshipReadinessRow[],
): Promise<ColumnNameLookup> {
	const ids = new Set<string>();
	for (const r of rows) {
		const pair = parseRelationshipTarget(r.target);
		if (pair) {
			ids.add(pair.fromColumnId);
			ids.add(pair.toColumnId);
		}
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
		"(from_column_id → to_column_id). Read-only; reflects the promoted detect run " +
		"for the session. pending_teaches counts un-applied teaches across the " +
		"workspace; if > 0, suggest a `replay` before trusting the bands. Use " +
		"`why_relationship` to explain a specific relationship's band.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe("The begin_session session to inspect (its session_id)."),
	}),
	outputSchema: LookRelationshipsResult,
}).server((input) => lookRelationships(input));
